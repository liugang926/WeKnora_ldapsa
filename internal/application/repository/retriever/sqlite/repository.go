package sqlite

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Tencent/WeKnora/internal/common"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"gorm.io/gorm"
)

// sqliteEmbedding stores metadata alongside the vec0 virtual table rows
type sqliteEmbedding struct {
	ID              uint      `gorm:"primarykey;autoIncrement"`
	CreatedAt       time.Time `gorm:"column:created_at"`
	UpdatedAt       time.Time `gorm:"column:updated_at"`
	SourceID        string    `gorm:"column:source_id;not null;uniqueIndex:idx_sqlite_emb_source"`
	SourceType      int       `gorm:"column:source_type;not null;uniqueIndex:idx_sqlite_emb_source"`
	ChunkID         string    `gorm:"column:chunk_id;index"`
	KnowledgeID     string    `gorm:"column:knowledge_id;index"`
	KnowledgeBaseID string    `gorm:"column:knowledge_base_id;index"`
	TagID           string    `gorm:"column:tag_id;index"`
	Content         string    `gorm:"column:content;not null"`
	Dimension       int       `gorm:"column:dimension;not null"`
	IsEnabled       *bool     `gorm:"column:is_enabled;default:true;index"`
}

func (sqliteEmbedding) TableName() string { return "lite_embeddings" }

type sqliteRepository struct {
	db        *gorm.DB
	initErr   error
	vecMutex  sync.Mutex
	vecTables map[int]bool // tracks which vec0 tables have been created (keyed by dimension)
}

func NewSQLiteRetrieveEngineRepository(db *gorm.DB) interfaces.RetrieveEngineRepository {
	logger.GetLogger(context.Background()).
		Info("[SQLite] Initializing SQLite retriever engine repository with sqlite-vec")

	initErr := db.AutoMigrate(&sqliteEmbedding{})
	if initErr == nil {
		initErr = initFTS5(db)
	}

	repo := &sqliteRepository{
		db:        db,
		initErr:   initErr,
		vecTables: make(map[int]bool),
	}

	repo.ensureExistingVecTables()

	return repo
}

func initFTS5(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var schema string
		if e := tx.Raw("SELECT sql FROM sqlite_master WHERE type='table' AND name='lite_embeddings_fts'").
			Scan(&schema).
			Error; e != nil {
			return e
		}
		if schema != "" && !strings.Contains(schema, "content='lite_embeddings'") {
			return nil
		}
		// Bootstrap cannot act as a Nextcloud worker. Rebuilding old protected
		// fulltext without a current generation lease must be explicit maintenance.
		if tx.Migrator().HasTable("knowledges") {
			var protected int64
			if e := tx.Table("lite_embeddings AS e").
				Joins("JOIN knowledges k ON k.id=e.knowledge_id AND k.knowledge_base_id=e.knowledge_base_id").
				Where("k.channel=?", types.ConnectorTypeNextcloud).
				Count(&protected).
				Error; e != nil {
				return e
			}
			if protected != 0 {
				return fmt.Errorf("Nextcloud FTS rebuild requires guarded maintenance")
			}
		}
		if schema != "" {
			if e := tx.Exec("DROP TABLE lite_embeddings_fts").Error; e != nil {
				return e
			}
		}
		if e := tx.Exec(
			"CREATE VIRTUAL TABLE lite_embeddings_fts USING fts5(content,source_id,chunk_id,knowledge_id,knowledge_base_id,content='',contentless_delete=1,tokenize='unicode61')",
		).Error; e != nil {
			return e
		}
		var rows []sqliteEmbedding
		if e := tx.Find(&rows).Error; e != nil {
			return e
		}
		for _, row := range rows {
			if e := tx.Exec(
				"INSERT INTO lite_embeddings_fts(rowid,content,source_id,chunk_id,knowledge_id,knowledge_base_id) VALUES(?,?,?,?,?,?)",
				row.ID,
				tokenizeCJKBigram(row.Content),
				row.SourceID,
				row.ChunkID,
				row.KnowledgeID,
				row.KnowledgeBaseID,
			).Error; e != nil {
				return e
			}
		}
		return nil
	})
}

func vecTableName(dim int) string {
	return fmt.Sprintf("vec_embeddings_%d", dim)
}

func (r *sqliteRepository) ensureVecTable(dim int) {
	r.vecMutex.Lock()
	defer r.vecMutex.Unlock()
	if dim <= 0 || r.vecTables[dim] {
		return
	}
	tbl := vecTableName(dim)
	createSQL := fmt.Sprintf(
		`CREATE VIRTUAL TABLE IF NOT EXISTS %s USING vec0(embedding float[%d] distance_metric=cosine)`,
		tbl, dim,
	)
	if err := r.db.Exec(createSQL).Error; err != nil {
		if strings.Contains(err.Error(), "already exists") {
			r.vecTables[dim] = true
			return
		}
		logger.GetLogger(context.Background()).Errorf("[SQLite] Failed to create vec0 table for dim %d: %v", dim, err)
		return
	}
	r.vecTables[dim] = true
}

func (r *sqliteRepository) ensureExistingVecTables() {
	var dims []int
	r.db.Model(&sqliteEmbedding{}).Distinct("dimension").Where("dimension > 0").Pluck("dimension", &dims)
	for _, dim := range dims {
		r.ensureVecTable(dim)
	}
}

func (r *sqliteRepository) EngineType() types.RetrieverEngineType {
	return types.SQLiteRetrieverEngineType
}

func (r *sqliteRepository) Support() []types.RetrieverType {
	return []types.RetrieverType{types.KeywordsRetrieverType, types.VectorRetrieverType}
}

func (r *sqliteRepository) Save(ctx context.Context, info *types.IndexInfo, params map[string]any) error {
	if info == nil {
		return fmt.Errorf("nil index info")
	}
	row := toSQLiteEmbedding(info)
	embedding := extractEmbedding(params, info.SourceID)
	row.Dimension = len(embedding)
	return r.saveRowsAtomic(ctx, []*types.IndexInfo{info}, []*sqliteEmbedding{row}, [][]float32{embedding})
}

func (r *sqliteRepository) BatchSave(ctx context.Context, infos []*types.IndexInfo, params map[string]any) error {
	if len(infos) == 0 {
		return nil
	}
	rows := make([]*sqliteEmbedding, len(infos))
	embs := make([][]float32, len(infos))
	for i, info := range infos {
		if info == nil {
			return fmt.Errorf("nil index info")
		}
		rows[i] = toSQLiteEmbedding(info)
		embs[i] = extractEmbedding(params, info.SourceID)
		rows[i].Dimension = len(embs[i])
	}
	return r.saveRowsAtomic(ctx, infos, rows, embs)
}

func (r *sqliteRepository) EstimateStorageSize(
	_ context.Context,
	indexInfoList []*types.IndexInfo,
	_ map[string]any,
) int64 {
	var total int64
	for _, info := range indexInfoList {
		total += int64(len(info.Content)) + 200
	}
	return total
}

func (r *sqliteRepository) DeleteByChunkIDList(ctx context.Context, ids []string, _ int, _ string) error {
	return r.mutateRowsAtomic(ctx, "chunk_id IN ?", ids, nil)
}

func (r *sqliteRepository) DeleteBySourceIDList(ctx context.Context, ids []string, _ int, _ string) error {
	return r.mutateRowsAtomic(ctx, "source_id IN ?", ids, nil)
}

func (r *sqliteRepository) DeleteByKnowledgeIDList(ctx context.Context, ids []string, _ int, _ string) error {
	return r.mutateRowsAtomic(ctx, "knowledge_id IN ?", ids, nil)
}

func (r *sqliteRepository) CopyIndices(
	ctx context.Context,
	_ string,
	knowledgeMap, chunkMap map[string]string,
	kbID string,
	_ int,
	_ string,
) error {
	return r.copyRowsAtomic(ctx, knowledgeMap, chunkMap, kbID)
}

func (r *sqliteRepository) BatchUpdateChunkEnabledStatus(ctx context.Context, statuses map[string]bool) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for id, value := range statuses {
			if e := r.mutateRowsInTx(ctx, tx, "chunk_id=?", id, map[string]any{"is_enabled": value}); e != nil {
				return e
			}
		}
		return nil
	})
}

func (r *sqliteRepository) BatchUpdateChunkTagID(ctx context.Context, tags map[string]string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for id, value := range tags {
			if e := r.mutateRowsInTx(ctx, tx, "chunk_id=?", id, map[string]any{"tag_id": value}); e != nil {
				return e
			}
		}
		return nil
	})
}

// --- Retrieve ---

func (r *sqliteRepository) Retrieve(ctx context.Context, params types.RetrieveParams) ([]*types.RetrieveResult, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}
	var results []*types.RetrieveResult

	if params.RetrieverType == types.KeywordsRetrieverType || params.RetrieverType == "" {
		res, err := r.keywordsRetrieve(ctx, params)
		if err != nil {
			return nil, err
		}
		results = append(results, res...)
	}

	if params.RetrieverType == types.VectorRetrieverType || params.RetrieverType == "" {
		res, err := r.vectorRetrieve(ctx, params)
		if err != nil {
			return nil, err
		}
		results = append(results, res...)
	}

	return results, nil
}

// --- Keywords retrieval via FTS5 ---
func (r *sqliteRepository) keywordsRetrieve(
	ctx context.Context,
	params types.RetrieveParams,
) ([]*types.RetrieveResult, error) {
	if params.Query == "" {
		return nil, nil
	}

	ftsQuery := sanitizeFTS5Query(params.Query)

	sql := `
		SELECT e.id, e.source_id, e.source_type, e.chunk_id,
			e.knowledge_id, e.knowledge_base_id, e.tag_id,
			e.content,
			(bm25(lite_embeddings_fts) * -1000000.0) AS score
		FROM lite_embeddings_fts
		JOIN lite_embeddings e ON e.id = lite_embeddings_fts.rowid
		WHERE lite_embeddings_fts MATCH ?
		AND (e.is_enabled IS NULL OR e.is_enabled = 1)
	`

	args := []interface{}{ftsQuery}

	for _, wp := range buildFilterWhere(params, "e") {
		sql += " AND " + wp.clause
		args = append(args, wp.args...)
	}

	sql += " ORDER BY score DESC LIMIT ?"
	args = append(args, params.TopK)

	type ftsResult struct {
		ID              uint
		SourceID        string
		SourceType      int
		ChunkID         string
		KnowledgeID     string
		KnowledgeBaseID string
		TagID           string
		Content         string
		Score           float64
	}

	var rows []ftsResult
	if err := r.db.WithContext(ctx).Raw(sql, args...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("FTS5 query failed: %w", err)
	}

	logger.GetLogger(ctx).Infof("[SQLite] keywordsRetrieve: matched=%d rows", len(rows))

	items := make([]*types.IndexWithScore, len(rows))
	for i, row := range rows {

		// bm25 is originally negative and very small, we multiplied it by -1000000.0 in SQL
		// to make it positive and human-readable.
		score := row.Score

		logger.GetLogger(ctx).Infof("[SQLite] keywordsRetrieve: #%d chunk_id=%s, score=%.4f",
			i+1, row.ChunkID, score)

		items[i] = &types.IndexWithScore{
			ID:              fmt.Sprintf("%d", row.ID),
			SourceID:        row.SourceID,
			SourceType:      types.SourceType(row.SourceType),
			ChunkID:         row.ChunkID,
			KnowledgeID:     row.KnowledgeID,
			KnowledgeBaseID: row.KnowledgeBaseID,
			TagID:           row.TagID,
			Content:         row.Content,
			Score:           score,
			MatchType:       types.MatchTypeKeywords,
		}
	}

	return []*types.RetrieveResult{{
		Results:             items,
		RetrieverEngineType: types.SQLiteRetrieverEngineType,
		RetrieverType:       types.KeywordsRetrieverType,
	}}, nil
}

func (r *sqliteRepository) vectorRetrieve(
	ctx context.Context,
	params types.RetrieveParams,
) ([]*types.RetrieveResult, error) {
	if len(params.Embedding) == 0 {
		return nil, nil
	}

	dim := len(params.Embedding)
	r.ensureVecTable(dim)

	queryBlob, err := sqlite_vec.SerializeFloat32(params.Embedding)
	if err != nil {
		return nil, fmt.Errorf("serialize query vector failed: %w", err)
	}

	tbl := vecTableName(dim)

	// ⚠️ sqlite-vec 要求必须有 k = ?
	vecSQL := fmt.Sprintf(`
		SELECT v.rowid, v.distance,
			e.source_id, e.source_type, e.chunk_id,
			e.knowledge_id, e.knowledge_base_id,
			e.tag_id, e.content
		FROM %s v
		JOIN lite_embeddings e ON e.id = v.rowid
		WHERE v.embedding MATCH ?
		AND k = ?
		AND v.rowid IN (
			SELECT filtered.id
			FROM lite_embeddings filtered
			WHERE (filtered.is_enabled IS NULL OR filtered.is_enabled = 1)
	`, tbl)

	args := []interface{}{
		queryBlob,
		params.TopK, // 这里就是 k
	}

	// 追加过滤条件
	for _, wp := range buildFilterWhere(params, "filtered") {
		vecSQL += " AND " + wp.clause
		args = append(args, wp.args...)
	}

	// ⚠️ 这里仍然建议加 ORDER BY，虽然 vec0 已经按距离返回
	vecSQL += ") ORDER BY v.distance ASC"

	type row struct {
		Rowid           uint
		Distance        float64
		SourceID        string
		SourceType      int
		ChunkID         string
		KnowledgeID     string
		KnowledgeBaseID string
		TagID           string
		Content         string
	}

	var rows []row
	if err := r.db.WithContext(ctx).
		Raw(vecSQL, args...).
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("sqlite-vec query failed: %w", err)
	}

	logger.GetLogger(ctx).
		Infof("[SQLite] vectorRetrieve: query_dim=%d, threshold=%.4f, matched=%d rows", dim, params.Threshold, len(rows))

	items := make([]*types.IndexWithScore, 0, len(rows))

	for i, v := range rows {
		// cosine distance = 1 - cosine_similarity
		score := 1 - v.Distance
		if params.Threshold > 0 && score < params.Threshold {
			continue
		}

		logger.GetLogger(ctx).Infof("[SQLite] vectorRetrieve: #%d chunk_id=%s, distance=%.4f, score=%.4f",
			i+1, v.ChunkID, v.Distance, score)

		items = append(items, &types.IndexWithScore{
			ID:              fmt.Sprintf("%d", v.Rowid),
			SourceID:        v.SourceID,
			SourceType:      types.SourceType(v.SourceType),
			ChunkID:         v.ChunkID,
			KnowledgeID:     v.KnowledgeID,
			KnowledgeBaseID: v.KnowledgeBaseID,
			TagID:           v.TagID,
			Content:         v.Content,
			Score:           score,
			MatchType:       types.MatchTypeEmbedding,
		})
	}

	return []*types.RetrieveResult{{
		Results:             items,
		RetrieverEngineType: types.SQLiteRetrieverEngineType,
		RetrieverType:       types.VectorRetrieverType,
	}}, nil
}

// --- Internal helpers ---

func toSQLiteEmbedding(info *types.IndexInfo) *sqliteEmbedding {
	enabled := info.IsEnabled
	return &sqliteEmbedding{
		SourceID:        info.SourceID,
		SourceType:      int(info.SourceType),
		ChunkID:         info.ChunkID,
		KnowledgeID:     info.KnowledgeID,
		KnowledgeBaseID: info.KnowledgeBaseID,
		TagID:           info.TagID,
		Content:         common.CleanInvalidUTF8(info.Content),
		Dimension:       0,
		IsEnabled:       &enabled,
	}
}

func extractEmbedding(params map[string]any, sourceID string) []float32 {
	if params == nil {
		return nil
	}
	embMap, ok := params["embedding"].(map[string][]float32)
	if !ok {
		return nil
	}
	return embMap[sourceID]
}

type whereClause struct {
	clause string
	args   []interface{}
}

func buildFilterWhere(params types.RetrieveParams, tableAlias string) []whereClause {
	var parts []whereClause
	if len(params.KnowledgeBaseIDs) > 0 {
		parts = append(parts, whereClause{
			clause: tableAlias + ".knowledge_base_id IN (" + placeholders(len(params.KnowledgeBaseIDs)) + ")",
			args:   toInterfaceSlice(params.KnowledgeBaseIDs),
		})
	}
	if len(params.KnowledgeIDs) > 0 {
		parts = append(parts, whereClause{
			clause: tableAlias + ".knowledge_id IN (" + placeholders(len(params.KnowledgeIDs)) + ")",
			args:   toInterfaceSlice(params.KnowledgeIDs),
		})
	}
	if len(params.TagIDs) > 0 {
		parts = append(parts, whereClause{
			clause: tableAlias + ".tag_id IN (" + placeholders(len(params.TagIDs)) + ")",
			args:   toInterfaceSlice(params.TagIDs),
		})
	}
	return parts
}

func placeholders(n int) string {
	p := make([]string, n)
	for i := range p {
		p[i] = "?"
	}
	return strings.Join(p, ",")
}

func toInterfaceSlice(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// tokenizeCJKBigram splits continuous CJK character sequences into overlapping bigrams.
// Non-CJK words are kept intact. This approach maximizes recall for CJK search.
func tokenizeCJKBigram(text string) string {
	var parts []string
	var currentCJK []rune
	var currentNonCJK strings.Builder

	flushCJK := func() {
		if len(currentCJK) == 0 {
			return
		}
		if len(currentCJK) == 1 {
			parts = append(parts, string(currentCJK[0]))
		} else {
			for i := 0; i < len(currentCJK)-1; i++ {
				parts = append(parts, string(currentCJK[i])+string(currentCJK[i+1]))
			}
		}
		currentCJK = currentCJK[:0]
	}

	flushNonCJK := func() {
		if currentNonCJK.Len() > 0 {
			parts = append(parts, currentNonCJK.String())
			currentNonCJK.Reset()
		}
	}

	for _, r := range text {
		// unicode.Han covers Chinese characters
		if unicode.Is(unicode.Han, r) {
			flushNonCJK()
			currentCJK = append(currentCJK, r)
		} else if unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r) {
			// Delimiters
			flushCJK()
			flushNonCJK()
		} else {
			// Alphanumeric or other languages
			flushCJK()
			currentNonCJK.WriteRune(r)
		}
	}
	flushCJK()
	flushNonCJK()

	return strings.Join(parts, " ")
}

// sanitizeFTS5Query builds an FTS5 query from user input by applying bigram tokenization.
func sanitizeFTS5Query(q string) string {
	q = strings.TrimSpace(q)
	if q == "" {
		return q
	}

	// Tokenize input query with bigrams (same as during indexing)
	tokenized := tokenizeCJKBigram(q)
	fields := strings.Fields(tokenized)

	var parts []string
	for _, f := range fields {
		if f != "" {
			parts = append(parts, `"`+f+`"`)
		}
	}

	if len(parts) == 0 {
		return ""
	}

	// Use OR because we want fuzzy match across multiple bigrams/words
	return strings.Join(parts, " OR ")
}
