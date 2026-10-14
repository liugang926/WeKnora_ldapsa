package types

import (
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func lineageTestIdentity(fileID, etag string) SourceIdentity {
	return SourceIdentity{
		Provider: ConnectorTypeNextcloud, TenantID: 7,
		KnowledgeBaseID: "kb", DataSourceID: "ds", PairOperationID: "pair",
		InstanceID: "instance", BindingID: "binding", FileID: fileID,
		ExternalID: "nextcloud:instance:" + fileID, KnowledgeID: "knowledge-" + fileID,
		Revision: 1, ETag: etag,
	}
}

func TestSourceLineageUnionKeepsOriginalRevisionsAndUnknown(t *testing.T) {
	a, err := NewCompleteSourceLineage(lineageTestIdentity("1", `"etag-original"`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewCompleteSourceLineage(lineageTestIdentity("1", `"etag-new"`), lineageTestIdentity("2", "etag-2"))
	if err != nil {
		t.Fatal(err)
	}
	ab, err := UnionSourceLineage(a, b, a)
	if err != nil {
		t.Fatal(err)
	}
	ba, err := UnionSourceLineage(b, a)
	if err != nil {
		t.Fatal(err)
	}
	if len(ab.Sources) != 3 || !reflect.DeepEqual(ab, ba) || ab.RequireComplete() != nil {
		t.Fatalf("union lost revision or was nondeterministic: %#v / %#v", ab, ba)
	}
	unknown, err := UnionSourceLineage(a, nil, b)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(unknown.RequireComplete(), ErrSourceLineageUnknown) || len(unknown.Sources) != 3 {
		t.Fatal("unknown must dominate without losing known dependencies")
	}
	ab.Sources[0].ETag = "mutated"
	if a.Sources[0].ETag != `"etag-original"` {
		t.Fatal("union aliases its inputs")
	}
	empty, err := NewCompleteSourceLineage()
	if err != nil || empty.RequireComplete() != nil || empty.Sources == nil {
		t.Fatal("explicit complete empty rejected")
	}
	absent, err := UnionSourceLineage()
	if err != nil || !errors.Is(absent.RequireComplete(), ErrSourceLineageUnknown) {
		t.Fatal("absence was certified complete")
	}
}

func TestSourceLineageInvalidInputCannotRetainCompleteState(t *testing.T) {
	good, err := NewCompleteSourceLineage(lineageTestIdentity("1", "etag"))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SourceLineage){
		"unsupported version":  func(l *SourceLineage) { l.Version = 2 },
		"unsupported state":    func(l *SourceLineage) { l.State = "safe" },
		"missing set":          func(l *SourceLineage) { l.Sources = nil },
		"missing etag":         func(l *SourceLineage) { l.Sources[0].ETag = "" },
		"invalid utf8 etag":    func(l *SourceLineage) { l.Sources[0].ETag = string([]byte{0xff}) },
		"missing pairing":      func(l *SourceLineage) { l.Sources[0].PairOperationID = "" },
		"missing revision":     func(l *SourceLineage) { l.Sources[0].Revision = 0 },
		"aliased identity":     func(l *SourceLineage) { l.Sources[0].ExternalID = "nextcloud:other:1" },
		"noncanonical file id": func(l *SourceLineage) { l.Sources[0].FileID = "01" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := *good
			bad.Sources = append([]SourceIdentity{}, good.Sources...)
			mutate(&bad)
			out, err := UnionSourceLineage(good, &bad)
			if !errors.Is(err, ErrSourceLineageInvalid) || !errors.Is(out.RequireComplete(), ErrSourceLineageUnknown) {
				t.Fatalf("invalid lineage did not fail closed: %v / %#v", err, out)
			}
		})
	}
}

func TestSourceLineageCodecLegacyAndErrorsResetToUnknown(t *testing.T) {
	good, err := NewCompleteSourceLineage(lineageTestIdentity("1", "etag-original"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := good.Value()
	if err != nil {
		t.Fatal(err)
	}
	var loaded SourceLineage
	if err := loaded.Scan(encoded); err != nil || !reflect.DeepEqual(&loaded, good) {
		t.Fatal("byte codec round trip", err)
	}
	if err := loaded.Scan(string(encoded.([]byte))); err != nil {
		t.Fatal("text codec", err)
	}
	if err := loaded.Scan(nil); err != nil || !errors.Is(loaded.RequireComplete(), ErrSourceLineageUnknown) {
		t.Fatal("NULL must be unknown")
	}
	for _, raw := range []string{
		`null`, `{`, `{}`, `{"version":1,"state":"complete"}`, `{"version":1,"state":"complete","sources":null}`,
		`{"version":1,"state":"complete","sources":[],"unexpected":"field"}`,
		`{"version":1,"state":"complete","sources":[]} {}`,
	} {
		loaded = *good
		if err := loaded.Scan(raw); err == nil || !errors.Is(loaded.RequireComplete(), ErrSourceLineageUnknown) {
			t.Fatalf("malformed data retained complete lineage: %q", raw)
		}
	}
	loaded = *good
	if err := loaded.Scan(123); !errors.Is(err, ErrSourceLineageInvalid) || loaded.State != SourceLineageUnknown {
		t.Fatal("database type error retained state")
	}
	for _, fieldValue := range []string{"etag-original", "knowledge-1"} {
		invalidUTF8 := strings.Replace(string(encoded.([]byte)), fieldValue, string([]byte{0xff}), 1)
		if err := loaded.Scan(invalidUTF8); !errors.Is(err, ErrSourceLineageInvalid) ||
			loaded.State != SourceLineageUnknown {
			t.Fatal("database codec changed source identity/ETag by replacing invalid UTF-8")
		}
	}
	checkpoint := ContextCheckpoint{Summary: "source-derived summary", SourceLineage: good}
	encoded, err = checkpoint.Value()
	if err != nil {
		t.Fatal(err)
	}
	var cp ContextCheckpoint
	if err := cp.Scan(encoded); err != nil || !reflect.DeepEqual(cp.SourceLineage, good) {
		t.Fatal("checkpoint lineage codec", err)
	}
	if err := cp.Scan(`{"summary":"legacy"}`); err != nil || cp.SourceLineage != nil {
		t.Fatal("legacy checkpoint acquired lineage")
	}
	if err := cp.Scan(nil); err != nil || cp.Summary != "" || cp.SourceLineage != nil {
		t.Fatal("checkpoint NULL retained stale data")
	}
}

func TestSourceLineageDuplicateJSONKeysRemainUnknown(t *testing.T) {
	good, err := NewCompleteSourceLineage(lineageTestIdentity("1", "etag-original"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := good.Value()
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"unknown overwritten complete": `{"version":1,"state":"unknown","state":"complete","sources":[]}`,
		"escaped duplicate state":      `{"version":1,"state":"unknown","st\u0061te":"complete","sources":[]}`,
		"case alias state":             `{"version":1,"state":"unknown","STATE":"complete","sources":[]}`,
		"unicode alias state":          `{"version":1,"state":"unknown","\u017ftate":"complete","sources":[]}`,
		"duplicate version":            `{"version":2,"version":1,"state":"complete","sources":[]}`,
		"duplicate source set":         `{"version":1,"state":"complete","sources":[],"sources":[]}`,
		"duplicate original etag": strings.Replace(string(encoded.([]byte)), `"etag":"etag-original"`,
			`"etag":"old","etag":"new"`, 1),
		"duplicate knowledge id": strings.Replace(string(encoded.([]byte)),
			`"knowledge_id":"knowledge-1"`, `"knowledge_id":"old","knowledge_id":"new"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			loaded := *good
			if err := loaded.Scan(raw); !errors.Is(err, ErrSourceLineageInvalid) ||
				loaded.State != SourceLineageUnknown {
				t.Fatalf("ambiguous envelope became complete: %v / %#v", err, loaded)
			}
		})
	}
}

func TestSourceLineageBoundsNeverTruncateACompleteSet(t *testing.T) {
	sources := make([]SourceIdentity, SourceLineageMaxSources)
	for i := range sources {
		sources[i] = lineageTestIdentity(strconv.Itoa(i+1), "etag")
	}
	// Use two individually valid input envelopes so overflow occurs at union.
	left, err := NewCompleteSourceLineage(sources[:400]...)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewCompleteSourceLineage(sources[400:800]...)
	if err != nil {
		t.Fatal(err)
	}
	last, err := NewCompleteSourceLineage(sources[800:]...)
	if err != nil {
		t.Fatal(err)
	}
	// The byte bound may be reached before the count bound; both deny output.
	out, err := UnionSourceLineage(left, right, last)
	if !errors.Is(err, ErrSourceLineageLimit) || out.State != SourceLineageUnknown {
		t.Fatal("oversized set was certified", err)
	}
	oversize := &SourceLineage{Version: 1, State: SourceLineageComplete, Sources: make([]SourceIdentity,
		SourceLineageMaxSources+1)}
	if !errors.Is(oversize.Validate(), ErrSourceLineageLimit) {
		t.Fatal("source count bound missing")
	}
	constructed, err := NewCompleteSourceLineage(oversize.Sources...)
	if !errors.Is(err, ErrSourceLineageLimit) || constructed.State != SourceLineageUnknown {
		t.Fatal("oversized producer input was copied/certified", err)
	}
	loaded := *left
	if err := json.Unmarshal([]byte(strings.Repeat(" ", SourceLineageMaxBytes+1)), &loaded); err == nil {
		t.Fatal("oversized JSON accepted")
	}
	// Direct UnmarshalJSON is used by Scan; encoding/json rejects invalid JSON
	// before invoking it, so test the database boundary separately.
	if err := loaded.Scan(strings.Repeat(" ", SourceLineageMaxBytes+1)); !errors.Is(err,
		ErrSourceLineageLimit) || loaded.State != SourceLineageUnknown {
		t.Fatal("oversized database envelope retained readable state")
	}
}

func TestSourceLineageRemainsInternalInPublicMessageJSON(t *testing.T) {
	lineage, err := NewCompleteSourceLineage(lineageTestIdentity("1", "private-etag"))
	if err != nil {
		t.Fatal(err)
	}
	message := Message{
		ID: "answer", Role: "assistant", Content: "safe answer",
		SourceLineage: lineage, ContextCheckpoint: &ContextCheckpoint{
			Summary:       "private summary",
			SourceLineage: lineage,
		},
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{
		"source_lineage", "context_checkpoint", "private-etag",
		"private summary", "pair_operation_id",
	} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("public Message JSON exposed %q", private)
		}
	}
	// The same metadata must still survive the independent persistence codec.
	persisted, err := message.ContextCheckpoint.Value()
	if err != nil || !strings.Contains(string(persisted.([]byte)), "private-etag") {
		t.Fatal("checkpoint persistence lost lineage", err)
	}
}

func TestSourceLineageNullableMessageFieldPersistsThroughGORM(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.Exec(`CREATE TABLE messages (id TEXT PRIMARY KEY,
		source_lineage TEXT, created_at DATETIME, updated_at DATETIME, deleted_at DATETIME)`).Error; err != nil {
		t.Fatal(err)
	}
	lineage, err := NewCompleteSourceLineage(lineageTestIdentity("1", "original-etag"))
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []Message{{ID: "legacy"}, {ID: "typed", SourceLineage: lineage}} {
		if err := db.Select("id", "source_lineage").Create(&message).Error; err != nil {
			t.Fatal(err)
		}
		var loaded Message
		if err := db.Select("id", "source_lineage").First(&loaded, "id = ?", message.ID).Error; err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(message.SourceLineage, loaded.SourceLineage) {
			t.Fatal("nullable Message codec changed lineage")
		}
	}
	if err := db.Exec(`INSERT INTO messages(id, source_lineage) VALUES ('ambiguous',
		'{"version":1,"state":"unknown","state":"complete","sources":[]}')`).Error; err != nil {
		t.Fatal(err)
	}
	var malformed Message
	if err := db.Select("id", "source_lineage").First(&malformed, "id = 'ambiguous'").Error; err == nil ||
		(malformed.SourceLineage != nil && malformed.SourceLineage.State == SourceLineageComplete) {
		t.Fatal("GORM accepted ambiguous persisted provenance", err)
	}
}
