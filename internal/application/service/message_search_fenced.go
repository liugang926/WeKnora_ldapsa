package service

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

const (
	historySearchCandidateBudget = 200
	historySearchMaximumResults  = 100
)

var errHistorySearchPageMaterialBudget = errors.New("history search page material budget exhausted")

type historySearchCandidateReader interface {
	OwnedHistorySearchCandidatesByIDs(
		context.Context,
		uint64,
		string,
		[]string,
		[]string,
	) ([]*types.MessageSearchCandidate, error)
	ListOwnedHistorySearchCandidates(
		context.Context,
		uint64,
		string,
		[]string,
		time.Time,
		string,
		int,
	) ([]*types.MessageSearchCandidate, error)
	HistorySearchPartnerCandidates(
		context.Context,
		uint64,
		string,
		*types.MessageSearchCandidate,
	) ([]*types.MessageSearchCandidate, error)
	OwnedHistorySearchCandidatesByKnowledgeIDs(
		context.Context,
		uint64,
		string,
		[]string,
		[]string,
		int,
	) ([]*types.MessageSearchCandidate, error)
}

type historySearchCursor struct {
	Version    int                     `json:"v"`
	Tenant     uint64                  `json:"tenant"`
	Owner      string                  `json:"owner"`
	QueryHash  string                  `json:"query_hash"`
	Mode       types.MessageSearchMode `json:"mode"`
	SessionIDs []string                `json:"session_ids"`
	Before     time.Time               `json:"before"`
	BeforeID   string                  `json:"before_id"`
	PendingIDs []string                `json:"pending_ids,omitempty"`
}

type historyKeywordPage struct {
	Hits       []*types.MessageWithSession
	Pairs      map[string][]*types.Message
	NextCursor string
	HasMore    bool
	Cursor     historySearchCursor
}

func canonicalHistorySearchSessions(ids []string) []string {
	values := append([]string{}, ids...)
	sort.Strings(values)
	out := values[:0]
	for _, id := range values {
		if id != "" && (len(out) == 0 || out[len(out)-1] != id) {
			out = append(out, id)
		}
	}
	return out
}

func historySearchCursorBinding(ctx context.Context, params *types.MessageSearchParams) historySearchCursor {
	hash := sha256.Sum256([]byte(params.Query))
	return historySearchCursor{
		Version: 1,
		Tenant:  types.MustTenantIDFromContext(ctx),
		Owner:   params.OwnerID,
		QueryHash: hex.EncodeToString(
			hash[:],
		),
		Mode:       params.Mode,
		SessionIDs: canonicalHistorySearchSessions(params.SessionIDs),
	}
}

// This cursor is a navigation position, never an authorization receipt. All
// pages query the current authenticated owner and obtain new original guards.
func decodeHistorySearchCursor(ctx context.Context, params *types.MessageSearchParams) (historySearchCursor, error) {
	expected := historySearchCursorBinding(ctx, params)
	if params.Cursor == "" {
		return expected, nil
	}
	if len(params.Cursor) > 32768 {
		return expected, access.ErrNextcloudPublicationDenied
	}
	body, err := base64.RawURLEncoding.DecodeString(params.Cursor)
	if err != nil {
		return expected, access.ErrNextcloudPublicationDenied
	}
	var actual historySearchCursor
	if err = json.Unmarshal(body, &actual); err != nil {
		return expected, access.ErrNextcloudPublicationDenied
	}
	left, _ := json.Marshal(expected.SessionIDs)
	right, _ := json.Marshal(actual.SessionIDs)
	if actual.Version != expected.Version || actual.Tenant != expected.Tenant || actual.Owner != expected.Owner ||
		actual.QueryHash != expected.QueryHash ||
		actual.Mode != expected.Mode ||
		string(left) != string(right) ||
		actual.Before.IsZero() ||
		actual.BeforeID == "" ||
		len(actual.BeforeID) > 128 ||
		len(actual.PendingIDs) > historySearchMaximumResults {
		return expected, access.ErrNextcloudPublicationDenied
	}
	return actual, nil
}

func historySearchPairKey(message *types.Message) string {
	if message.RequestID == "" {
		return searchPairKey(message.SessionID, "message:"+message.ID)
	}
	return searchPairKey(message.SessionID, "request:"+message.RequestID)
}

func historySearchCandidateDenied(err error) bool {
	return errors.Is(err, access.ErrNextcloudPublicationDenied) || errors.Is(err, types.ErrSourceLineageUnknown) ||
		errors.Is(err, repository.ErrKnowledgeInputChanged) ||
		errors.Is(err, gorm.ErrRecordNotFound)
}

// Read only the exact, small exchange after its immutable producer metadata
// has supplied the real source/body generations and the read guard has taken
// their leases. There is no SQL body lookup before this admission.
func (s *messageService) beginHistorySearchPair(
	ctx context.Context,
	params *types.MessageSearchParams,
	candidate *types.MessageSearchCandidate,
	remaining *int,
) (*MessageMaterialGuard, *types.Message, error) {
	reader, ok := s.messageRepo.(historySearchCandidateReader)
	if !ok || s.materialStore == nil || s.sourceLineageGuard == nil {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	metadata, err := reader.HistorySearchPartnerCandidates(
		ctx,
		types.MustTenantIDFromContext(ctx),
		params.OwnerID,
		candidate,
	)
	if err != nil {
		return nil, nil, err
	}
	if len(metadata) == 0 {
		return nil, nil, access.ErrNextcloudPublicationDenied
	}
	if len(metadata) > types.MessageSearchMaximumExchangeMessages {
		return nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	if len(metadata) > *remaining {
		return nil, nil, errHistorySearchPageMaterialBudget
	}
	*remaining -= len(metadata)
	request := types.MessageMaterialReadRequest{
		TenantID:  types.MustTenantIDFromContext(ctx),
		SessionID: candidate.SessionID,
	}
	assistantCount := 0
	seen := make(map[string]bool)
	for _, row := range metadata {
		if row == nil || row.SessionID != candidate.SessionID || row.RequestID != candidate.RequestID || seen[row.ID] ||
			(row.Role != "user" && row.Role != "assistant") {
			return nil, nil, access.ErrNextcloudPublicationDenied
		}
		seen[row.ID] = true
		if row.Role == "assistant" {
			assistantCount++
			if assistantCount > 1 {
				return nil, nil, access.ErrNextcloudPublicationDenied
			}
		}
		request.MessageIDs = append(request.MessageIDs, row.ID)
	}
	guard, err := s.beginMessageMaterialRead(ctx, request)
	if err != nil {
		return nil, nil, err
	}
	for _, row := range guard.snapshot.Messages {
		if row != nil && row.ID == candidate.ID {
			if row.SessionID != candidate.SessionID || row.RequestID != candidate.RequestID ||
				row.Role != candidate.Role ||
				!row.CreatedAt.Equal(candidate.CreatedAt) {
				guard.Close()
				return nil, nil, access.ErrNextcloudPublicationDenied
			}
			return guard, row, nil
		}
	}
	guard.Close()
	return nil, nil, access.ErrNextcloudPublicationDenied
}

func (s *messageService) fencedKeywordHistorySearch(
	ctx context.Context,
	params *types.MessageSearchParams,
) (*historyKeywordPage, error) {
	reader, ok := s.messageRepo.(historySearchCandidateReader)
	if !ok || s.materialStore == nil || s.sourceLineageGuard == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	cursor, err := decodeHistorySearchCursor(ctx, params)
	if err != nil {
		return nil, err
	}
	var pending []*types.MessageSearchCandidate
	if len(cursor.PendingIDs) > 0 {
		pending, err = reader.OwnedHistorySearchCandidatesByIDs(
			ctx,
			cursor.Tenant,
			cursor.Owner,
			params.SessionIDs,
			cursor.PendingIDs,
		)
		if err != nil {
			return nil, err
		}
	}
	normal, err := reader.ListOwnedHistorySearchCandidates(
		ctx,
		cursor.Tenant,
		cursor.Owner,
		params.SessionIDs,
		cursor.Before,
		cursor.BeforeID,
		historySearchCandidateBudget+1-len(pending),
	)
	if err != nil {
		return nil, err
	}
	candidates := append(append([]*types.MessageSearchCandidate{}, pending...), normal...)
	page := &historyKeywordPage{Pairs: make(map[string][]*types.Message)}
	processed := 0
	remaining := historySearchCandidateBudget
	needle := strings.ToLower(params.Query)
	for index, candidate := range candidates {
		if index >= historySearchCandidateBudget || len(page.Hits) >= params.Limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		guard, original, err := s.beginHistorySearchPair(ctx, params, candidate, &remaining)
		if errors.Is(err, errHistorySearchPageMaterialBudget) {
			break
		}
		processed = index + 1
		if err != nil {
			if historySearchCandidateDenied(err) {
				continue
			}
			return nil, err
		}
		rows, retained, err := s.retainAgentConversationCandidate(ctx, guard)
		if err != nil {
			guard.Close()
			return nil, err
		}
		if !strings.Contains(strings.ToLower(original.Content), needle) {
			if !retained {
				guard.Close()
			}
			continue
		}
		// When both original fields match, only the newer matching row emits
		// the pair. This prevents duplicate exchanges across candidate pages
		// without keeping an unbounded client/server seen-message inventory.
		duplicate := false
		for _, partner := range guard.snapshot.Messages {
			if partner.ID != original.ID && strings.Contains(strings.ToLower(partner.Content), needle) &&
				(partner.CreatedAt.After(original.CreatedAt) ||
					(partner.CreatedAt.Equal(original.CreatedAt) && partner.ID > original.ID)) {
				duplicate = true
				break
			}
		}
		if duplicate {
			if !retained {
				guard.Close()
			}
			continue
		}
		if !retained {
			rows, err = s.bindAdmittedMessageGuard(ctx, guard, guard.snapshot.Request)
			if err != nil {
				return nil, err
			}
		}
		for _, row := range rows {
			if row.ID == original.ID {
				page.Hits = append(page.Hits, &types.MessageWithSession{Message: *row})
				page.Pairs[historySearchPairKey(row)] = rows
			}
		}
	}
	page.HasMore = processed < len(candidates)
	cursor.PendingIDs = nil
	for index := processed; index < len(pending); index++ {
		cursor.PendingIDs = append(cursor.PendingIDs, pending[index].ID)
	}
	if processed > len(pending) {
		last := candidates[processed-1]
		cursor.Before, cursor.BeforeID = last.CreatedAt, last.ID
	}
	page.Cursor = cursor
	return page, nil
}

// Expand partners only from the already-held original snapshots. Raw
// GetMessagesByRequestIDs would reintroduce an unfenced SQL body read here.
func expandHeldHistoricalSearchPairs(
	items []*types.MessageSearchResultItem,
	pairs map[string][]*types.Message,
) ([]*types.MessageSearchResultItem, error) {
	seen := make(map[string]bool)
	for _, item := range items {
		seen[item.ID] = true
	}
	out := append([]*types.MessageSearchResultItem{}, items...)
	for _, item := range items {
		rows, ok := pairs[historySearchPairKey(&item.Message)]
		if !ok {
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		for _, row := range rows {
			if !seen[row.ID] {
				seen[row.ID] = true
				out = append(
					out,
					&types.MessageSearchResultItem{MessageWithSession: types.MessageWithSession{Message: *row}},
				)
			}
		}
	}
	return out, nil
}

// Only a keyword hit that passed the final projection filter may be retried
// because ranking/limit omitted it. Closed projections are consumed metadata,
// not pending matches: retrying them can monopolize every subsequent page.
func keepEligibleHistoricalCursorHits(page *historyKeywordPage, items []*types.MessageSearchResultItem) {
	if page == nil {
		return
	}
	eligible := make(map[string]bool)
	for _, item := range items {
		eligible[historySearchPairKey(&item.Message)] = true
	}
	kept := page.Hits[:0]
	for _, hit := range page.Hits {
		if eligible[historySearchPairKey(&hit.Message)] {
			kept = append(kept, hit)
		}
	}
	page.Hits = kept
}

// Hybrid ranking can use the first-page vector hits ahead of a keyword hit.
// Carry only omitted keyword control IDs (no text) into the next page so a
// result limit cannot silently discard already-scanned legitimate matches.
func completeHistorySearchCursor(page *historyKeywordPage, result *types.MessageSearchResult) error {
	if page == nil {
		return nil
	}
	returned := make(map[string]bool)
	for _, group := range result.Items {
		key := searchPairKey(group.SessionID, "request:"+group.RequestID)
		if group.RequestID == "" {
			key = searchPairKey(group.SessionID, "message:"+group.MessageID)
		}
		returned[key] = true
	}
	for _, hit := range page.Hits {
		if !returned[historySearchPairKey(&hit.Message)] {
			page.Cursor.PendingIDs = append(page.Cursor.PendingIDs, hit.ID)
		}
	}
	result.HasMore = page.HasMore || len(page.Cursor.PendingIDs) != 0
	if !result.HasMore {
		return nil
	}
	if page.Cursor.Before.IsZero() || page.Cursor.BeforeID == "" {
		return access.ErrNextcloudPublicationUnavailable
	}
	body, err := json.Marshal(page.Cursor)
	if err != nil {
		return err
	}
	result.NextCursor = base64.RawURLEncoding.EncodeToString(body)
	return nil
}
