package service

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/types"
)

type historyVectorBoundaryReranker struct {
	call func(context.Context, string, []string) ([]rerank.RankResult, error)
}

func (r historyVectorBoundaryReranker) GetModelID() string   { return "controlled" }
func (r historyVectorBoundaryReranker) GetModelName() string { return "controlled" }
func (r historyVectorBoundaryReranker) Rerank(ctx context.Context, q string, d []string) ([]rerank.RankResult, error) {
	return r.call(ctx, q, d)
}

func TestHistoryVectorRerankBoundaryChecksLookupAndCall(t *testing.T) {
	var events []string
	results := []*types.SearchResult{{ID: "c1", KnowledgeID: "k1", KnowledgeBaseID: "kb", Content: "actual child text", Score: .4}}
	check := func() error { events = append(events, "check"); return nil }
	load := func(ctx context.Context, id string) (rerank.Reranker, error) {
		events = append(events, "lookup")
		if !logprivacy.ContentSuppressed(ctx) {
			t.Fatal("lookup must suppress content")
		}
		return historyVectorBoundaryReranker{call: func(ctx context.Context, q string, d []string) ([]rerank.RankResult, error) {
			events = append(events, "rerank")
			if !logprivacy.ContentSuppressed(ctx) || q != "query" || !reflect.DeepEqual(d, []string{"actual child text"}) {
				t.Fatal("model boundary changed original input")
			}
			return []rerank.RankResult{{Index: 0, RelevanceScore: .9, Document: rerank.DocumentInfo{Text: "provider replacement must be ignored"}}}, nil
		}}, nil
	}
	out, err := runHistoryVectorRerank(context.Background(), &types.RetrievalConfig{RerankModelID: "r", RerankThreshold: .2}, "query", results, []string{"actual child text"}, check, load)
	if err != nil || len(out) != 1 || out[0].Content != "actual child text" || out[0].Score != .9 || out[0] == results[0] || results[0].Score != .4 {
		t.Fatalf("authorized projection mismatch: rows=%d err=%v", len(out), err)
	}
	wanted := []string{"check", "lookup", "check", "check", "rerank", "check", "check"}
	if !reflect.DeepEqual(events, wanted) {
		t.Fatalf("boundary order: %v", events)
	}
}

func TestHistoryVectorRerankBoundaryRejectsRevocationAndLookupMutation(t *testing.T) {
	for _, test := range []struct {
		name        string
		afterLookup bool
		afterCall   bool
		mutate      bool
		callMutate  bool
		lookupError bool
		callError   bool
	}{
		{name: "revoked before lookup"},
		{name: "revoked by lookup", afterLookup: true},
		{name: "lookup error still checks revocation", afterLookup: true, lookupError: true},
		{name: "lookup mutates actual input", afterLookup: true, mutate: true},
		{name: "revoked by model", afterCall: true},
		{name: "model mutates actual input", afterCall: true, callMutate: true},
		{name: "provider error still checks revocation", afterCall: true, callError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			results := []*types.SearchResult{{ID: "c", KnowledgeID: "k", Content: "actual"}}
			current := test.afterLookup || test.afterCall
			lookups, calls, checks := 0, 0, 0
			check := func() error {
				checks++
				if !current {
					return access.ErrNextcloudPublicationDenied
				}
				return nil
			}
			load := func(context.Context, string) (rerank.Reranker, error) {
				lookups++
				if test.mutate {
					results[0].Content = "unproved replacement"
				} else if test.afterLookup {
					current = false
				}
				if test.lookupError {
					return nil, errors.New("controlled lookup failure")
				}
				return historyVectorBoundaryReranker{call: func(context.Context, string, []string) ([]rerank.RankResult, error) {
					calls++
					if test.callMutate {
						results[0].Content = "unproved model replacement"
					} else if test.afterCall {
						current = false
					}
					if test.callError {
						return nil, errors.New("controlled model failure")
					}
					return []rerank.RankResult{{Index: 0, RelevanceScore: 1}}, nil
				}}, nil
			}
			out, err := runHistoryVectorRerank(context.Background(), &types.RetrievalConfig{RerankModelID: "r"}, "q", results, []string{"actual"}, check, load)
			want := access.ErrNextcloudPublicationDenied
			if test.mutate || test.callMutate {
				want = repository.ErrKnowledgeInputChanged
			}
			if !errors.Is(err, want) || len(out) != 0 {
				t.Fatalf("stale output accepted: err=%v", err)
			}
			if !test.afterLookup && !test.afterCall && lookups != 0 {
				t.Fatal("revocation must precede lookup")
			}
			if test.afterLookup && calls != 0 {
				t.Fatal("lookup revocation/mutation must precede model call")
			}
			if test.afterCall && (calls != 1 || checks < 4) {
				t.Fatalf("missing post-model current check: calls=%d checks=%d", calls, checks)
			}
		})
	}
}

func TestHistoryVectorRerankBoundaryRejectsMalformedProviderIndexes(t *testing.T) {
	for _, test := range []struct {
		name  string
		ranks []rerank.RankResult
	}{
		{"negative", []rerank.RankResult{{Index: -1, RelevanceScore: 1}}},
		{"out of range", []rerank.RankResult{{Index: 1, RelevanceScore: 1}}},
		{"duplicate", []rerank.RankResult{{Index: 0, RelevanceScore: 1}, {Index: 0, RelevanceScore: 1}}},
		{"NaN", []rerank.RankResult{{Index: 0, RelevanceScore: math.NaN()}}},
		{"infinity", []rerank.RankResult{{Index: 0, RelevanceScore: math.Inf(1)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			results := []*types.SearchResult{{ID: "c", KnowledgeID: "k", Content: "actual"}}
			load := func(context.Context, string) (rerank.Reranker, error) {
				return historyVectorBoundaryReranker{call: func(context.Context, string, []string) ([]rerank.RankResult, error) { return test.ranks, nil }}, nil
			}
			out, err := runHistoryVectorRerank(context.Background(), &types.RetrievalConfig{RerankModelID: "r"}, "q", results, []string{"actual"}, func() error { return nil }, load)
			if !errors.Is(err, access.ErrNextcloudPublicationUnavailable) || len(out) != 0 {
				t.Fatalf("invalid provider output accepted: %v", err)
			}
		})
	}
}

func TestHistoryVectorRerankBoundaryErrorsCannotBecomeEmptySuccess(t *testing.T) {
	for _, stage := range []string{"lookup", "model"} {
		t.Run(stage, func(t *testing.T) {
			checks := 0
			results := []*types.SearchResult{{ID: "c", KnowledgeID: "k", Content: "actual"}}
			load := func(context.Context, string) (rerank.Reranker, error) {
				if stage == "lookup" {
					return nil, errors.New("controlled failure")
				}
				return historyVectorBoundaryReranker{call: func(context.Context, string, []string) ([]rerank.RankResult, error) {
					return nil, errors.New("controlled failure")
				}}, nil
			}
			out, err := runHistoryVectorRerank(context.Background(), &types.RetrievalConfig{RerankModelID: "r"}, "q", results, []string{"actual"}, func() error { checks++; return nil }, load)
			if !errors.Is(err, access.ErrNextcloudPublicationUnavailable) || len(out) != 0 {
				t.Fatalf("failure became empty success: %v", err)
			}
			wanted := 2
			if stage == "model" {
				wanted = 4
			}
			if checks != wanted {
				t.Fatalf("error skipped current check: %d", checks)
			}
		})
	}
}

func TestHistoryVectorCandidateEmptyWhitelistNeverInvokesRetrieval(t *testing.T) {
	// An empty server-owned selector must stop before any registry/repository
	// or legacy Retrieve is available. There is no fabricated message proof.
	service := &knowledgeBaseService{}
	rows, err := service.SearchHistoryVectorCandidates(context.Background(), "kb", nil, types.SearchParams{})
	if err != nil || len(rows) != 0 {
		t.Fatalf("empty selector reached backend: %v", err)
	}
}
