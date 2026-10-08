package repository

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

func faqInputMetadata(entry *types.FAQEntryPayload) *types.FAQChunkMetadata {
	strategy := types.AnswerStrategyAll
	if entry.AnswerStrategy != nil && *entry.AnswerStrategy != "" {
		strategy = *entry.AnswerStrategy
	}
	meta := &types.FAQChunkMetadata{StandardQuestion: strings.TrimSpace(entry.StandardQuestion), SimilarQuestions: entry.SimilarQuestions, NegativeQuestions: entry.NegativeQuestions, Answers: entry.Answers, AnswerStrategy: strategy, Version: 1, Source: "faq"}
	meta.Sanitize()
	return meta
}
func faqOriginalContent(meta *types.FAQChunkMetadata, kb *types.KnowledgeBase) string {
	copy := *kb
	copy.EnsureDefaults()
	var b strings.Builder
	fmt.Fprintf(&b, "Q: %s\n", meta.StandardQuestion)
	if len(meta.SimilarQuestions) > 0 {
		b.WriteString("Similar Questions:\n")
		for _, q := range meta.SimilarQuestions {
			fmt.Fprintf(&b, "- %s\n", q)
		}
	}
	if copy.FAQConfig != nil && copy.FAQConfig.IndexMode == types.FAQIndexModeQuestionAnswer && len(meta.Answers) > 0 {
		b.WriteString("Answers:\n")
		for _, a := range meta.Answers {
			fmt.Fprintf(&b, "- %s\n", a)
		}
	}
	return b.String()
}
func faqComparable(c *types.Chunk) *types.Chunk {
	copy := *c
	copy.CreatedAt = time.Time{}
	copy.UpdatedAt = time.Time{}
	return &copy
}
func faqUnionStrings(a, b []string) []string {
	return types.SanitizeStrings(append(append([]string{}, a...), b...))
}
func faqManifestTag(snapshot *types.FAQInputSnapshot, entry *types.FAQEntryPayload) (string, error) {
	name := strings.TrimSpace(entry.TagName)
	if name == "" {
		name = types.UntaggedTagName
	}
	for _, tag := range snapshot.Tags {
		if entry.TagID > 0 && tag.SeqID == entry.TagID || entry.TagID == 0 && tag.Name == name {
			return tag.ID, nil
		}
	}
	return "", ErrKnowledgeInputChanged
}
func faqManifestFieldPatch(snapshot *types.FAQInputSnapshot, chunk *types.Chunk, patch types.FAQEntryFieldsUpdate) error {
	if patch.IsEnabled != nil {
		chunk.IsEnabled = *patch.IsEnabled
	}
	if patch.IsRecommended != nil {
		if *patch.IsRecommended {
			chunk.Flags = chunk.Flags.SetFlag(types.ChunkFlagRecommended)
		} else {
			chunk.Flags = chunk.Flags.ClearFlag(types.ChunkFlagRecommended)
		}
	}
	if patch.TagID != nil {
		chunk.TagID = ""
		if *patch.TagID > 0 {
			for _, tag := range snapshot.Tags {
				if tag.SeqID == *patch.TagID {
					chunk.TagID = tag.ID
				}
			}
			if chunk.TagID == "" {
				return ErrKnowledgeInputChanged
			}
		}
	}
	return nil
}

// The immutable original input and original catalog determine every permitted
// write. Rechecking arbitrary current text or accepting a projection cannot
// authorize a new body. Own later status updates are a separate exact case.
func validateFAQProducerOutputs(session *faqInputSessionRow, current *types.FAQInputSnapshot, outputs []*types.Chunk, create bool) error {
	var op types.FAQInputOperation
	if err := json.Unmarshal([]byte(session.InputBody), &op); err != nil {
		return err
	}
	var original types.FAQInputSnapshot
	if err := json.Unmarshal([]byte(session.OriginalSnapshot), &original); err != nil {
		return err
	}
	oldRows := map[string]*types.Chunk{}
	currentRows := map[string]*types.Chunk{}
	currentProofs := map[string]*types.FAQEntryInputProof{}
	for _, c := range original.Chunks {
		oldRows[c.ID] = c
	}
	for _, c := range current.Chunks {
		currentRows[c.ID] = c
	}
	for _, p := range current.Proofs {
		currentProofs[p.ChunkID] = p
	}
	for _, out := range outputs {
		if previous := currentRows[out.ID]; previous != nil && !create {
			if p := currentProofs[out.ID]; p != nil && p.SessionID == session.ID && previous.Status == int(types.ChunkStatusStored) && out.Status == int(types.ChunkStatusIndexed) {
				expected := faqComparable(previous)
				expected.Status = out.Status
				if faqInputEqual(expected, faqComparable(out)) && expected.SourceContent == out.SourceContent && expected.ContextHeader == out.ContextHeader {
					continue
				}
			}
		}
		old := oldRows[out.ID]
		var expected *types.Chunk
		var entry *types.FAQEntryPayload
		if create {
			if old != nil || (op.Kind != "create" && op.Kind != "import") {
				return ErrKnowledgeInputChanged
			}
			if op.Kind == "create" {
				entry = op.Entry
			} else if op.Batch != nil {
				for i := range op.Batch.Entries {
					if faqInputMetadata(&op.Batch.Entries[i]).StandardQuestion == faqChunkStandard(out) {
						entry = &op.Batch.Entries[i]
						break
					}
				}
			}
			if entry == nil {
				return ErrKnowledgeInputChanged
			}
			tag, err := faqManifestTag(current, entry)
			if err != nil {
				return err
			}
			expected = &types.Chunk{ID: out.ID, TenantID: session.TenantID, KnowledgeBaseID: session.KnowledgeBaseID, KnowledgeID: out.KnowledgeID, ChunkType: types.ChunkTypeFAQ, TagID: tag, IsEnabled: true, Flags: types.ChunkFlagRecommended, Status: int(types.ChunkStatusStored), IndexStatus: "ready"}
			if entry.ID != nil && *entry.ID > 0 {
				expected.SeqID = *entry.ID
			}
			if expected.SeqID != out.SeqID {
				return ErrKnowledgeInputChanged
			}
			if entry.IsEnabled != nil {
				expected.IsEnabled = *entry.IsEnabled
			}
			if entry.IsRecommended != nil && !*entry.IsRecommended {
				expected.Flags = 0
			}
			meta := faqInputMetadata(entry)
			if err := expected.SetFAQMetadata(meta); err != nil {
				return err
			}
			expected.Content = faqOriginalContent(meta, current.KnowledgeBase)
		} else {
			if old == nil {
				return ErrKnowledgeInputChanged
			}
			expected = faqComparable(old)
			switch op.Kind {
			case "update":
				if old.SeqID != op.EntrySeqID || op.Entry == nil {
					return ErrKnowledgeInputChanged
				}
				entry = op.Entry
				meta := faqInputMetadata(entry)
				before, err := old.FAQMetadata()
				if err != nil {
					return err
				}
				meta.Version = before.Version + 1
				if err := expected.SetFAQMetadata(meta); err != nil {
					return err
				}
				expected.Content = faqOriginalContent(meta, current.KnowledgeBase)
				expected.TagID = ""
				if entry.TagID > 0 {
					tag, err := faqManifestTag(current, entry)
					if err != nil {
						return err
					}
					expected.TagID = tag
				}
				if entry.IsEnabled != nil {
					expected.IsEnabled = *entry.IsEnabled
				}
				if entry.IsRecommended != nil {
					if *entry.IsRecommended {
						expected.Flags = expected.Flags.SetFlag(types.ChunkFlagRecommended)
					} else {
						expected.Flags = expected.Flags.ClearFlag(types.ChunkFlagRecommended)
					}
				}
			case "similar":
				if old.SeqID != op.EntrySeqID {
					return ErrKnowledgeInputChanged
				}
				meta, err := old.FAQMetadata()
				if err != nil {
					return err
				}
				addition := []string{}
				seen := map[string]bool{meta.StandardQuestion: true}
				for _, q := range meta.SimilarQuestions {
					seen[q] = true
				}
				for _, q := range op.Questions {
					q = strings.TrimSpace(q)
					if q != "" && !seen[q] {
						addition = append(addition, q)
						seen[q] = true
					}
				}
				meta.SimilarQuestions = append(meta.SimilarQuestions, addition...)
				meta.Version++
				if err := expected.SetFAQMetadata(meta); err != nil {
					return err
				}
				expected.Content = faqOriginalContent(meta, current.KnowledgeBase)
			case "enabled":
				if op.EntryID != old.ID || op.Enabled == nil {
					return ErrKnowledgeInputChanged
				}
				expected.IsEnabled = *op.Enabled
			case "tag":
				if op.EntryID != old.ID {
					return ErrKnowledgeInputChanged
				}
				expected.TagID = ""
				if op.TagID != nil {
					expected.TagID = *op.TagID
				}
			case "fields":
				if op.Fields == nil {
					return ErrKnowledgeInputChanged
				}
				excluded := false
				for _, id := range op.Fields.ExcludeIDs {
					if id == old.SeqID {
						excluded = true
					}
				}
				if !excluded {
					for _, seq := range sortedFAQManifestTags(op.Fields.ByTag) {
						for _, tag := range current.Tags {
							if tag.SeqID == seq && expected.TagID == tag.ID {
								if err := faqManifestFieldPatch(current, expected, op.Fields.ByTag[seq]); err != nil {
									return err
								}
							}
						}
					}
				}
				if patch, ok := op.Fields.ByID[old.SeqID]; ok {
					if err := faqManifestFieldPatch(current, expected, patch); err != nil {
						return err
					}
				}
			case "import":
				if op.Batch == nil {
					return ErrKnowledgeInputChanged
				}
				before, err := old.FAQMetadata()
				if err != nil {
					return err
				}
				for i := range op.Batch.Entries {
					if faqInputMetadata(&op.Batch.Entries[i]).StandardQuestion == before.StandardQuestion {
						entry = &op.Batch.Entries[i]
						break
					}
				}
				if entry == nil {
					return ErrKnowledgeInputChanged
				}
				meta := faqInputMetadata(entry)
				meta.Version = before.Version + 1
				meta.Source = before.Source
				meta.SimilarQuestions = faqUnionStrings(before.SimilarQuestions, meta.SimilarQuestions)
				meta.NegativeQuestions = faqUnionStrings(before.NegativeQuestions, meta.NegativeQuestions)
				if err := expected.SetFAQMetadata(meta); err != nil {
					return err
				}
				expected.Content = faqOriginalContent(meta, current.KnowledgeBase)
				if entry.IsEnabled != nil {
					expected.IsEnabled = *entry.IsEnabled
				}
				if entry.IsRecommended != nil {
					if *entry.IsRecommended {
						expected.Flags = expected.Flags.SetFlag(types.ChunkFlagRecommended)
					} else {
						expected.Flags = expected.Flags.ClearFlag(types.ChunkFlagRecommended)
					}
				}
			default:
				return ErrKnowledgeInputChanged
			}
		}
		if !faqInputEqual(faqComparable(expected), faqComparable(out)) || expected.SourceContent != out.SourceContent || expected.ContextHeader != out.ContextHeader {
			a, b := reflect.ValueOf(*faqComparable(expected)), reflect.ValueOf(*faqComparable(out))
			for i := 0; i < a.NumField(); i++ {
				if !reflect.DeepEqual(a.Field(i).Interface(), b.Field(i).Interface()) {
					return fmt.Errorf("%w: FAQ producer field %s changed", ErrKnowledgeInputChanged, a.Type().Field(i).Name)
				}
			}
			return ErrKnowledgeInputChanged
		}
	}
	return nil
}
func faqChunkStandard(c *types.Chunk) string {
	meta, err := c.FAQMetadata()
	if err != nil || meta == nil {
		return ""
	}
	return meta.StandardQuestion
}
func sortedFAQManifestTags(values map[int64]types.FAQEntryFieldsUpdate) []int64 {
	ids := make([]int64, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}
