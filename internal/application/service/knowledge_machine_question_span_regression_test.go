package service

import (
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func runKnowledgeMachineQuestionSpanExternalMutation(t *testing.T, pg bool) {
	for _, mutation := range []string{"metadata edit", "metadata ABA", "body ABA"} {
		t.Run(mutation, func(t *testing.T) {
			m := newMachinePipelineFixture(t, pg)
			var kb types.KnowledgeBase
			require.NoError(t, m.f.db.Where("id = ?", "kb").Take(&kb).Error)
			kb.QuestionGenerationConfig = &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 2}
			require.NoError(
				t,
				m.f.store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(m.f.ctx), &kb),
			)
			m.f.model.detectQuestions = true
			m.f.model.response = `{"summary":"A complete generated summary","gist":"Generated original profile title","topics":["actual topic"],"doc_type":"guide","typical_question":"What is this actual topic?"}`
			row := m.ingest(t)
			completed := map[string]int{}
			for i := 0; i < len(m.f.queue.tasks); i++ {
				task := m.f.queue.tasks[i]
				require.NoError(t, m.run(t, task), task.Type())
				completed[task.Type()]++
			}
			for _, kind := range []string{types.TypeDocumentProcess, types.TypeSummaryGeneration, types.TypeQuestionGeneration, types.TypeKnowledgeAutoTag} {
				require.Positive(t, completed[kind], "actual pipeline must run %s", kind)
			}
			require.Equal(t, 2, m.f.model.calls)
			require.Equal(t, 1, m.model.calls)
			require.NoError(t, m.f.db.Where("id = ?", row.ID).Take(row).Error)
			require.Equal(t, types.ParseStatusCompleted, row.ParseStatus)
			require.Equal(t, "enabled", row.EnableStatus)
			require.Equal(t, types.SummaryStatusCompleted, row.SummaryStatus)
			require.Equal(t, "A complete generated summary", row.Description)
			require.Equal(t, "source.md", row.Title)
			require.NotNil(t, row.Profile)
			require.Equal(t, "Generated original profile title", row.Profile.Gist)
			require.Equal(t, []string{"actual topic"}, row.Profile.Topics)
			require.Equal(t, "guide", row.Profile.DocType)
			require.Equal(t, "What is this actual topic?", row.Profile.TypicalQuestion)
			require.Equal(t, "actual-etag", row.GetMetadata()["nextcloud_etag"])
			var published int64
			require.NoError(
				t,
				m.f.db.Table("nextcloud_source_versions").
					Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND candidate_knowledge_id = ? AND state = ?", 7, "kb", m.ds.ID, row.ID, "published").
					Count(&published).
					Error,
			)
			require.EqualValues(t, 1, published)
			var autoTags int64
			require.NoError(
				t,
				m.f.db.Model(&types.KnowledgeTagRelation{}).
					Where("knowledge_id = ? AND tag_id = ?", row.ID, "machine-topic").
					Count(&autoTags).
					Error,
			)
			require.EqualValues(t, 1, autoTags)

			var chunks []*types.Chunk
			require.NoError(
				t,
				m.f.db.Where("knowledge_id = ? AND chunk_type = ?", row.ID, types.ChunkTypeText).
					Order("id ASC").
					Find(&chunks).
					Error,
			)
			require.NotEmpty(t, chunks)
			chunk := chunks[0]
			questions, err := chunk.DocumentMetadata()
			require.NoError(t, err)
			require.NotNil(t, questions)
			require.Equal(
				t,
				[]string{"What is the actual body of this document?", "How does the neighboring content apply?"},
				questions.GetQuestionStrings(),
			)
			require.Equal(t, chunk.ContentRevision, questions.GeneratedQuestionsRevision)

			// This is the real question producer's immutable transition. The
			// positive span read must permit it before any external SQL edit.
			type questionResult struct {
				ChunkID, AdmissionID, ProducerInputID                                              string
				PreviousRevision, ResultRevision                                                   uint64
				PreviousMetadataDigest, PreviousBodyDigest, ResultMetadataDigest, ResultBodyDigest string
			}
			var originalResults []questionResult
			require.NoError(
				t,
				m.f.db.Table("knowledge_question_metadata_outputs").
					Where("chunk_id = ?", chunk.ID).
					Order("result_revision ASC").
					Find(&originalResults).
					Error,
			)
			require.Len(t, originalResults, 1)
			require.Greater(t, originalResults[0].ResultRevision, originalResults[0].PreviousRevision)
			var questionInputs int64
			require.NoError(
				t,
				m.f.db.Table("knowledge_producer_inputs").
					Where("id = ? AND admission_id = ? AND purpose = ?", originalResults[0].ProducerInputID, originalResults[0].AdmissionID, "question").
					Count(&questionInputs).
					Error,
			)
			require.EqualValues(t, 1, questionInputs)
			revision := func() uint64 {
				var current struct{ Revision uint64 }
				require.NoError(
					t,
					m.f.db.Table("knowledge_input_row_versions").
						Where("row_kind = ? AND row_id = ?", "chunk", chunk.ID).
						Take(&current).
						Error,
				)
				return current.Revision
			}
			originalRevision := revision()
			require.Equal(t, originalResults[0].ResultRevision, originalRevision)
			span, err := NewSpanOutputAuthorizer(
				m.f.store,
				m.f.guard,
			).Begin(m.f.ctx, types.SpanOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: row.ID})
			require.NoError(t, err)
			snapshot, err := span.Snapshot()
			require.NoError(t, err)
			require.NoError(t, span.Check(m.f.ctx, snapshot), "the own question result is a valid original transition")
			foundAuto := false
			for _, output := range snapshot.Rows {
				if output.Name == autoTagSpanName {
					foundAuto = true
					require.Equal(t, types.SpanStatusDone, output.Status)
					require.Equal(t, "complete", output.ContentProvenance)
					require.NotEmpty(t, output.Output)
				}
			}
			require.True(t, foundAuto)

			originalMetadata := append(types.JSON(nil), chunk.Metadata...)
			originalBody := chunk.Content
			column := "metadata"
			var changed interface{}
			var restored interface{} = originalMetadata
			if mutation == "body ABA" {
				column = "content"
				changed = originalBody + "\nExternal body mutation after the question producer."
				restored = originalBody
			} else {
				var fields map[string]interface{}
				require.NoError(t, json.Unmarshal(originalMetadata, &fields))
				fields["external_annotation"] = "unproved edit after the question producer"
				encoded, err := json.Marshal(fields)
				require.NoError(t, err)
				changed = types.JSON(encoded)
			}
			update := m.f.db.Model(&types.Chunk{}).Where("id = ?", chunk.ID).Update(column, changed)
			require.NoError(t, update.Error)
			require.EqualValues(t, 1, update.RowsAffected)
			require.Equal(t, originalRevision+1, revision())
			require.Error(
				t,
				span.Check(m.f.ctx, snapshot),
				"own question receipt cannot authorize an external chunk edit",
			)
			if mutation != "metadata edit" {
				update = m.f.db.Model(&types.Chunk{}).Where("id = ?", chunk.ID).Update(column, restored)
				require.NoError(t, update.Error)
				require.EqualValues(t, 1, update.RowsAffected)
				var actual types.Chunk
				require.NoError(t, m.f.db.Where("id = ?", chunk.ID).Take(&actual).Error)
				require.Equal(t, originalBody, actual.Content)
				require.Equal(t, originalMetadata, actual.Metadata)
				require.Equal(t, originalRevision+2, revision())
				require.Error(
					t,
					span.Check(m.f.ctx, snapshot),
					"restoring exact chunk bytes cannot turn an external ABA into an own question result",
				)
			}
			var actualResults []questionResult
			require.NoError(
				t,
				m.f.db.Table("knowledge_question_metadata_outputs").
					Where("chunk_id = ?", chunk.ID).
					Order("result_revision ASC").
					Find(&actualResults).
					Error,
			)
			require.Equal(
				t,
				originalResults,
				actualResults,
				"external writes must not mint or advance the actual question result receipt",
			)
		})
	}
}

func TestKnowledgeMachineQuestionSpanExternalMutationSQLite(t *testing.T) {
	runKnowledgeMachineQuestionSpanExternalMutation(t, false)
}

func TestPostgresKnowledgeMachineQuestionSpanExternalMutation(t *testing.T) {
	runKnowledgeMachineQuestionSpanExternalMutation(t, true)
}
