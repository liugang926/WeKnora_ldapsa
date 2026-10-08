package config

import (
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestKnowledgeEnrichmentPromptExactBuiltinAndCodeOrigins(t *testing.T) {
	for _, kind := range []string{"summary", "question"} {
		t.Run(kind, func(t *testing.T) {
			raw := enrichmentSummaryDefaults
			if kind == "question" {
				raw = enrichmentQuestionDefaults
			}
			var defaults struct {
				Templates []PromptTemplate `yaml:"templates"`
			}
			require.NoError(t, yaml.Unmarshal(raw, &defaults))
			require.NotEmpty(t, defaults.Templates)
			cfg := &Config{Conversation: &ConversationConfig{}}
			if kind == "summary" {
				cfg.Conversation.GenerateSummaryPrompt = defaults.Templates[0].Content
			} else {
				cfg.Conversation.GenerateQuestionsPrompt = defaults.Templates[0].Content
			}
			lineage, e := KnowledgeEnrichmentPromptLineage(cfg, kind)
			require.NoError(t, e)
			require.NoError(t, lineage.RequireComplete())
			require.Empty(t, lineage.Sources)
			if kind == "summary" {
				cfg.Conversation.GenerateSummaryPrompt = "unproved operator YAML"
			} else {
				cfg.Conversation.GenerateQuestionsPrompt = "unproved operator YAML"
			}
			_, e = KnowledgeEnrichmentPromptLineage(cfg, kind)
			require.Error(t, e)
			require.Error(t, BindIndependentEnrichmentPromptLiteral(cfg, kind, "different cached literal"))
			require.NoError(t, BindIndependentEnrichmentPromptLiteral(cfg, kind, "unproved operator YAML"))
			_, e = KnowledgeEnrichmentPromptLineage(cfg, kind)
			require.NoError(t, e)
			if kind == "summary" {
				cfg.Conversation.GenerateSummaryPrompt += " changed"
			} else {
				cfg.Conversation.GenerateQuestionsPrompt += " changed"
			}
			_, e = KnowledgeEnrichmentPromptLineage(cfg, kind)
			require.Error(t, e)
		})
	}
}
