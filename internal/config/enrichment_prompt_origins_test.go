package config

import (
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"os"
	"testing"
)

func TestKnowledgeEnrichmentActualDefaultPromptFilesHaveCodeOrigins(t *testing.T) {
	for _, purpose := range []string{"summary", "question"} {
		file := "../../config/prompt_templates/generate_summary.yaml"
		if purpose == "question" {
			file = "../../config/prompt_templates/generate_questions.yaml"
		}
		raw, err := os.ReadFile(file)
		require.NoError(t, err)
		var defaults struct {
			Templates []PromptTemplate `yaml:"templates"`
		}
		require.NoError(t, yaml.Unmarshal(raw, &defaults))
		require.NotEmpty(t, defaults.Templates)
		cfg := &Config{Conversation: &ConversationConfig{}}
		for _, prompt := range defaults.Templates {
			if purpose == "summary" {
				cfg.Conversation.GenerateSummaryPrompt = prompt.Content
			} else {
				cfg.Conversation.GenerateQuestionsPrompt = prompt.Content
			}
			lineage, err := KnowledgeEnrichmentPromptLineage(cfg, purpose)
			require.NoError(t, err, prompt.ID)
			require.NoError(t, lineage.RequireComplete())
		}
	}
}

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
