package config

import (
	_ "embed"
	"github.com/Tencent/WeKnora/internal/types"
	"gopkg.in/yaml.v3"
)

//go:embed enrichment_summary_defaults.yaml
var enrichmentSummaryDefaults []byte

//go:embed enrichment_questions_defaults.yaml
var enrichmentQuestionDefaults []byte

func enrichmentPromptText(cfg *Config, purpose string) string {
	if cfg == nil || cfg.Conversation == nil {
		return ""
	}
	if purpose == "summary" {
		return cfg.Conversation.GenerateSummaryPrompt
	}
	if purpose == "question" {
		return cfg.Conversation.GenerateQuestionsPrompt
	}
	return ""
}

// BindIndependentEnrichmentPromptLiteral is an explicit trusted code boundary.
// It accepts the exact literal being installed, never arbitrary operator YAML
// or a marker decoded from an HTTP body. A later edit invalidates the binding.
func BindIndependentEnrichmentPromptLiteral(cfg *Config, purpose, literal string) error {
	if cfg == nil || literal == "" || enrichmentPromptText(cfg, purpose) != literal || (purpose != "summary" && purpose != "question") {
		return types.ErrSourceLineageUnknown
	}
	if cfg.enrichmentPromptLiterals == nil {
		cfg.enrichmentPromptLiterals = map[string]string{}
	}
	cfg.enrichmentPromptLiterals[purpose] = types.GraphDigest(literal)
	return nil
}

// KnowledgeEnrichmentPromptLineage only certifies exact code-owned inputs.
// Unknown old custom YAML cannot acquire an origin from its current contents.
func KnowledgeEnrichmentPromptLineage(cfg *Config, purpose string) (*types.SourceLineage, error) {
	actual := enrichmentPromptText(cfg, purpose)
	if actual == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	if cfg.enrichmentPromptLiterals[purpose] == types.GraphDigest(actual) {
		return types.NewCompleteSourceLineage()
	}
	var raw []byte
	if purpose == "summary" {
		raw = enrichmentSummaryDefaults
	} else if purpose == "question" {
		raw = enrichmentQuestionDefaults
	} else {
		return nil, types.ErrSourceLineageUnknown
	}
	var defaults struct {
		Templates []PromptTemplate `yaml:"templates"`
	}
	if err := yaml.Unmarshal(raw, &defaults); err != nil {
		return nil, err
	}
	for _, literal := range defaults.Templates {
		if literal.Content == actual {
			return types.NewCompleteSourceLineage()
		}
	}
	return nil, types.ErrSourceLineageUnknown
}
