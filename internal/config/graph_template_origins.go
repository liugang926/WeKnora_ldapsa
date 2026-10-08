package config

import (
	_ "embed"
	"github.com/Tencent/WeKnora/internal/types"
	"gopkg.in/yaml.v3"
)

//go:embed graph_template_defaults.yaml
var builtInGraphTemplates []byte

// BuiltInGraphTemplateOrigin only replaces an exact built-in projection with
// its independent code literal. Arbitrary operator YAML cannot certify itself
// as source-free, and its examples are never authenticated by chunk receipts.
func BuiltInGraphTemplateOrigin(actual *types.PromptTemplateStructured, kind string) (*types.PromptTemplateStructured, error) {
	var defaults struct {
		Extract *ExtractManagerConfig `yaml:"extract"`
	}
	if err := yaml.Unmarshal(builtInGraphTemplates, &defaults); err != nil {
		return nil, err
	}
	if actual == nil || defaults.Extract == nil {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	var literal *types.PromptTemplateStructured
	switch kind {
	case "graph":
		literal = defaults.Extract.ExtractGraph
	case "entity":
		literal = defaults.Extract.ExtractEntity
	default:
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	if types.GraphDigest(actual) != types.GraphDigest(literal) {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	independent, _ := types.NewCompleteSourceLineage()
	return types.BindGraphPromptTemplateOrigin(literal, independent)
}
