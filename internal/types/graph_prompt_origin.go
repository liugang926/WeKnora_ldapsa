package types

import "encoding/json"

// GraphPromptTemplateOrigin is private producer metadata. A literal default
// uses a detached code-owned replacement; custom human/config/derived inputs
// require a trusted writer to bind their exact value and exhaustive lineage.
type GraphPromptTemplateOrigin struct {
	Digest        string
	SourceLineage *SourceLineage
}

// BindGraphPromptTemplateOrigin freezes an original template and its complete source lineage.
func BindGraphPromptTemplateOrigin(
	template *PromptTemplateStructured,
	lineage *SourceLineage,
) (*PromptTemplateStructured, error) {
	if template == nil || lineage.RequireComplete() != nil {
		return nil, ErrGraphSourceReceiptUnknown
	}
	encoded, err := json.Marshal(template)
	if err != nil {
		return nil, err
	}
	var frozen PromptTemplateStructured
	if err := json.Unmarshal(encoded, &frozen); err != nil {
		return nil, err
	}
	sources, err := UnionSourceLineage(lineage)
	if err != nil {
		return nil, err
	}
	frozen.SourceOrigin = &GraphPromptTemplateOrigin{Digest: GraphDigest(&frozen), SourceLineage: sources}
	return &frozen, nil
}

// GraphPromptTemplateLineage validates the frozen origin before returning its lineage.
func GraphPromptTemplateLineage(template *PromptTemplateStructured) (*SourceLineage, error) {
	if template == nil || template.SourceOrigin == nil || template.SourceOrigin.Digest != GraphDigest(template) ||
		template.SourceOrigin.SourceLineage.RequireComplete() != nil {
		return UnknownSourceLineage(), ErrGraphSourceReceiptUnknown
	}
	return UnionSourceLineage(template.SourceOrigin.SourceLineage)
}
