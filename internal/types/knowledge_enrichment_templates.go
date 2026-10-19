package types

import (
	"database/sql/driver"
	"encoding/json"
)

// KnowledgeEnrichmentTemplates are actual KB configuration inputs. Their
// origin is the private configuration receipt, never a client lineage claim.
type KnowledgeEnrichmentTemplates struct {
	Summary  string `json:"summary,omitempty"`
	Question string `json:"question,omitempty"`
}

func (c KnowledgeEnrichmentTemplates) Value() (driver.Value, error) { return json.Marshal(c) }
func (c *KnowledgeEnrichmentTemplates) Scan(value any) error {
	if value == nil {
		return nil
	}
	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return ErrSourceLineageUnknown
	}
	return json.Unmarshal(data, c)
}
func (c *KnowledgeEnrichmentTemplates) Content(purpose string) string {
	if c == nil {
		return ""
	}
	if purpose == "summary" {
		return c.Summary
	}
	if purpose == "question" {
		return c.Question
	}
	return ""
}
func (c *KnowledgeEnrichmentTemplates) Set(purpose, content string) error {
	if purpose == "summary" {
		c.Summary = content
		return nil
	}
	if purpose == "question" {
		c.Question = content
		return nil
	}
	return ErrSourceLineageUnknown
}
