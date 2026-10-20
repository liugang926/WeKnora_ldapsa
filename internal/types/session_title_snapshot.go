package types

// SessionTitleSnapshot is an internal compare token, never a client DTO.
type SessionTitleSnapshot struct {
	Text          string         `json:"-"`
	Revision      uint64         `json:"-"`
	SourceLineage *SourceLineage `json:"-"`
}
