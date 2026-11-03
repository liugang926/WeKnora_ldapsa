package types

import "gorm.io/gorm"

// KnowledgeBaseAuthorizationMetadata is a fresh structural projection, not a
// configuration snapshot or evidence that any knowledge content may be read.
type KnowledgeBaseAuthorizationMetadata struct {
	ID        string
	TenantID  uint64
	DeletedAt gorm.DeletedAt
}
