package repository

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type knowledgeConfigurationInputOrigin struct {
	OriginID        string `gorm:"primaryKey"`
	TenantID        uint64
	KnowledgeBaseID string
	ConfigRevision  uint64
	OriginKind      string
	InputDigest     string
	SourceLineage   string
}

func (knowledgeConfigurationInputOrigin) TableName() string { return "knowledge_config_input_origins" }

func knowledgeConfigurationOriginDigest(kb *types.KnowledgeBase) (string, error) {
	config, err := knowledgeConfigDigest(kb, &types.Knowledge{})
	if err != nil {
		return "", err
	}
	return KnowledgeInputDigest(struct {
		Tenant           uint64
		KB, Type, Config string
	}{kb.TenantID, kb.ID, kb.Type, config})
}
func knowledgeConfigurationControlRevision(tx *gorm.DB, id string) (uint64, error) {
	var row struct{ Revision uint64 }
	if err := tx.Table("knowledge_configuration_control_versions").Where("knowledge_base_id = ?", id).Take(&row).Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

func readKnowledgeConfigurationOrigin(tx *gorm.DB, kb *types.KnowledgeBase) (*types.SourceLineage, error) {
	revision, err := knowledgeConfigurationControlRevision(tx, kb.ID)
	if err != nil {
		return nil, err
	}
	var fact knowledgeConfigurationInputOrigin
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND config_revision = ?", kb.TenantID, kb.ID, revision).Take(&fact).Error; err != nil {
		return nil, err
	}
	digest, err := knowledgeConfigurationOriginDigest(kb)
	if err != nil {
		return nil, err
	}
	if digest != fact.InputDigest {
		return nil, ErrKnowledgeInputChanged
	}
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(fact.SourceLineage), &lineage) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	if err := lineage.RequireComplete(); err != nil {
		return nil, err
	}
	if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
		return nil, err
	}
	return &lineage, nil
}
func admitKnowledgeConfigurationOrigin(tx *gorm.DB, kb *types.KnowledgeBase, origin knowledgeInputOrigin) error {
	if err := origin.lineage.RequireComplete(); err != nil {
		return err
	}
	if err := verifyKnowledgeInheritedSources(tx, origin.lineage); err != nil {
		return err
	}
	digest, err := knowledgeConfigurationOriginDigest(kb)
	if err != nil {
		return err
	}
	revision, err := knowledgeConfigurationControlRevision(tx, kb.ID)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(origin.lineage)
	if err != nil {
		return err
	}
	return tx.Create(&knowledgeConfigurationInputOrigin{uuid.NewString(), kb.TenantID, kb.ID, revision, origin.kind, digest, string(encoded)}).Error
}

// CreateKnowledgeBaseWithInput binds the actual new human/code/derived config
// in the same transaction as creation. It cannot adopt an old raw config.
func (s *KnowledgeInputProvenanceStore) CreateKnowledgeBaseWithInput(ctx context.Context, kb *types.KnowledgeBase) error {
	origin, ok := ctx.Value(knowledgeInputOriginKey{}).(knowledgeInputOrigin)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(kb).Error; err != nil {
			return err
		}
		var actual types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", kb.ID, kb.TenantID).Take(&actual).Error; err != nil {
			return err
		}
		if err := admitKnowledgeBaseNameInput(tx, ctx, nil, &actual, s.displayAuthorityFactory); err != nil {
			return err
		}
		return admitKnowledgeConfigurationOrigin(tx, &actual, origin)
	}, knowledgeDisplayWriteOptions(s.db))
}

// ReplaceKnowledgeBaseConfigurationWithInput is exclusively a trusted full
// replacement producer. Passing a freshly loaded legacy object is not a human
// input boundary. Partial settings edits use the preserving method below.
func (s *KnowledgeInputProvenanceStore) ReplaceKnowledgeBaseConfigurationWithInput(ctx context.Context, kb *types.KnowledgeBase) error {
	origin, ok := ctx.Value(knowledgeInputOriginKey{}).(knowledgeInputOrigin)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var old types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", kb.ID, kb.TenantID).Take(&old).Error; err != nil {
			return err
		}
		if err := tx.Omit("ever_had_nextcloud_source").Save(kb).Error; err != nil {
			return err
		}
		var actual types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", kb.ID, kb.TenantID).Take(&actual).Error; err != nil {
			return err
		}
		return admitKnowledgeConfigurationOrigin(tx, &actual, origin)
	}, knowledgeDisplayWriteOptions(s.db))
}

// UpdateKnowledgeBaseWithInput preserves the original provenance of retained
// fields. A name-only edit cannot declare legacy examples source-independent.
func (s *KnowledgeInputProvenanceStore) UpdateKnowledgeBaseWithInput(ctx context.Context, kb *types.KnowledgeBase) error {
	origin, ok := ctx.Value(knowledgeInputOriginKey{}).(knowledgeInputOrigin)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var old types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", kb.ID, kb.TenantID).Take(&old).Error; err != nil {
			return err
		}
		oldControlDigest, err := knowledgeConfigurationOriginDigest(&old)
		if err != nil {
			return err
		}
		requestedControlDigest, err := knowledgeConfigurationOriginDigest(kb)
		if err != nil {
			return err
		}
		var lineage *types.SourceLineage
		if oldControlDigest != requestedControlDigest {
			previous, err := readKnowledgeConfigurationOrigin(tx, &old)
			if err != nil {
				return err
			}
			lineage, err = types.UnionSourceLineage(previous, origin.lineage)
			if err != nil {
				return err
			}
		}
		if err := tx.Omit("ever_had_nextcloud_source").Save(kb).Error; err != nil {
			return err
		}
		var actual types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", kb.ID, kb.TenantID).Take(&actual).Error; err != nil {
			return err
		}
		oldDigest, _ := knowledgeConfigurationOriginDigest(&old)
		newDigest, _ := knowledgeConfigurationOriginDigest(&actual)
		if err := admitKnowledgeBaseNameInput(tx, ctx, &old, &actual, s.displayAuthorityFactory); err != nil {
			return err
		}
		if oldDigest == newDigest {
			return nil
		}
		return admitKnowledgeConfigurationOrigin(tx, &actual, knowledgeInputOrigin{lineage, origin.kind})
	}, knowledgeDisplayWriteOptions(s.db))
}
