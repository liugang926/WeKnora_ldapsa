package nextcloud

import (
	"context"
	"fmt"
	"reflect"

	"github.com/Tencent/WeKnora/internal/types"
)

// RecoveryManifest is a complete, stable observation from the configured
// machine endpoint. It contains no credentials and grants no personal reads.
type RecoveryManifest struct {
	Identity   PairingIdentity        `json:"identity"`
	Generation string                 `json:"generation"`
	Files      []RecoveryManifestFile `json:"files"`
}

type RecoveryManifestFile struct {
	FileID int64  `json:"file_id"`
	ETag   string `json:"etag"`
	Path   string `json:"path"`
}

// InspectRecoveryManifest requires two complete manifest scans with identical
// generation and entries, enclosed by live pairing identity checks. A partial
// page, ACL failure, instance change or concurrent manifest change fails closed.
func InspectRecoveryManifest(ctx context.Context, ds *types.DataSourceConfig) (RecoveryManifest, error) {
	var result RecoveryManifest
	before, err := InspectPairing(ctx, ds)
	if err != nil {
		return result, err
	}
	cfg, err := parseConfig(ds)
	if err != nil {
		return result, err
	}
	cli := newClient(cfg)
	first, generation, err := cli.manifest(ctx, before.BindingID)
	if err != nil {
		return result, err
	}
	second, nextGeneration, err := cli.manifest(ctx, before.BindingID)
	if err != nil {
		return result, err
	}
	after, err := InspectPairing(ctx, ds)
	if err != nil {
		return result, err
	}
	if before != after || generation == "" || generation != nextGeneration || !reflect.DeepEqual(first, second) {
		return result, fmt.Errorf("Nextcloud recovery manifest changed during reconciliation")
	}
	result.Identity, result.Generation = after, generation
	result.Files = make([]RecoveryManifestFile, 0, len(second))
	for _, file := range second {
		result.Files = append(result.Files, RecoveryManifestFile{file.FileID, file.ETag, file.Path})
	}
	return result, nil
}
