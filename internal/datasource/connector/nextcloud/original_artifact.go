package nextcloud

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/Tencent/WeKnora/internal/types"
)

type originalFetchedArtifact struct {
	digest string
	input  OriginalFetchedArtifactInput
}

// OriginalFetchedArtifactInput is a detached descriptor of actual fetched
// bytes. The opaque issuer and actual item still have to match every field.
type OriginalFetchedArtifactInput struct {
	ExternalID, BindingID, InstanceID, FileID, ETag, Path, FileName string
	ArtifactSHA256, ItemDigest                                      string
	SourceConfigDigest                                              string
}

func fetchedArtifactInput(item *types.FetchedItem) OriginalFetchedArtifactInput {
	hash := sha256.Sum256(item.Content)
	encoded, _ := json.Marshal(item)
	all := sha256.Sum256(encoded)
	return OriginalFetchedArtifactInput{ExternalID: item.ExternalID, BindingID: item.SourceResourceID, InstanceID: item.Metadata["nextcloud_instance_id"], FileID: item.Metadata["nextcloud_file_id"], ETag: item.Metadata["nextcloud_etag"], Path: item.Metadata["nextcloud_path"], FileName: item.FileName, ArtifactSHA256: hex.EncodeToString(hash[:]), ItemDigest: hex.EncodeToString(all[:])}
}
func originalArtifactConfigDigest(cfg config) string {
	key := sha256.Sum256([]byte(cfg.token))
	body, _ := json.Marshal(struct{ Base, Key, KeyID string }{cfg.baseURL.String(), hex.EncodeToString(key[:]), cfg.keyID})
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}
func VerifyOriginalArtifactConfiguration(source *types.DataSourceConfig, input OriginalFetchedArtifactInput) error {
	cfg, err := parseConfig(source)
	if err != nil {
		return err
	}
	if input.SourceConfigDigest == "" || input.SourceConfigDigest != originalArtifactConfigDigest(cfg) {
		return errors.New("original Nextcloud artifact source configuration changed")
	}
	return nil
}
func sealFetchedArtifact(item *types.FetchedItem, cfg config) {
	input := fetchedArtifactInput(item)
	input.SourceConfigDigest = originalArtifactConfigDigest(cfg)
	item.OriginalArtifact = originalFetchedArtifact{input: input, digest: input.ArtifactSHA256}
}
func VerifyOriginalFetchedArtifact(item *types.FetchedItem) (OriginalFetchedArtifactInput, error) {
	if item == nil {
		return OriginalFetchedArtifactInput{}, errors.New("actual Nextcloud artifact is missing")
	}
	proof, ok := item.OriginalArtifact.(originalFetchedArtifact)
	actual := fetchedArtifactInput(item)
	actual.SourceConfigDigest = proof.input.SourceConfigDigest
	if !ok || len(item.Content) == 0 || proof.input.SourceConfigDigest == "" || !reflect.DeepEqual(proof.input, actual) || proof.digest != proof.input.ArtifactSHA256 {
		return OriginalFetchedArtifactInput{}, errors.New("original Nextcloud artifact changed or is unknown")
	}
	return proof.input, nil
}
