package dingtalk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
)

// originalFetchedArtifact can only be issued inside the connector after a
// successful scoped fetch. It is never decoded from metadata or a queue.
type originalFetchedArtifact struct {
	input      OriginalFetchedArtifactInput
	invocation *OriginalFetchInvocation
}

// OriginalFetchInvocation is an in-process producer association, not authorization.
// Its nonzero private storage gives each Begin a distinct identity; JSON cannot recreate it.
type (
	OriginalFetchInvocation    struct{ marker byte }
	originalFetchInvocationKey struct{}
)

// BeginOriginalFetch associates subsequent real fetch results with one invocation.
func BeginOriginalFetch(ctx context.Context) (context.Context, *OriginalFetchInvocation) {
	invocation := &OriginalFetchInvocation{marker: 1}
	return context.WithValue(ctx, originalFetchInvocationKey{}, invocation), invocation
}

// VerifyOriginalFetchedArtifactForInvocation rejects results from any other Begin.
func VerifyOriginalFetchedArtifactForInvocation(
	invocation *OriginalFetchInvocation,
	item *types.FetchedItem,
) (OriginalFetchedArtifactInput, error) {
	if item == nil || invocation == nil {
		return OriginalFetchedArtifactInput{}, errors.New("DingTalk fetch invocation is missing")
	}
	proof, ok := item.OriginalArtifact.(originalFetchedArtifact)
	if !ok || proof.invocation != invocation {
		return OriginalFetchedArtifactInput{}, errors.New("DingTalk artifact belongs to another fetch invocation")
	}
	return VerifyOriginalFetchedArtifact(item)
}

// OriginalFetchedArtifactInput is a detached description, not an issuing
// capability. It contains neither credentials nor document text.
type OriginalFetchedArtifactInput struct {
	WorkspaceID, DocumentID, ResourceID, Revision  string
	ArtifactSHA256, ItemDigest, SourceConfigDigest string
}

func artifactHash(body []byte) string {
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:])
}

// Bind the actual selection passed to FetchAll, which may differ from the
// configuration's ResourceIDs. Verification requires that same selection.
func originalArtifactConfigDigest(source *types.DataSourceConfig, selected []string) (string, error) {
	cfg, err := parseConfig(source)
	if err != nil {
		return "", err
	}
	resources := uniqueIDs(selected)
	sort.Strings(resources)
	encoded, err := json.Marshal(struct {
		Type      string
		Config    *config
		Resources []string
		Settings  map[string]interface{}
	}{source.Type, cfg, resources, source.Settings})
	if err != nil {
		return "", errors.New("invalid DingTalk artifact configuration")
	}
	return artifactHash(encoded), nil
}

// VerifyOriginalArtifactConfiguration compares a detached description against
// current effective source configuration. It does not prove issuance or grant
// access: callers must first VerifyOriginalFetchedArtifact and apply their
// existing task/source/knowledge-base checks.
func VerifyOriginalArtifactConfiguration(source *types.DataSourceConfig, input OriginalFetchedArtifactInput) error {
	if source == nil {
		return errors.New("DingTalk artifact configuration is missing")
	}
	digest, err := originalArtifactConfigDigest(source, source.ResourceIDs)
	if err != nil {
		return err
	}
	if input.SourceConfigDigest == "" || digest != input.SourceConfigDigest {
		return errors.New("original DingTalk artifact configuration changed")
	}
	return nil
}

func sealFetchedArtifact(
	ctx context.Context,
	item *types.FetchedItem,
	scope syncScope,
	document node,
	configDigest string,
) {
	if item == nil || len(item.Content) == 0 || item.IsDeleted || configDigest == "" {
		return
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		return
	}
	invocation, _ := ctx.Value(originalFetchInvocationKey{}).(*OriginalFetchInvocation)
	item.OriginalArtifact = originalFetchedArtifact{input: OriginalFetchedArtifactInput{
		WorkspaceID: scope.Reference.WorkspaceID, DocumentID: document.ID,
		ResourceID: scope.ResourceID, Revision: document.revision(),
		ArtifactSHA256: artifactHash(item.Content), ItemDigest: artifactHash(encoded), SourceConfigDigest: configDigest,
	}, invocation: invocation}
}

// VerifyOriginalFetchedArtifact validates an actual in-process connector result.
// Copies of the descriptor cannot create a receipt; JSON round trips deliberately
// lose the private issuer. Call before modifying any fetched item fields.
func VerifyOriginalFetchedArtifact(item *types.FetchedItem) (OriginalFetchedArtifactInput, error) {
	if item == nil {
		return OriginalFetchedArtifactInput{}, errors.New("actual DingTalk artifact is missing")
	}
	proof, ok := item.OriginalArtifact.(originalFetchedArtifact)
	encoded, err := json.Marshal(item)
	if !ok || err != nil || len(item.Content) == 0 || item.IsDeleted ||
		proof.input.SourceConfigDigest == "" || proof.input.WorkspaceID == "" ||
		proof.input.DocumentID != item.ExternalID || proof.input.ResourceID != item.SourceResourceID ||
		proof.input.ArtifactSHA256 != artifactHash(item.Content) || proof.input.ItemDigest != artifactHash(encoded) {
		return OriginalFetchedArtifactInput{}, errors.New("original DingTalk artifact changed or is unknown")
	}
	return proof.input, nil
}
