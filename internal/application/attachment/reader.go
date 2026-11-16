// Package attachment owns session-authorized attachment reads. It has no agent,
// tool, database or sandbox dependency; adapters supply storage and projections.
package attachment

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

const pollInterval = 500 * time.Millisecond

// Store supplies the session inventory and tenant-scoped document lookup.
type Store interface {
	SessionAttachments(ctx context.Context, recheck bool) (types.MessageAttachments, error)
	TemporaryDocument(ctx context.Context, id string, stateOnly bool) (*types.TemporaryDocument, error)
}

// Reader coordinates authorized attachment reads independently of tools and sandboxes.
type Reader struct {
	store      Store
	parseWait  time.Duration
	readImage  func(context.Context, string) ([]byte, error)
	Images     *ImageReader
	Projection Projection
}

// NewReader binds durable storage to one conversation and its parse wait budget.
func NewReader(store Store, wait time.Duration, readImage func(context.Context, string) ([]byte, error)) *Reader {
	return &Reader{store: store, parseWait: wait, readImage: readImage}
}

// WaitTimeout bounds parsing plus one image batch: the batch is described
// concurrently, with room for one queue round behind other reads.
func (r *Reader) WaitTimeout() time.Duration {
	wait := r.parseWait
	if r.Images != nil && r.Images.Describe != nil {
		wait += imageDescribeFullTimeout + imageDescribeTimeout
	}
	return wait
}

// List resolves each authorized document without
// waiting or projecting files. Listing must not reload the message history per item.
func (r *Reader) List(ctx context.Context) ([]interfaces.AttachmentDocument, error) {
	inventory, err := r.inventory(ctx, false)
	if err != nil {
		return nil, err
	}
	out := make([]interfaces.AttachmentDocument, 0, len(inventory))
	for _, item := range inventory {
		doc, err := r.store.TemporaryDocument(ctx, item.ID, false)
		if err != nil {
			return nil, fmt.Errorf("load attachment %s: %w", item.ID, err)
		}
		out = append(out, interfaces.AttachmentDocument{Attachment: item, Document: doc})
	}
	if err := r.Confirm(ctx, out...); err != nil {
		return nil, err
	}
	return out, nil
}

// Resolve opens a session attachment, optionally waiting for parsing and syncing its text.
func (r *Reader) Resolve(ctx context.Context, ref string, wait bool) (*interfaces.AttachmentDocument, error) {
	inventory, err := r.inventory(ctx, false)
	if err != nil {
		return nil, err
	}
	item, err := findAttachment(inventory, ref)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", interfaces.ErrAttachmentNotFound, err)
	}
	var doc *types.TemporaryDocument
	if wait {
		doc, err = r.awaitDocument(ctx, item)
	} else {
		doc, err = r.store.TemporaryDocument(ctx, item.ID, false)
	}
	if err != nil {
		return nil, err
	}
	result := &interfaces.AttachmentDocument{Attachment: item, Document: doc}
	if wait && doc != nil && doc.Status == types.TemporaryDocumentStatusReady && r.Projection != nil {
		if err := r.Confirm(ctx, *result); err != nil {
			return nil, err
		}
		check := func(ctx context.Context) error { return r.Confirm(ctx, *result) }
		result.ParsedPath, err = r.Projection.Document(ctx, doc, check)
		if err != nil {
			result.Warning = "Could not sync parsed text to sandbox: " + err.Error()
		}
	}
	if err := r.Confirm(ctx, *result); err != nil {
		return nil, err
	}
	return result, nil
}

// Confirm checks the current document revision followed by the live session,
// owner and material inventory. A cache write does not change the parsed revision.
func (r *Reader) Confirm(ctx context.Context, expected ...interfaces.AttachmentDocument) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, item := range expected {
		current, err := r.store.TemporaryDocument(ctx, item.Attachment.ID, true)
		if err != nil {
			return err
		}
		prev := item.Document
		if (current == nil) != (prev == nil) || (current != nil &&
			(current.ID != prev.ID || current.TenantID != prev.TenantID || current.SessionID != prev.SessionID ||
				current.Status != prev.Status || !current.CreatedAt.Equal(prev.CreatedAt) ||
				!current.UpdatedAt.Equal(prev.UpdatedAt))) {
			return interfaces.ErrAttachmentNotFound
		}
	}
	return r.confirmInventory(ctx, expected)
}

func (r *Reader) confirmInventory(ctx context.Context, expected []interfaces.AttachmentDocument) error {
	current, err := r.inventory(ctx, true)
	if err != nil {
		return err
	}
	byID := make(map[string]types.MessageAttachment, len(current))
	for _, item := range current {
		byID[item.ID] = item
	}
	for _, item := range expected {
		if actual, ok := byID[item.Attachment.ID]; !ok || actual != item.Attachment {
			return interfaces.ErrAttachmentNotFound
		}
	}
	return nil
}

// inventory returns the session's stored attachments, deduplicated by ID.
// Inline uploads without a stored document have nothing to read and are left
// out.
func (r *Reader) inventory(ctx context.Context, recheck bool) (types.MessageAttachments, error) {
	if r.store == nil {
		return nil, fmt.Errorf("attachments are unavailable")
	}
	all, err := r.store.SessionAttachments(ctx, recheck)
	if err != nil {
		return nil, fmt.Errorf("failed to load conversation attachments: %w", err)
	}
	seen := make(map[string]int, len(all))
	out := make(types.MessageAttachments, 0, len(all))
	for _, attachment := range all {
		id := strings.TrimSpace(attachment.ID)
		if id == "" {
			continue
		}
		if i, ok := seen[id]; ok {
			// Later rows carry the content persisted after resolution.
			if out[i].Content == "" {
				out[i] = attachment
			}
			continue
		}
		seen[id] = len(out)
		out = append(out, attachment)
	}
	return out, nil
}

func findAttachment(inventory types.MessageAttachments, ref string) (types.MessageAttachment, error) {
	ref = strings.TrimSpace(ref)
	var byName []types.MessageAttachment
	for _, attachment := range inventory {
		if types.AttachmentHandle(attachment.ID) == ref || attachment.ID == ref {
			return attachment, nil
		}
		if strings.EqualFold(attachment.FileName, ref) {
			byName = append(byName, attachment)
		}
	}
	if len(byName) > 0 {
		return byName[len(byName)-1], nil
	}
	if len(inventory) == 0 {
		return types.MessageAttachment{}, fmt.Errorf("this conversation has no readable attachments")
	}
	// An unrecognized reference has no authorized result to recheck. Do not
	// embed an earlier snapshot of other private filenames in this error.
	return types.MessageAttachment{}, fmt.Errorf("unknown attachment %q; list attachment:// for current files", ref)
}

// awaitDocument loads the document, polling while it is still being parsed
// until parseWait elapses or ctx is cancelled.
func (r *Reader) awaitDocument(ctx context.Context, item types.MessageAttachment) (*types.TemporaryDocument, error) {
	deadline := time.Now().Add(r.parseWait)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		document, err := r.store.TemporaryDocument(ctx, item.ID, false)
		if err != nil {
			return nil, fmt.Errorf("failed to load attachment: %w", err)
		}
		if document == nil || !attachmentPending(document.Status) || !time.Now().Before(deadline) {
			return document, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
			if err := r.confirmInventory(ctx, []interfaces.AttachmentDocument{{Attachment: item}}); err != nil {
				return nil, err
			}
		}
	}
}

func attachmentPending(status string) bool {
	return status == types.TemporaryDocumentStatusUploaded || status == types.TemporaryDocumentStatusProcessing
}
