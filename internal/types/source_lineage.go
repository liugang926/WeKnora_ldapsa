package types

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// SourceLineageVersion identifies the supported lineage envelope schema.
	SourceLineageVersion = 1
	// SourceLineageMaxSources bounds the number of controlled source identities.
	SourceLineageMaxSources = 1024
	// SourceLineageMaxBytes bounds the serialized lineage envelope.
	SourceLineageMaxBytes = 256 * 1024
	// SourceLineageComplete marks a producer-accounted set of sources.
	SourceLineageComplete = "complete"
	// SourceLineageUnknown marks an unaccounted or legacy set of sources.
	SourceLineageUnknown = "unknown"
)

var (
	// ErrSourceLineageInvalid reports malformed source identity or envelope data.
	ErrSourceLineageInvalid = errors.New("invalid source lineage")
	// ErrSourceLineageUnknown reports a lineage whose source set is not complete.
	ErrSourceLineageUnknown = errors.New("source lineage is unknown")
	// ErrSourceLineageLimit reports an envelope exceeding its size or count limits.
	ErrSourceLineageLimit = errors.New("source lineage exceeds limit")
)

// SourceIdentity identifies the original source revision, not its current row
// or a visible citation. Only trusted source adapters may construct identities.
// ETag is preserved as an opaque token; equal bytes do not make revisions equal.
type SourceIdentity struct {
	Provider        string `json:"provider"`
	TenantID        uint64 `json:"tenant_id"`
	KnowledgeBaseID string `json:"knowledge_base_id"`
	DataSourceID    string `json:"datasource_id"`
	PairOperationID string `json:"pair_operation_id"`
	InstanceID      string `json:"instance_id"`
	BindingID       string `json:"binding_id"`
	FileID          string `json:"file_id"`
	ExternalID      string `json:"external_id"`
	KnowledgeID     string `json:"knowledge_id"`
	// Revision is the exact open build-fence epoch, paired with KnowledgeID.
	// It is not the append-only observation/audit sequence number.
	Revision uint64 `json:"revision"`
	ETag     string `json:"etag"`
}

// Validate checks the identity and original revision fields without authorizing them.
func (s SourceIdentity) Validate() error {
	if s.Provider != ConnectorTypeNextcloud || s.TenantID == 0 || s.TenantID > math.MaxInt64 ||
		s.Revision == 0 || s.Revision > math.MaxInt64 {
		return fmt.Errorf("%w: source provider, tenant or revision", ErrSourceLineageInvalid)
	}
	for _, id := range []string{
		s.KnowledgeBaseID, s.DataSourceID, s.PairOperationID,
		s.InstanceID, s.BindingID, s.FileID, s.ExternalID, s.KnowledgeID,
	} {
		if id == "" || strings.TrimSpace(id) != id || !utf8.ValidString(id) || len(id) > 512 ||
			strings.ContainsAny(id, "\x00\r\n") {
			return fmt.Errorf("%w: source identity", ErrSourceLineageInvalid)
		}
	}
	fileID, err := strconv.ParseInt(s.FileID, 10, 64)
	if err != nil || fileID < 1 || strconv.FormatInt(fileID, 10) != s.FileID ||
		s.ExternalID != "nextcloud:"+s.InstanceID+":"+s.FileID {
		return fmt.Errorf("%w: source file identity", ErrSourceLineageInvalid)
	}
	if strings.TrimSpace(s.ETag) == "" || !utf8.ValidString(s.ETag) || len(s.ETag) > 4096 ||
		strings.ContainsAny(s.ETag, "\x00\r\n") {
		return fmt.Errorf("%w: original ETag", ErrSourceLineageInvalid)
	}
	return nil
}

// SourceLineage is an internal, versioned record of every controlled source
// influencing a generated message. NULL/zero values remain unknown. A complete
// empty set is an explicit producer assertion, never inferred from absence.
type SourceLineage struct {
	Version int              `json:"version"`
	State   string           `json:"state"`
	Sources []SourceIdentity `json:"sources"`
}

type sourceLineageJSON SourceLineage

// UnknownSourceLineage constructs an explicitly unknown lineage envelope.
func UnknownSourceLineage() *SourceLineage {
	return &SourceLineage{Version: SourceLineageVersion, State: SourceLineageUnknown, Sources: []SourceIdentity{}}
}

// NewCompleteSourceLineage is for producers that have accounted for every
// input. It does not discover dependencies or authorize the source identities.
func NewCompleteSourceLineage(sources ...SourceIdentity) (*SourceLineage, error) {
	if len(sources) > SourceLineageMaxSources {
		return UnknownSourceLineage(), ErrSourceLineageLimit
	}
	lineage := &SourceLineage{
		Version: SourceLineageVersion, State: SourceLineageComplete,
		Sources: append([]SourceIdentity{}, sources...),
	}
	return UnionSourceLineage(lineage)
}

// Validate checks the envelope, its source identities, and serialized size bounds.
func (l *SourceLineage) Validate() error {
	if l == nil || l.Version != SourceLineageVersion ||
		(l.State != SourceLineageComplete && l.State != SourceLineageUnknown) || l.Sources == nil {
		return fmt.Errorf("%w: envelope or version", ErrSourceLineageInvalid)
	}
	if len(l.Sources) > SourceLineageMaxSources {
		return ErrSourceLineageLimit
	}
	for _, source := range l.Sources {
		if err := source.Validate(); err != nil {
			return err
		}
	}
	b, err := json.Marshal(sourceLineageJSON(*l))
	if err != nil {
		return err
	}
	if len(b) > SourceLineageMaxBytes {
		return ErrSourceLineageLimit
	}
	return nil
}

// RequireComplete rejects unknown lineage before accepting a validated complete set.
func (l *SourceLineage) RequireComplete() error {
	if l == nil {
		return ErrSourceLineageUnknown
	}
	if err := l.Validate(); err != nil {
		return err
	}
	if l.State != SourceLineageComplete {
		return ErrSourceLineageUnknown
	}
	return nil
}

// UnionSourceLineage returns a fresh deterministic set. Unknown dominates;
// malformed or oversized inputs produce unknown plus an error, never a
// truncated complete result. No inputs is unknown; producers must explicitly
// assert a complete empty set with NewCompleteSourceLineage().
func UnionSourceLineage(inputs ...*SourceLineage) (*SourceLineage, error) {
	out := UnknownSourceLineage()
	if len(inputs) == 0 {
		return out, nil
	}
	out.State = SourceLineageComplete
	seen := make(map[SourceIdentity]struct{})
	for _, input := range inputs {
		if input == nil {
			out.State = SourceLineageUnknown
			continue
		}
		if err := input.Validate(); err != nil {
			return UnknownSourceLineage(), err
		}
		if input.State == SourceLineageUnknown {
			out.State = SourceLineageUnknown
		}
		for _, source := range input.Sources {
			seen[source] = struct{}{}
			if len(seen) > SourceLineageMaxSources {
				return UnknownSourceLineage(), ErrSourceLineageLimit
			}
		}
	}
	for source := range seen {
		out.Sources = append(out.Sources, source)
	}
	slices.SortFunc(out.Sources, func(a, b SourceIdentity) int {
		// The fixed-field encoding is an unambiguous sort key, including ETag
		// and revision. Delimiter concatenation could alias source identities.
		left, _ := json.Marshal(a)
		right, _ := json.Marshal(b)
		return bytes.Compare(left, right)
	})
	if err := out.Validate(); err != nil {
		return UnknownSourceLineage(), err
	}
	return out, nil
}

// MarshalJSON encodes a validated lineage envelope.
func (l SourceLineage) MarshalJSON() ([]byte, error) {
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(sourceLineageJSON(l))
}

// UnmarshalJSON decodes and validates a canonical lineage envelope.
func (l *SourceLineage) UnmarshalJSON(data []byte) error {
	*l = *UnknownSourceLineage()
	if len(data) > SourceLineageMaxBytes {
		return ErrSourceLineageLimit
	}
	if !utf8.Valid(data) {
		return fmt.Errorf("%w: invalid UTF-8 envelope", ErrSourceLineageInvalid)
	}
	if err := uniqueSourceLineageJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var candidate sourceLineageJSON
	if err := decoder.Decode(&candidate); err != nil {
		return fmt.Errorf("%w: decode envelope", ErrSourceLineageInvalid)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%w: trailing data", ErrSourceLineageInvalid)
	}
	next := SourceLineage(candidate)
	if err := next.Validate(); err != nil {
		return err
	}
	*l = next
	return nil
}

// encoding/json normally lets later duplicate keys overwrite earlier ones.
// Reject ambiguity before typed decoding, including keys inside each source.
func uniqueSourceLineageJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var readValue func(int) error
	readValue = func(depth int) error {
		if depth > 8 {
			return fmt.Errorf("%w: excessive JSON nesting", ErrSourceLineageInvalid)
		}
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%w: malformed JSON", ErrSourceLineageInvalid)
		}
		delim, container := token.(json.Delim)
		if !container {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("%w: malformed object", ErrSourceLineageInvalid)
				}
				key, ok := keyToken.(string)
				if !ok {
					return fmt.Errorf("%w: malformed object key", ErrSourceLineageInvalid)
				}
				// Struct decoding also accepts case-folded aliases (even some
				// Unicode aliases). Version 1 permits only its canonical keys.
				if !canonicalSourceLineageJSONKey(depth, key) {
					return fmt.Errorf("%w: noncanonical object key", ErrSourceLineageInvalid)
				}
				if _, duplicate := seen[key]; duplicate {
					return fmt.Errorf("%w: duplicate object key", ErrSourceLineageInvalid)
				}
				seen[key] = struct{}{}
				if err := readValue(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := readValue(depth + 1); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("%w: malformed container", ErrSourceLineageInvalid)
		}
		end, err := decoder.Token()
		if err != nil || (delim == '{' && end != json.Delim('}')) || (delim == '[' && end != json.Delim(']')) {
			return fmt.Errorf("%w: malformed container end", ErrSourceLineageInvalid)
		}
		return nil
	}
	if err := readValue(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("%w: trailing data", ErrSourceLineageInvalid)
	}
	return nil
}

func canonicalSourceLineageJSONKey(depth int, key string) bool {
	if depth == 0 {
		return key == "version" || key == "state" || key == "sources"
	}
	if depth == 2 {
		switch key {
		case "provider", "tenant_id", "knowledge_base_id", "datasource_id", "pair_operation_id",
			"instance_id", "binding_id", "file_id", "external_id", "knowledge_id", "revision", "etag":
			return true
		}
	}
	return false
}

// Value implements the database driver encoding of a lineage envelope.
func (l SourceLineage) Value() (driver.Value, error) { return l.MarshalJSON() }

// Scan implements database decoding while keeping NULL lineage unknown.
func (l *SourceLineage) Scan(value any) error {
	*l = *UnknownSourceLineage()
	switch value := value.(type) {
	case nil:
		return nil
	case []byte:
		return l.UnmarshalJSON(value)
	case string:
		return l.UnmarshalJSON([]byte(value))
	default:
		return fmt.Errorf("%w: unsupported database type", ErrSourceLineageInvalid)
	}
}
