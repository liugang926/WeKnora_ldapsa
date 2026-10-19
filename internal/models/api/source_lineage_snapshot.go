package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/Tencent/WeKnora/internal/types"
)

// Detach all wire-visible nested values before authorization. An authorizer
// or another owner of the original options/maps must not alter the model
// payload after its receipt was checked. Internal scalar routing metadata is
// copied explicitly because it intentionally does not use JSON fields.
func FreezeSourceModelOptions(options *Options) (*Options, error) {
	if options == nil {
		return nil, nil
	}
	snapshot := *options
	snapshot.Tools = append([]Tool(nil), options.Tools...)
	for i := range snapshot.Tools {
		snapshot.Tools[i].Function.Parameters = append(json.RawMessage(nil), options.Tools[i].Function.Parameters...)
	}
	snapshot.Format = append(json.RawMessage(nil), options.Format...)
	if options.Thinking != nil {
		value := *options.Thinking
		snapshot.Thinking = &value
	}
	if options.ParallelToolCalls != nil {
		value := *options.ParallelToolCalls
		snapshot.ParallelToolCalls = &value
	}
	return &snapshot, nil
}

func FreezeSourceModelMessages(messages []Message) ([]Message, error) {
	for _, message := range messages {
		if err := validateProviderArtifactValue(reflect.ValueOf(message.ReasoningMetadata), 0); err != nil {
			return nil, err
		}
		for _, call := range message.ToolCalls {
			if err := validateProviderArtifactValue(reflect.ValueOf(call.ProviderMetadata), 0); err != nil {
				return nil, err
			}
		}
	}
	encoded, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}
	var snapshot []Message
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, err
	}
	for i := range snapshot {
		snapshot[i].SourceLineage, err = types.UnionSourceLineage(messages[i].SourceLineage)
		if err != nil {
			return nil, err
		}
		snapshot[i].Kind = messages[i].Kind
		snapshot[i].TurnID = messages[i].TurnID
	}
	return snapshot, nil
}

// Provider artifacts must be the supported JSON domain (plus explicitly raw
// JSON), not arbitrary structs/pointers whose private fields or custom JSON
// methods could silently discard opaque material while certifying empty.
func validateProviderArtifactValue(value reflect.Value, depth int) error {
	if !value.IsValid() {
		return nil
	}
	if depth > 32 {
		return types.ErrSourceLineageUnknown
	}
	if value.CanInterface() {
		switch raw := value.Interface().(type) {
		case json.RawMessage:
			if len(raw) > 0 && !json.Valid(raw) {
				return types.ErrSourceLineageUnknown
			}
			return nil
		case types.JSON:
			if len(raw) > 0 && !json.Valid(raw) {
				return types.ErrSourceLineageUnknown
			}
			return nil
		}
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return nil
		}
		return validateProviderArtifactValue(value.Elem(), depth+1)
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return types.ErrSourceLineageUnknown
		}
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateProviderArtifactValue(iterator.Value(), depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return types.ErrSourceLineageUnknown
		}
		for i := 0; i < value.Len(); i++ {
			if err := validateProviderArtifactValue(value.Index(i), depth+1); err != nil {
				return err
			}
		}
		return nil
	case reflect.Bool,
		reflect.String,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Float32,
		reflect.Float64:
		return nil
	default:
		return fmt.Errorf("%w: unsupported provider artifact value", types.ErrSourceLineageUnknown)
	}
}
