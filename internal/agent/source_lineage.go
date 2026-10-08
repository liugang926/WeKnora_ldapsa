package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Tencent/WeKnora/internal/agent/skills"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// This error deliberately contains no source names, tool diagnostics or model
// output. A rejected prefix must not be published through a control event.
var ErrAgentSourceLineageDenied = errors.New("agent source lineage is unavailable or unauthorized")

func independentAgentInput() *types.SourceLineage {
	l, _ := types.NewCompleteSourceLineage()
	return l
}

func toolResultLineage(result *types.ToolResult) *types.SourceLineage {
	if result == nil {
		return types.UnknownSourceLineage()
	}
	return result.SourceLineage
}

// SetSourceLineageVerifier is mandatory. The caller binds the original
// interactive caller and current server-resolved session/Agent source scope.
func (e *AgentEngine) SetSourceLineageVerifier(check func(context.Context, *types.SourceLineage) error) {
	e.lineageVerifier = check
	e.turnLineage = types.NewRAGSourceLineage(check)
	if e.compactor != nil {
		e.compactor.SetSourceLineageVerifier(e.authorizeCompaction)
	}
}

// These receipts are server-only producer assertions over the actual rendered
// values. A selected KB, role, citation or absence of a datasource is no receipt.
func (e *AgentEngine) SetSystemPromptSourceLineage(l *types.SourceLineage) { e.systemPromptLineage = l }
func (e *AgentEngine) SetMemoryPromptSourceLineage(l *types.SourceLineage) { e.memoryPromptLineage = l }
func (e *AgentEngine) SetUserInputSourceLineage(l *types.SourceLineage)    { e.userInputLineage = l }

// The receipt is bound to a private copy of the exact skill metadata values.
// System prompt receipts cannot certify skill/package descriptions.
func (e *AgentEngine) SetSkillsMetadataSourceLineage(l *types.SourceLineage) {
	e.skillsMetadataLineage, _ = types.UnionSourceLineage(l)
	if e.skillsManager == nil {
		e.skillsMetadataBinding = nil
		return
	}
	e.skillsMetadataBinding, _ = json.Marshal(e.skillsManager.GetAllMetadata())
}

func cloneSkillsMetadata(metadata []*skills.SkillMetadata) []*skills.SkillMetadata {
	bytes, err := json.Marshal(metadata)
	if err != nil {
		return nil
	}
	var copy []*skills.SkillMetadata
	if err := json.Unmarshal(bytes, &copy); err != nil {
		return nil
	}
	return copy
}
func (e *AgentEngine) skillsMetadataInputLineage(metadata []*skills.SkillMetadata) *types.SourceLineage {
	actual, err := json.Marshal(metadata)
	if err != nil || e.skillsMetadataBinding == nil || !bytes.Equal(actual, e.skillsMetadataBinding) {
		return types.UnknownSourceLineage()
	}
	lineage, _ := types.UnionSourceLineage(e.skillsMetadataLineage)
	return lineage
}

func (e *AgentEngine) SetRAGChunkSnapshotReader(reader interfaces.RAGChunkSnapshotReader) {
	if e.toolRegistry != nil {
		e.toolRegistry.SetRAGChunkSnapshotReader(reader)
	}
}

func (e *AgentEngine) SourceLineageSnapshot() *types.SourceLineage {
	return e.turnLineage.Snapshot()
}

func (e *AgentEngine) rememberLineageFailure(err error) error {
	if err == nil {
		return nil
	}
	e.lineageMu.Lock()
	defer e.lineageMu.Unlock()
	if e.lineageErr == nil {
		e.lineageErr = ErrAgentSourceLineageDenied
	}
	if e.lineageCancel != nil {
		e.lineageCancel()
	}
	return ErrAgentSourceLineageDenied
}

func (e *AgentEngine) lineageFailure() error {
	e.lineageMu.Lock()
	defer e.lineageMu.Unlock()
	return e.lineageErr
}

func (e *AgentEngine) addLineage(inputs ...*types.SourceLineage) error {
	if err := e.lineageFailure(); err != nil {
		return err
	}
	if err := e.turnLineage.Add(inputs...); err != nil {
		return e.rememberLineageFailure(err)
	}
	return nil
}

func (e *AgentEngine) checkLineage(ctx context.Context) error {
	_, err := e.verifiedLineage(ctx)
	return err
}

func (e *AgentEngine) verifiedLineage(ctx context.Context) (*types.SourceLineage, error) {
	if err := e.lineageFailure(); err != nil {
		return nil, err
	}
	lineage, err := e.turnLineage.CheckedSnapshot(ctx)
	if err != nil {
		return nil, e.rememberLineageFailure(err)
	}
	if err := e.lineageFailure(); err != nil {
		return nil, err
	}
	return lineage, nil
}

func (e *AgentEngine) accountMessages(ctx context.Context, messages []chat.Message) error {
	// Account the actual wire messages, including metadata/reasoning/tool args,
	// rather than reconstructing evidence from citations or selected scopes.
	for _, message := range messages {
		if err := e.addLineage(message.SourceLineage); err != nil {
			return err
		}
	}
	return e.checkLineage(ctx)
}

func (e *AgentEngine) authorizeCompaction(ctx context.Context, lineage *types.SourceLineage) error {
	if err := e.addLineage(lineage); err != nil {
		return err
	}
	return e.checkLineage(ctx)
}

// emitEvent is the sole engine publication boundary. Every event carries the
// complete prefix; tool adapters may add only typed internal producer receipts.
func (e *AgentEngine) emitEvent(ctx context.Context, evt event.Event) error {
	e.lineageEmitMu.Lock()
	defer e.lineageEmitMu.Unlock()
	if evt.SourceLineage != nil {
		l, ok := evt.SourceLineage.(*types.SourceLineage)
		if !ok {
			return e.rememberLineageFailure(types.ErrSourceLineageUnknown)
		}
		if err := e.addLineage(l); err != nil {
			return err
		}
	}
	lineage, err := e.verifiedLineage(ctx)
	if err != nil {
		return err
	}
	evt.SourceLineage = lineage
	if e.eventBus == nil {
		return e.rememberLineageFailure(fmt.Errorf("missing event bus"))
	}
	return e.rememberLineageFailure(e.eventBus.Emit(ctx, evt))
}

func (e *AgentEngine) promptSourceLineage() *types.SourceLineage {
	inputs := []*types.SourceLineage{independentAgentInput()}
	// Default server prompt text is a literal producer. Custom templates,
	// recalled memory and skill metadata have separate, required receipts.
	if e.systemPromptTemplate != "" {
		inputs = append(inputs, e.systemPromptLineage)
	}
	if e.memoryPrompt != "" {
		inputs = append(inputs, e.memoryPromptLineage)
	}
	if e.renderedSkillsMetadataLineage != nil {
		inputs = append(inputs, e.renderedSkillsMetadataLineage)
	} else if e.skillsManager != nil && e.skillsManager.IsEnabled() && len(e.skillsManager.GetAllMetadata()) > 0 {
		inputs = append(inputs, types.UnknownSourceLineage())
	}
	for _, kb := range e.knowledgeBasesInfo {
		if kb != nil {
			inputs = append(inputs, kb.SourceLineage)
		}
	}
	for _, doc := range e.selectedDocs {
		if doc != nil {
			inputs = append(inputs, doc.SourceLineage)
		}
	}
	if e.questionOrigin != nil {
		inputs = append(inputs, e.questionOrigin.SourceLineage)
		if e.questionOrigin.Document != nil {
			inputs = append(inputs, e.questionOrigin.Document.SourceLineage)
		}
	}
	for _, service := range e.pinnedMCPServices {
		if service != nil {
			inputs = append(inputs, service.SourceLineage)
		}
	}
	for _, skill := range e.pinnedSkills {
		if skill != nil {
			inputs = append(inputs, skill.SourceLineage)
		}
	}
	l, _ := types.UnionSourceLineage(inputs...)
	return l
}

func (e *AgentEngine) currentUserSourceLineage(images []string) *types.SourceLineage {
	// The engine's raw human-text producer is explicitly accounted here. An
	// image/attachment/resolved quote requires its own trusted reader receipt.
	input := independentAgentInput()
	if e.userInputLineage != nil || len(images) > 0 {
		input = e.userInputLineage
	}
	l, _ := types.UnionSourceLineage(input, e.promptSourceLineage())
	return l
}
