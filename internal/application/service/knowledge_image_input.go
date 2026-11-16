package service

import "github.com/Tencent/WeKnora/internal/types"

func imageProducerControls(p types.ImageMultimodalPayload) any {
	legacy := struct {
		Tenant                                               uint64
		KB, KG, Parent, URL, Local, Digest, Language, Source string
		OCR, Caption                                         bool
		Attempt, Index                                       int
	}{
		p.TenantID,
		p.KnowledgeBaseID,
		p.KnowledgeID,
		p.ChunkID,
		p.ImageURL,
		p.ImageLocalPath,
		p.ImageDigest,
		p.Language,
		p.ImageSourceType,
		p.EnableOCR,
		p.EnableCaption,
		p.Attempt,
		p.ImageIndex,
	}
	if !hasImagePipelineControls(p) {
		return legacy
	}
	return struct {
		Original       any
		Pipeline       types.ImagePipelineID
		Params         map[string]any
		Attrs          bool
		Actions        types.ImageActionsConfig
		SourceLocators types.SourceLocators
		Vector         *bool
	}{
		legacy, p.ImagePipelineID, p.ImagePipelineParams,
		p.ImageAttrsEnabled, p.ImageActions, p.SourceLocators, p.ImageVectorEnabled,
	}
}

func hasImagePipelineControls(p types.ImageMultimodalPayload) bool {
	return p.ImagePipelineID != "" || len(p.ImagePipelineParams) != 0 || p.ImageAttrsEnabled ||
		len(p.ImageActions.OCR.On) != 0 || len(p.SourceLocators) != 0 || p.ImageVectorEnabled != nil
}
