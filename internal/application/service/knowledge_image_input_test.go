package service

import (
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestImageInputBindsRoutingAndRetainsLegacyReceipt(t *testing.T) {
	const queued = `{"tenant_id":4,"knowledge_base_id":"kb","knowledge_id":"doc","chunk_id":"p",` +
		`"image_url":"local://original","_image_digest":"sum","enable_ocr":true,"enable_caption":false}`
	var p types.ImageMultimodalPayload
	if err := json.Unmarshal([]byte(queued), &p); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(imageProducerControls(p))
	if err != nil {
		t.Fatal(err)
	}
	// Existing private receipts are digests of this exact JSON layout.
	const legacy = `{"Tenant":4,"KB":"kb","KG":"doc","Parent":"p",` +
		`"URL":"local://original","Local":"","Digest":"sum","Language":"",` +
		`"Source":"","OCR":true,"Caption":false,"Attempt":0,` +
		`"Index":0}`
	if string(encoded) != legacy {
		t.Fatalf("in-flight image receipt changed: %s", encoded)
	}
	p.ImagePipelineID = types.ImagePipelineDefault
	reference := types.GraphDigest(imageProducerControls(p))
	on := true
	for _, change := range []func(*types.ImageMultimodalPayload){
		func(q *types.ImageMultimodalPayload) { q.ImagePipelineID = types.ImagePipelineSmartOCR },
		func(q *types.ImageMultimodalPayload) { q.ImagePipelineParams = map[string]any{"ocr_enabled": false} },
		func(q *types.ImageMultimodalPayload) { q.ImageAttrsEnabled = true },
		func(q *types.ImageMultimodalPayload) { q.ImageActions = types.DefaultImageActions() },
		func(q *types.ImageMultimodalPayload) { q.SourceLocators = types.SourceLocators{{}} },
		func(q *types.ImageMultimodalPayload) { q.ImageVectorEnabled = &on },
	} {
		modified := p
		change(&modified)
		if types.GraphDigest(imageProducerControls(modified)) == reference {
			t.Fatal("changed image job controls retained the same receipt")
		}
	}
}
