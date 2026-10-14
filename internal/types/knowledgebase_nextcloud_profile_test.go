package types

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNextcloudKnowledgeBaseMarshalOmitsHistoricalGeneratedProfile(t *testing.T) {
	kb := &KnowledgeBase{
		ID: "kb-nextcloud", EverHadNextcloudSource: true,
		GeneratedProfile: &KnowledgeBaseProfile{Gist: "private-department-summary"},
	}
	body, err := json.Marshal(kb)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "private-department-summary") || strings.Contains(string(body),
		"generated_profile") {
		t.Fatalf("Nextcloud profile leaked from KB response: %s", body)
	}
	if kb.GeneratedProfile == nil {
		t.Fatal("serialization changed the stored profile")
	}
}
