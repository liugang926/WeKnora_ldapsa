package session

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestHasExplicitInputMaterialsExcludesQuestionOrigin(t *testing.T) {
	origin := &types.QuestionOrigin{
		KnowledgeBaseID: "kb-1",
		KnowledgeID:     "doc-1",
	}
	tests := []struct {
		name string
		req  *qaRequestContext
		want bool
	}{
		{name: "nil request"},
		{
			name: "question origin is not an explicit material",
			req:  &qaRequestContext{questionOrigin: origin},
		},
		{
			name: "image remains explicit",
			req:  &qaRequestContext{images: []ImageAttachment{{}}},
			want: true,
		},
		{
			name: "inline attachment remains explicit",
			req: &qaRequestContext{
				attachments: types.MessageAttachments{{}},
			},
			want: true,
		},
		{
			name: "pre-uploaded attachment remains explicit",
			req:  &qaRequestContext{attachmentIDs: []string{"attachment-1"}},
			want: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, hasExplicitInputMaterials(test.req))
		})
	}
}
