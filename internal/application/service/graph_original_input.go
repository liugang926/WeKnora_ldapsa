package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type graphOriginalInputVerifier struct {
	reader interfaces.KnowledgeBuildInputReader
}

func NewGraphOriginalInputVerifier(reader interfaces.KnowledgeBuildInputReader) GraphOriginalInputVerifier {
	return &graphOriginalInputVerifier{reader: reader}
}

func (v *graphOriginalInputVerifier) RecheckGraphOriginalInputs(
	ctx context.Context,
	receipt *types.GraphSourceReceipt,
) error {
	if v == nil || v.reader == nil || receipt.Validate() != nil {
		return types.ErrGraphSourceReceiptUnknown
	}
	copy, err := types.CopyGraphSourceReceipt(receipt)
	if err != nil {
		return err
	}
	return v.reader.RecheckKnowledgeBuildInputs(ctx, copy.OriginalInput)
}
