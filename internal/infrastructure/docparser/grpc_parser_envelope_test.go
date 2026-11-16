package docparser

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/docreader/proto"
	"github.com/Tencent/WeKnora/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unaryLocatorServer struct {
	proto.UnimplementedDocReaderServer
}

func (unaryLocatorServer) Read(context.Context, *proto.ReadRequest) (*proto.ReadResponse, error) {
	return &proto.ReadResponse{
		MarkdownContent: "甲乙",
		SourceBlocks: []*proto.SourceBlock{{
			Start: 0, End: 2, LocatorJson: `{"type":"pdf","page":3}`,
		}},
	}, nil
}

func TestGRPCDocumentReaderFileCapAndUnarySourceBlocks(t *testing.T) {
	t.Setenv("MAX_FILE_SIZE_MB", "1")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	proto.RegisterDocReaderServer(server, unaryLocatorServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	reader, err := NewGRPCDocumentReader(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &types.ReadRequest{FileName: "at-cap.pdf", FileContent: make([]byte, 1<<20)}
	result, err := reader.Read(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SourceBlocks) != 1 || result.SourceBlocks[0].Locator.Page != 3 {
		t.Fatalf("unary fallback lost source blocks: %+v", result.SourceBlocks)
	}
	req.FileContent = append(req.FileContent, 0)
	if _, err := reader.Read(ctx, req); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversize file: got %v, want ResourceExhausted", err)
	}
}
