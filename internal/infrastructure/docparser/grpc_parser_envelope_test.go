package docparser

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Tencent/WeKnora/docreader/proto"
	"github.com/Tencent/WeKnora/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type envelopeDocReaderServer struct {
	pb.UnimplementedDocReaderServer
	unary      bool
	replyBytes int
	calls      atomic.Int32
}

func (s *envelopeDocReaderServer) text(req *pb.ReadRequest) string {
	if s.replyBytes > 0 {
		return strings.Repeat("x", s.replyBytes)
	}
	return fmt.Sprintf("parsed:%x", sha256.Sum256(req.FileContent))
}

func (s *envelopeDocReaderServer) Read(_ context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.calls.Add(1)
	return &pb.ReadResponse{MarkdownContent: s.text(req)}, nil
}

func (s *envelopeDocReaderServer) ReadStream(
	req *pb.ReadRequest, stream grpc.ServerStreamingServer[pb.ReadStreamResponse],
) error {
	s.calls.Add(1)
	if s.unary {
		return status.Error(codes.Unimplemented, "older DocReader")
	}
	return stream.Send(&pb.ReadStreamResponse{Payload: &pb.ReadStreamResponse_Meta{Meta: &pb.ReadStreamMeta{
		MarkdownContent: s.text(req),
	}}})
}

func TestGRPCDocumentReaderFileEnvelope(t *testing.T) {
	t.Setenv("MAX_FILE_SIZE_MB", "1")
	const limit = 1 << 20
	for _, unary := range []bool{false, true} {
		for _, tt := range []struct {
			name                  string
			size, metadata, reply int
			reject                bool
		}{
			{"below limit", limit - 8192, 0, 0, false},
			{"file exactly at upload cap", limit, 0, 0, false},
			{"legal file with parser metadata", limit - 512, 4096, 0, false},
			{"response content at cap", 16, 0, limit, false},
			{"raw file over cap", limit + 1, 0, 0, true},
			{"metadata beyond envelope budget", 16, 3 << 20, 0, true},
		} {
			t.Run(fmt.Sprintf("%s/unary=%t", tt.name, unary), func(t *testing.T) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				server := grpc.NewServer(
					grpc.MaxRecvMsgSize(limit+(1<<20)), grpc.MaxSendMsgSize(limit+(1<<20)),
				)
				remote := &envelopeDocReaderServer{unary: unary, replyBytes: tt.reply}
				pb.RegisterDocReaderServer(server, remote)
				go func() { _ = server.Serve(listener) }()
				t.Cleanup(func() { server.Stop(); _ = listener.Close() })
				reader, err := NewGRPCDocumentReader(listener.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = reader.Close() })
				overrides := map[string]string{"pdf_force_scanned": "false"}
				if tt.metadata > 0 {
					overrides["parser_metadata"] = strings.Repeat("m", tt.metadata)
				}
				content := []byte(strings.Repeat("a", tt.size))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				got, err := reader.Read(ctx, &types.ReadRequest{
					FileContent: content, FileName: "report.txt", FileType: "txt",
					ParserEngine: "builtin", RequestID: "envelope-test", ParserEngineOverrides: overrides,
				})
				if tt.reject {
					if status.Code(err) != codes.ResourceExhausted {
						t.Fatalf("oversized request error=%v", err)
					}
					if remote.calls.Load() != 0 {
						t.Fatal("oversized input reached the parser")
					}
					return
				}
				if err != nil {
					t.Fatalf("legal file rejected: %v", err)
				}
				want := fmt.Sprintf("parsed:%x", sha256.Sum256(content))
				if tt.reply > 0 {
					want = strings.Repeat("x", tt.reply)
				}
				if got == nil || got.MarkdownContent != want {
					t.Fatal("request or response bytes changed")
				}
				wantCalls := int32(1)
				if unary {
					wantCalls = 2
				}
				if remote.calls.Load() != wantCalls {
					t.Fatalf("RPC calls=%d want=%d", remote.calls.Load(), wantCalls)
				}
			})
		}
	}
}

type unaryLocatorServer struct {
	pb.UnimplementedDocReaderServer
}

func (unaryLocatorServer) Read(context.Context, *pb.ReadRequest) (*pb.ReadResponse, error) {
	return &pb.ReadResponse{
		MarkdownContent: "甲乙",
		SourceBlocks: []*pb.SourceBlock{{
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
	pb.RegisterDocReaderServer(server, unaryLocatorServer{})
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
