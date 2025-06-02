package rpcz

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var (
	// SamplingRate is the chance it samples the message.
	SamplingRate        = 1.0
	RetainRPCsPerMethod = 10
	RecordMetadata      = true
)

var (
	ClientOptions = []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(StreamClientInterceptor),
	}
	ServerOptions = []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(StreamServerInterceptor),
	}
)

var (
	mtx       sync.Mutex
	perMethod = map[string]*callForMethod{}
)

func sampleCall(method string) bool {
	if SamplingRate >= 1.0 {
		return true
	} else if SamplingRate <= 0.0 {
		return false
	}
	return rand.Float64() < SamplingRate
}

type callForMethod struct {
	calls []*capturedCall
	// ptr points at where the next call the circular buffer will be written.
	ptr int
}

// Callers should hold mtx.
func (cfm *callForMethod) add(c *capturedCall) {
	cfm.calls[cfm.ptr] = c
	cfm.ptr = (cfm.ptr + 1) % RetainRPCsPerMethod
}

type capturedCall struct {
	inbound       bool
	statusCode    codes.Code
	statusMessage string
	start         time.Time
	deadline      time.Duration
	duration      time.Duration
	peer          net.Addr
	metadata      metadata.MD
	messageBuffer
}

type capturedMessage struct {
	inbound bool
	stamp   time.Time
	message string
}

func (c *capturedCall) Start(ctx context.Context, md metadata.MD, req interface{}) {
	c.start = time.Now()
	if dl, ok := ctx.Deadline(); ok {
		c.deadline = dl.Sub(c.start)
	}
	c.metadata = md
	if req != nil {
		c.recordMessageLocked(req, c.inbound)
	}
}

func (c *capturedCall) Record(method string) {
	if c.inbound {
		method = "recv: " + method
	} else {
		method = "sent: " + method
	}
	mtx.Lock()
	defer mtx.Unlock()
	cfm, ok := perMethod[method]
	if !ok {
		cfm = &callForMethod{
			calls: make([]*capturedCall, RetainRPCsPerMethod),
		}
		perMethod[method] = cfm
	}
	cfm.add(c)
}

func (c *capturedCall) RecordMessage(msg interface{}, inbound bool) {
	mtx.Lock()
	defer mtx.Unlock()
	c.recordMessageLocked(msg, inbound)
}

func (c *capturedCall) recordMessageLocked(msg interface{}, inbound bool) {
	if c.huge {
		c.messageCount++
		return
	}
	if c.messageCount >= 64 {
		c.huge = true
		c.messageCount++
		return
	}
	var m string
	if str, ok := msg.(fmt.Stringer); ok {
		m = str.String()
	} else {
		m = fmt.Sprint(msg)
	}
	c.messageBuffer.addMessage(capturedMessage{
		inbound: inbound,
		stamp:   time.Now(),
		message: m,
	})
}

func (c *capturedCall) SetPeer(peer net.Addr) {
	mtx.Lock()
	defer mtx.Unlock()
	c.peer = peer
}

func (c *capturedCall) Complete(err error, peer net.Addr, addReply bool, reply interface{}) {
	mtx.Lock()
	defer mtx.Unlock()
	if addReply {
		c.recordMessageLocked(reply, !c.inbound)
	}
	c.duration = time.Now().Sub(c.start)
	st := status.Convert(err)
	c.statusCode = st.Code()
	c.statusMessage = st.Message()
	if peer != nil {
		c.peer = peer
	}
}

func UnaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if !sampleCall(method) {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	c := &capturedCall{}
	c.Start(ctx, metadataFromOutgoingContext(ctx), req)
	c.Record(method)
	var p peer.Peer
	opts = append(opts, grpc.Peer(&p))
	err := invoker(ctx, method, req, reply, cc, opts...)
	c.Complete(err, p.Addr, err == nil, reply)
	return err
}

func UnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if !sampleCall(info.FullMethod) {
		return handler(ctx, req)
	}
	c := &capturedCall{}
	c.inbound = true
	if p, ok := peer.FromContext(ctx); ok {
		c.peer = p.Addr
	}
	c.Start(ctx, metadataFromIncomingContext(ctx), req)
	c.Record(info.FullMethod)
	resp, err := handler(ctx, req)
	c.Complete(err, nil, err == nil, resp)
	return resp, err
}

func metadataFromIncomingContext(ctx context.Context) metadata.MD {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		return md
	}
	return nil
}

func metadataFromOutgoingContext(ctx context.Context) metadata.MD {
	if !RecordMetadata {
		return nil
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		return md
	}
	return nil
}
