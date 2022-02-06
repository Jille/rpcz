package rpcz

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var (
	// SamplingRate is the chance it samples the message.
	SamplingRate        = 1.0
	RetainRPCsPerMethod = 10
)

const (
	KeepFirstNStreamingMessages = 5
	KeepLastNStreamingMessages  = 5
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

	capturedCallPool = sync.Pool{
		New: func() interface{} {
			return &capturedCall{}
		},
	}
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
	if old := cfm.calls[cfm.ptr]; old != nil {
		*old = capturedCall{}
		capturedCallPool.Put(old)
	}
	cfm.calls[cfm.ptr] = c
	cfm.ptr = (cfm.ptr + 1) % RetainRPCsPerMethod
}

type capturedCall struct {
	inbound      bool
	huge         bool
	start        time.Time
	deadline     time.Duration
	duration     time.Duration
	status       *status.Status
	peer         net.Addr
	messages     [KeepFirstNStreamingMessages + KeepLastNStreamingMessages]capturedMessage
	messageCount uint64
}

type capturedMessage struct {
	inbound bool
	stamp   time.Time
	message string
}

func (c *capturedCall) Start(ctx context.Context, req interface{}) {
	c.start = time.Now()
	if dl, ok := ctx.Deadline(); ok {
		c.deadline = dl.Sub(c.start)
	}
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
	var m string
	if str, ok := msg.(fmt.Stringer); ok {
		m = str.String()
	} else {
		m = fmt.Sprint(msg)
	}
	cm := capturedMessage{
		inbound: inbound,
		stamp:   time.Now(),
		message: m,
	}
	if c.messageCount < KeepFirstNStreamingMessages+KeepLastNStreamingMessages {
		c.messages[c.messageCount] = cm
	} else {
		c.messages[KeepFirstNStreamingMessages+((c.messageCount-KeepFirstNStreamingMessages)%KeepLastNStreamingMessages)] = cm
		if c.messageCount >= 64 {
			c.huge = true
		}
	}
	c.messageCount++
}

func (c *capturedCall) firstMessages() []capturedMessage {
	if c.messageCount < KeepFirstNStreamingMessages {
		return c.messages[:c.messageCount]
	}
	return c.messages[:KeepFirstNStreamingMessages]
}

func (c *capturedCall) lastMessages() []capturedMessage {
	if c.huge || c.messageCount <= KeepFirstNStreamingMessages {
		return nil
	}
	if (c.messageCount-KeepFirstNStreamingMessages)%KeepLastNStreamingMessages == 0 {
		// We can avoid a copy.
		return c.messages[KeepFirstNStreamingMessages:]
	}
	ret := make([]capturedMessage, KeepLastNStreamingMessages)
	p := copy(ret, c.messages[KeepFirstNStreamingMessages+((c.messageCount-KeepFirstNStreamingMessages)%KeepLastNStreamingMessages):])
	copy(ret[p:], c.messages[KeepLastNStreamingMessages:])
	return ret
}

func (c *capturedCall) droppedMessages() uint64 {
	if c.messageCount <= KeepFirstNStreamingMessages+KeepLastNStreamingMessages {
		return 0
	}
	return c.messageCount - KeepFirstNStreamingMessages + KeepLastNStreamingMessages
}

func (c *capturedCall) Complete(err error, peer net.Addr, addReply bool, reply interface{}) {
	mtx.Lock()
	defer mtx.Unlock()
	if addReply {
		c.recordMessageLocked(reply, !c.inbound)
	}
	c.duration = time.Now().Sub(c.start)
	c.status = status.Convert(err)
	if peer != nil {
		c.peer = peer
	}
}

func UnaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if !sampleCall(method) {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	c := capturedCallPool.Get().(*capturedCall)
	c.Start(ctx, req)
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
	c := capturedCallPool.Get().(*capturedCall)
	c.inbound = true
	if p, ok := peer.FromContext(ctx); ok {
		c.peer = p.Addr
	}
	c.Start(ctx, req)
	c.Record(info.FullMethod)
	resp, err := handler(ctx, req)
	c.Complete(err, nil, err == nil, resp)
	return resp, err
}
