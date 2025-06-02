package rpcz

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func StreamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if !sampleCall(method) {
		return streamer(ctx, desc, cc, method, opts...)
	}
	c := &capturedCall{}
	c.Start(ctx, metadataFromOutgoingContext(ctx), nil)
	c.Record(method)
	var p peer.Peer
	opts = append(opts, grpc.Peer(&p))
	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		c.Complete(err, p.Addr, false, nil)
		return stream, err
	}
	if p, ok := peer.FromContext(stream.Context()); ok { // grpc.Peer() will only fill p after the entire call completes, so we have to fetch it from the context here.
		c.SetPeer(p.Addr)
	}
	return &clientStream{stream, c}, nil
}

type clientStream struct {
	parent grpc.ClientStream
	cc     *capturedCall
}

func (s *clientStream) Header() (metadata.MD, error) {
	return s.parent.Header()
}

func (s *clientStream) Trailer() metadata.MD {
	return s.parent.Trailer()
}

func (s *clientStream) Context() context.Context {
	return s.parent.Context()
}

func (s *clientStream) CloseSend() error {
	if err := s.parent.CloseSend(); err != nil {
		s.cc.Complete(err, nil, false, nil)
		return err
	}
	return nil
}

func (s *clientStream) SendMsg(m interface{}) error {
	s.cc.RecordMessage(m, false)
	return s.parent.SendMsg(m)
}

func (s *clientStream) RecvMsg(m interface{}) error {
	if err := s.parent.RecvMsg(m); err != nil {
		if err == io.EOF {
			s.cc.Complete(nil, nil, false, nil)
		} else {
			s.cc.Complete(err, nil, false, nil)
		}
		return err
	}
	s.cc.RecordMessage(m, true)
	return nil
}

func StreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !sampleCall(info.FullMethod) {
		return handler(srv, ss)
	}
	c := &capturedCall{}
	c.inbound = true
	if p, ok := peer.FromContext(ss.Context()); ok {
		c.peer = p.Addr
	}
	c.Start(ss.Context(), metadataFromIncomingContext(ss.Context()), nil)
	c.Record(info.FullMethod)
	err := handler(srv, &serverStream{ss, c})
	c.Complete(err, nil, false, nil)
	return err
}

type serverStream struct {
	parent grpc.ServerStream
	cc     *capturedCall
}

func (s *serverStream) SetHeader(md metadata.MD) error {
	return s.parent.SetHeader(md)
}

func (s *serverStream) SendHeader(md metadata.MD) error {
	return s.parent.SendHeader(md)
}

func (s *serverStream) SetTrailer(md metadata.MD) {
	s.parent.SetTrailer(md)
}

func (s *serverStream) Context() context.Context {
	return s.parent.Context()
}

func (s *serverStream) SendMsg(m interface{}) error {
	if err := s.parent.SendMsg(m); err != nil {
		s.cc.Complete(err, nil, true, m)
		return err
	}
	s.cc.RecordMessage(m, false)
	return nil
}

func (s *serverStream) RecvMsg(m interface{}) error {
	if err := s.parent.RecvMsg(m); err != nil {
		if err != io.EOF {
			s.cc.Complete(err, nil, false, nil)
		}
		return err
	}
	s.cc.RecordMessage(m, true)
	return nil
}
