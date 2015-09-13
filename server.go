package sarama

import "net"

type Server struct {
	ln      net.Listener
	handler RequestHandler
}

type RequestHandler interface {
	Metadata(*MetadataRequest) *MetadataResponse
	Offset(*OffsetRequest) *OffsetResponse

	Produce(*ProduceRequest) *ProduceResponse
	Fetch(*FetchRequest) *FetchResponse

	ConsumerMetadata(*ConsumerMetadataRequest) *ConsumerMetadataResponse
	CommitOffset(*OffsetCommitRequest) *OffsetCommitResponse
	FetchOffset(*OffsetFetchRequest) *OffsetFetchResponse
}

func NewServer(addr string, handler RequestHandler) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{ln: ln, handler: handler}
	go s.acceptConns()
	return s, nil
}

func (s *Server) Close() error {
	return s.ln.Close()
}

func (s *Server) acceptConns() {
	for {
		conn, err := s.ln.Accept()
		switch err {
		case nil:
			go s.handleConn(conn)
		default:
			Logger.Println(err)
			return
		}
	}
}

func (s *Server) handleConn(conn net.Conn) {
	for {
		req, err := decodeRequest(conn)
		if err != nil {
			Logger.Println(err)
			Logger.Println(conn.Close())
			return
		}

		var responseBody encoder
		switch body := req.body.(type) {
		case *MetadataRequest:
			if r := s.handler.Metadata(body); r != nil {
				responseBody = r
			}
		case *OffsetRequest:
			if r := s.handler.Offset(body); r != nil {
				responseBody = r
			}
		case *ProduceRequest:
			if r := s.handler.Produce(body); r != nil {
				responseBody = r
			}
		case *FetchRequest:
			if r := s.handler.Fetch(body); r != nil {
				responseBody = r
			}
		case *ConsumerMetadataRequest:
			if r := s.handler.ConsumerMetadata(body); r != nil {
				responseBody = r
			}
		case *OffsetCommitRequest:
			if r := s.handler.CommitOffset(body); r != nil {
				responseBody = r
			}
		case *OffsetFetchRequest:
			if r := s.handler.FetchOffset(body); r != nil {
				responseBody = r
			}
		default:
		}

		if responseBody == nil {
			Logger.Println("nil response or unhandled request type, aborting connection")
			Logger.Println(conn.Close())
			return
		}

		responseBuf, err := encode(responseBody)
		if err != nil {
			Logger.Println(err)
			Logger.Println(conn.Close())
			return
		}

		responseHeader, err := encode(&responseHeader{
			length:        int32(len(responseBuf) + 4),
			correlationID: req.correlationID,
		})
		if err != nil {
			Logger.Println(err)
			Logger.Println(conn.Close())
			return
		}

		if _, err := conn.Write(responseHeader); err != nil {
			Logger.Println(err)
			Logger.Println(conn.Close())
			return
		}

		if _, err := conn.Write(responseBuf); err != nil {
			Logger.Println(err)
			Logger.Println(conn.Close())
			return
		}
	}
}
