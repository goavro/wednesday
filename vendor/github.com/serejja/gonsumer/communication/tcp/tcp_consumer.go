package tcp

import (
	"bufio"
	"encoding/json"
	"errors"
	log "github.com/golang/glog"
	"github.com/serejja/gonsumer"
	"net"
)

const messageDelimiter = byte('\n')

// Consumer is a TCP layer that can wrap a Gonsumer to expose its functionality via TCP.
type Consumer struct {
	address        string
	consumer       gonsumer.Consumer
	customHandlers map[string]func([]byte) (*Response, error)

	close  chan struct{}
	closed chan struct{}
}

// NewConsumer wraps the given Gonsumer to listen for external commands on the given address.
func NewConsumer(addr string, consumer gonsumer.Consumer) *Consumer {
	return &Consumer{
		address:        addr,
		consumer:       consumer,
		customHandlers: make(map[string]func([]byte) (*Response, error)),
		close:          make(chan struct{}),
		closed:         make(chan struct{}),
	}
}

// RegisterCustomHandler allows to expose any additional functionality which is not exposed by default.
func (tc *Consumer) RegisterCustomHandler(key string, handler func([]byte) (*Response, error)) {
	tc.customHandlers[key] = handler
}

// Start starts listening the given TCP address. Returns an error if anything went wrong while
// listening TCP.
func (tc *Consumer) Start() error {
	log.Info("Starting TCP consumer")
	tcpAddr, err := net.ResolveTCPAddr("tcp", tc.address)
	if err != nil {
		return err
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}

	log.Info("Listening TCP at %s", tc.address)
	go tc.closeListener(listener)
	go tc.listen(listener)

	return nil
}

// Stop stops both this TCP wrapper and the underlying consumer.
func (tc *Consumer) Stop() {
	log.Info("Stopping TCP consumer")
	tc.close <- struct{}{}
	tc.consumer.Stop()
	<-tc.closed
}

// AwaitTermination blocks until Stop() is called.
func (tc *Consumer) AwaitTermination() {
	tc.consumer.AwaitTermination()
}

// Join blocks until consumer has at least one topic/partition to consume, e.g. until len(Assignment()) > 0.
func (tc *Consumer) Join() {
	tc.consumer.Join()
}

func (tc *Consumer) listen(listener *net.TCPListener) {
	for {
		if log.V(2) {
			log.Info("Accepting client connection")
		}
		connection, err := listener.Accept()
		if err != nil {
			log.Warning("%s", err)
			return
		}

		if log.V(2) {
			log.Info("Accepted client connection")
		}
		go tc.handleConnection(connection)
	}
}

func (tc *Consumer) closeListener(listener *net.TCPListener) {
	<-tc.close
	err := listener.Close()
	if err != nil {
		log.Warning("%s", err)
	}
	close(tc.closed)
}

func (tc *Consumer) handleConnection(connection net.Conn) {
	scanner := bufio.NewScanner(connection)
	for scanner.Scan() {
		response, err := tc.handleRequest(scanner.Text())
		if err != nil {
			response = NewResponse(false, err.Error(), nil)
		}

		jsonResponse, err := json.Marshal(response)
		if err != nil {
			log.Warning("Cannot marshal response to JSON: %s", err)
		} else {
			_, err = connection.Write(append(jsonResponse, messageDelimiter))
			if err != nil {
				log.Warning("Cannot write response to connection: %s", err)
			}
		}
	}
}

func (tc *Consumer) handleRequest(req string) (*Response, error) {
	request := new(request)
	err := json.Unmarshal([]byte(req), &request)
	if err != nil {
		return nil, err
	}

	switch request.Key {
	case RequestKeyAdd:
		return tc.handleAdd(request)
	case RequestKeyRemove:
		return tc.handleRemove(request)
	case RequestKeyAssignments:
		return tc.handleAssignments(request), nil
	case RequestKeyOffset:
		return tc.handleOffset(request)
	case RequestKeyCommit:
		return tc.handleCommit(request)
	case RequestKeySetOffset:
		return tc.handleSetOffset(request)
	case RequestKeyLag:
		return tc.handleLag(request)
	default:
		{
			handler, ok := tc.customHandlers[request.Key]
			if ok {
				return handler([]byte(request.Data))
			}
			log.Warning("Unknown request key %s", request.Key)
		}
	}

	return nil, errors.New("Unknown request key")
}

func (tc *Consumer) handleAdd(request *request) (*Response, error) {
	tp := new(topicPartition)
	err := json.Unmarshal(request.Data, &tp)
	if err != nil {
		return nil, err
	}

	err = tc.consumer.Add(tp.Topic, tp.Partition)
	if err != nil {
		return nil, err
	}

	return NewResponse(true, "", nil), nil
}

func (tc *Consumer) handleRemove(request *request) (*Response, error) {
	tp := new(topicPartition)
	err := json.Unmarshal(request.Data, &tp)
	if err != nil {
		return nil, err
	}

	err = tc.consumer.Remove(tp.Topic, tp.Partition)
	if err != nil {
		return nil, err
	}

	return NewResponse(true, "", nil), nil
}

func (tc *Consumer) handleAssignments(request *request) *Response {
	return NewResponse(true, "", tc.consumer.Assignment())
}

func (tc *Consumer) handleOffset(request *request) (*Response, error) {
	tp := new(topicPartition)
	err := json.Unmarshal(request.Data, &tp)
	if err != nil {
		return nil, err
	}

	offset, err := tc.consumer.Offset(tp.Topic, tp.Partition)
	if err != nil {
		return nil, err
	}

	return NewResponse(true, "", offset), nil
}

func (tc *Consumer) handleCommit(request *request) (*Response, error) {
	tp := new(topicPartitionOffset)
	err := json.Unmarshal(request.Data, &tp)
	if err != nil {
		return nil, err
	}

	err = tc.consumer.Commit(tp.Topic, tp.Partition, tp.Offset)
	if err != nil {
		return nil, err
	}

	return NewResponse(true, "", nil), nil
}

func (tc *Consumer) handleSetOffset(request *request) (*Response, error) {
	tp := new(topicPartitionOffset)
	err := json.Unmarshal(request.Data, &tp)
	if err != nil {
		return nil, err
	}

	err = tc.consumer.SetOffset(tp.Topic, tp.Partition, tp.Offset)
	if err != nil {
		return nil, err
	}

	return NewResponse(true, "", nil), nil
}

func (tc *Consumer) handleLag(request *request) (*Response, error) {
	tp := new(topicPartition)
	err := json.Unmarshal(request.Data, &tp)
	if err != nil {
		return nil, err
	}

	lag, err := tc.consumer.Lag(tp.Topic, tp.Partition)
	if err != nil {
		return nil, err
	}

	return NewResponse(true, "", lag), nil
}
