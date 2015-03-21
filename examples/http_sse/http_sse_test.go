package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

func TestSSEMessage(t *testing.T) {
	message := &sarama.ConsumerMessage{
		Topic:     "test",
		Partition: 1,
		Key:       []byte("test"),
		Value:     []byte("hello \nworld"),
		Offset:    23456,
	}

	sseMessage := kafkaMessageToSSEMessage(message)
	if sseMessage != "id: 23456\ndata: hello \ndata: world\n\n" {
		t.Error("Invalid sse message,", sseMessage)
	}
}

func TestHandler404(t *testing.T) {
	req, err := http.NewRequest("GET", "http://example.com/", nil)
	if err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()

	s := &SSEServer{}
	s.Handler().ServeHTTP(res, req)

	if res.Code != 404 {
		t.Error("Expected HTTP status 404, found", res.Code)
	}
}

func TestHandlerHTML(t *testing.T) {
	req, err := http.NewRequest("GET", "http://example.com/topic/123.html", nil)
	if err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()

	s := &SSEServer{}
	s.Handler().ServeHTTP(res, req)

	if res.Code != 200 {
		t.Error("Expected HTTP status 200, found", res.Code)
	}

	if res.Header().Get("Content-Type") != "text/html" {
		t.Error("Expected text/html, found", res.Header().Get("Content-Type"))
	}
}

func TestPartitionStream(t *testing.T) {
	res := httptest.NewRecorder()

	cons := mocks.NewConsumer(t, nil)
	pcm := cons.ExpectConsumePartition("my_topic", 0, sarama.OffsetNewest)
	pcm.YieldMessage(&sarama.ConsumerMessage{Offset: 123, Value: []byte("Hello world")})
	pcm.YieldMessage(&sarama.ConsumerMessage{Value: []byte("Goodbye world")})

	pc, err := cons.ConsumePartition("my_topic", 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatal(err)
	}
	pc.AsyncClose()

	streamMessages(res, pc)

	if res.Header().Get("Content-Type") != "text/event-stream" {
		t.Error("Expect the proper Content-Type header to be set")
	}

	if !res.Flushed {
		t.Error("Expected the response to be flushed")
	}

	expectedBody := "id: 123\ndata: Hello world\n\nid: 124\ndata: Goodbye world\n\n"
	if expectedBody != res.Body.String() {
		t.Error("Response body is unexpected:", res.Body.String())
	}

	if err := cons.Close(); err != nil {
		t.Error(err)
	}
}
