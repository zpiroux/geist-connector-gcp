package gpubsub

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

var printTestOutput bool

func TestNewExtractor(t *testing.T) {

	spec, err := entity.NewSpec(regSpecPubsub)
	assert.NoError(t, err)

	printTestOutput = true
	id := "mockId"
	ec := &extractorConfig{
		client: &MockClient{},
		spec:   spec,
	}

	client := MockClient{}
	ctx := context.Background()

	tPrintf("Starting TestNewExtractor\n")

	// Check handling of incorrect input
	_, err = newExtractor(ctx, ec, id)
	assert.Error(t, err)

	ec.client = &client
	_, err = newExtractor(ctx, ec, id)
	assert.Error(t, err)

	// Valid input
	extractor := newTestExtractor(t, regSpecPubsub)
	assert.NotNil(t, extractor)

	extractor = newTestExtractor(t, pubsubSrcKafkaSinkSpec)
	assert.NotNil(t, extractor)
}

// This test currently test extraction of a single event (sent by MockSubscription.Receive()
func TestExtractor_StreamExtract(t *testing.T) {

	var (
		err       error
		retryable bool
	)
	ctx := context.Background()

	tPrintf("Starting TestExtractor_StreamExtract\n")

	extractor := newTestExtractor(t, regSpecPubsub)
	assert.NotNil(t, extractor)

	extractor.SetSub(&MockSubscription{})
	extractor.SetMsgAckNackFunc(ack, nack)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		extractor.StreamExtract(
			ctx,
			reportEvent,
			&err,
			&retryable)
		wg.Done()
	}()
	wg.Wait()
	assert.NoError(t, err)
}

func reportEvent(ctx context.Context, events []entity.Event) entity.EventProcessingResult {
	return entity.EventProcessingResult{Status: entity.ExecutorStatusSuccessful}
}

func TestExtractor_SendToSource(t *testing.T) {

	var err error

	client := &MockClient{}
	ctx := context.Background()

	spec, err := entity.NewSpec(regSpecPubsub)
	assert.NoError(t, err)

	ec := newExtractorConfig(client, spec, []string{"coolTopic"}, receiveSettings{})
	extractor, err := newExtractor(ctx, ec, "mockId")
	assert.NoError(t, err)
	assert.NotNil(t, extractor)

	extractor.SetTopic(&MockTopic{})
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := extractor.SendToSource(ctx, "Hi! I'm an event as string")
		assert.EqualError(t, err, "context canceled")
		_, err = extractor.SendToSource(ctx, []byte("Hi! I'm an event as bytes"))
		assert.EqualError(t, err, "context canceled")
		_, err = extractor.SendToSource(ctx, 123456789)
		assert.EqualError(t, err, "invalid type for eventData (int), only string and []byte allowed")
		wg.Done()
	}()
	cancel()
	wg.Wait()
}

func newTestExtractor(t *testing.T, specData []byte) *extractor {

	client := &MockClient{}
	ctx := context.Background()

	spec, err := entity.NewSpec(specData)
	assert.NoError(t, err)

	ec := newExtractorConfig(client, spec, []string{"coolTopic"}, receiveSettings{})
	extractor, err := newExtractor(ctx, ec, "mockId")
	assert.NoError(t, err)
	assert.NotNil(t, extractor)

	return extractor
}

type MockClient struct{}

func (m *MockClient) Topic(id string) *pubsub.Topic {
	return &pubsub.Topic{}
}

func (m *MockClient) CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	return &pubsub.Subscription{}, nil
}

func (m *MockClient) Subscription(id string) *pubsub.Subscription {
	return &pubsub.Subscription{}
}

type MockSubscription struct {
	name string
}

// The real Receive() runs until canceled, but this mock one currently only sends one event
// and then exits without error.
// TODO: Add more scenarios
func (s *MockSubscription) Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error {

	tPrintf("In Receive in MockSubscription\n")

	msg := pubsub.Message{
		Data:        []byte("foo"),
		ID:          "mockMsgId",
		PublishTime: time.Now(),
	}

	f(ctx, &msg)

	return nil
}

func (s *MockSubscription) Delete(ctx context.Context) error {
	return nil
}

func (s *MockSubscription) String() string {
	return s.name
}

type MockTopic struct {
}

func (t *MockTopic) Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult {

	return &pubsub.PublishResult{}

}

func ack(m *pubsub.Message) {
	tPrintf("Ack called with msg.Data: %s, full msg: %+v\n", string(m.Data), m)
}

func nack(m *pubsub.Message) {
	tPrintf("Nack called with msg.Data: %s, full msg: %+v\n", string(m.Data), m)
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}

// TODO: Maybe move these into test dir as json files

var regSpecPubsub = []byte(`
{
	"namespace": "geisttest",
	"streamIdSuffix": "spec-reg",
	"description": "An example of a simple custom/alternative Spec registration stream, using pubsub as source instead of geistapi.",
	"version": 1,
	"source": {
	   "type": "pubsub",
	   "config": {
		  "topics": [
			 {
				"env": "all",
				"names": [
				   "geist-spec-reg"
				]
			 }
		  ],
		  "subscription": {
			 "type": "shared",
			 "name": "geist-spec-reg-sub"
		  }
	   }
	},
	"transform": {
	   "extractFields": [
		  {
			 "fields": [
				{
				   "id": "namespace",
				   "jsonPath": "namespace"
				},
				{
				   "id": "idSuffix",
				   "jsonPath": "streamIdSuffix"
				},
				{
				   "id": "rawEvent",
				   "type": "string"
				}
			 ]
		  }
	   ]
	},
	"sink": {
	   "type": "firestore",
	   "config": {
		  "kinds": [
			 {
				"name": "EtlSpec",
				"entityNameFromIds": {
				   "ids": [
					  "namespace",
					  "idSuffix"
				   ],
				   "delimiter": "-"
				},
				"properties": [
				   {
					  "id": "rawEvent",
					  "name": "specData",
					  "index": false
				   }
				]
			 }
		  ]
	   }
	}
 }
`)

var pubsubSrcKafkaSinkSpec = []byte(`
{
	"namespace": "geisttest",
	"streamIdSuffix": "foologanalytics",
	"description": "A stream for transforming Foo log data into SystemX events, to provide interactive Foo log analytic capabilities, including cost-efficient metric creation.",
	"version": 1,
	"source": {
	   "type": "pubsub",
	   "config": {
		  "topics": [
			 {
				"env": "all",
				"names": [
				   "geisttest-foologanalytics"
				]
			 }
		  ],
		  "subscription": {
			 "type": "shared",
			 "name": "geist-foologdata"
		  }
	   }
	},
	"transform": {
	   "extractFields": [
		  {
			 "fields": [
				{
				   "id": "payload"
				}
			 ]
		  }
	   ]
	},
	"sink": {
	   "type": "kafka",
	   "config": {
		  "topic": [
			 {
				"env": "all",
				"topicSpec": {
				   "name": "geisttest.foologdatasink",
				   "numPartitions": 6,
				   "replicationFactor": 3
				}
			 }
		  ],
		  "properties": [
			 {
				"key": "client.id",
				"value": "geisttest_foologanalytics"
			 }
		  ],
		  "message": {
			 "payloadFromId": "payload"
		  }
	   }
	}
 }
`)
