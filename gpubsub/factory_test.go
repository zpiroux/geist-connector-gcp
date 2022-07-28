package gpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist"
	"github.com/zpiroux/geist/entity"
)

func TestGeistIntegration(t *testing.T) {
	ctx := context.Background()
	geistConfig := geist.NewConfig()
	psConfig := PubsubConfig{}

	lf := NewMockExtractorFactory(ctx, psConfig)
	err := geistConfig.RegisterExtractorType(lf)
	assert.NoError(t, err)

	geist, err := geist.New(ctx, geistConfig)
	assert.NoError(t, err)

	assert.True(t, geist.Entities()["extractor"]["pubsub"])
	assert.False(t, geist.Entities()["loader"]["pubsub"])
	assert.False(t, geist.Entities()["loader"]["some_other_sink"])

	go func() {
		streamId, err := geist.RegisterStream(ctx, spec)
		assert.NoError(t, err)
		assert.Equal(t, "my-tiny-stream", streamId)

		// Since it's overkill to have a fully functional pubsub mock to which we can post events,
		// the inserted MockSubscription sends a single event to its Receive function, which the real
		// extractor is using, meaning the full geist and extractor logic and integration is tested.
		time.Sleep(2 * time.Second)

		err = geist.Shutdown(ctx)
		assert.NoError(t, err)
	}()

	err = geist.Run(ctx)
	assert.NoError(t, err)
}

type MockExtractorFactory struct {
	realExtractorFactory *extractorFactory
}

func NewMockExtractorFactory(ctx context.Context, config PubsubConfig) entity.ExtractorFactory {
	var mef MockExtractorFactory
	mef.realExtractorFactory = &extractorFactory{
		config: config,
		client: &MockClient{},
	}
	return &mef
}

func (mef *MockExtractorFactory) SourceId() string {
	return mef.realExtractorFactory.SourceId()
}

func (mef *MockExtractorFactory) NewExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	realExtractor, err := mef.realExtractorFactory.NewExtractor(ctx, spec, id)
	re := realExtractor.(*extractor)
	re.SetSub(&MockSubscription{})
	re.SetMsgAckNackFunc(ack, nack)
	return re, err
}

func (lf *MockExtractorFactory) Close() error {
	return nil
}

var spec = []byte(`
	{
		"namespace": "my",
		"streamIdSuffix": "tiny-stream",
		"description": "Tiny test stream logging event data to console.",
		"version": 1,
		"source": {
			"type": "pubsub",
			"config": {
				"topics": [
				   {
					  "env": "all",
					  "names": [
						 "my-cool-topic"
					  ]
				   }
				],
				"subscription": {
				   "type": "unique"
				}
			 }		
		},
		"transform": {
			"extractFields": [
				{
					"fields": [
						{
							"id": "rawEvent"
						}
					]
				}
			]
		},
		"sink": {
			"type": "void",
			"config": {
				"properties": [
				   {
					  "key": "logEventData",
					  "value": "true"
				   }
				]
			}
		}
	}
`)
