package gbigquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist"
	"github.com/zpiroux/geist/entity"
)

func TestGeistIntegration(t *testing.T) {
	ctx := context.Background()
	geistConfig := geist.NewConfig()
	bqConfig := Config{}

	lf := NewMockLoaderFactory(ctx, bqConfig)
	err := geistConfig.RegisterLoaderType(lf)
	assert.NoError(t, err)

	geist, err := geist.New(ctx, geistConfig)
	assert.NoError(t, err)

	assert.True(t, geist.Entities()["loader"]["bigquery"])
	assert.False(t, geist.Entities()["extractor"]["bigquery"])
	assert.False(t, geist.Entities()["loader"]["some_other_sink"])

	go func() {
		streamId, err := geist.RegisterStream(ctx, spec)
		assert.NoError(t, err)
		assert.Equal(t, "geist-xtobigquery", streamId)
		err = geist.Shutdown(ctx)
		assert.NoError(t, err)
	}()

	err = geist.Run(ctx)
	assert.NoError(t, err)
}

type MockLoaderFactory struct {
	realLoaderFactory *loaderFactory
}

func NewMockLoaderFactory(ctx context.Context, config Config) entity.LoaderFactory {
	var mlf MockLoaderFactory
	mlf.realLoaderFactory = &loaderFactory{
		config: config,
		client: nil,
	}
	return &mlf
}

func (mlf *MockLoaderFactory) SinkId() string {
	return mlf.realLoaderFactory.SinkId()
}

func (mlf *MockLoaderFactory) NewLoader(ctx context.Context, c entity.Config) (entity.Loader, error) {
	return newLoader(ctx, c.Spec, c.ID, &MockBigQueryClient{}, &bigQueryMetadataMutex)
}

func (mlf *MockLoaderFactory) NewSinkExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return nil, nil
}

func (mlf *MockLoaderFactory) Close(ctx context.Context) error {
	return nil
}

var spec = []byte(`
{
   "namespace": "geist",
   "streamIdSuffix": "xtobigquery",
   "description": "Generic spec for any source storing raw events in a simple table",
   "version": 1,
   "ops": {
      "logEventData": true
   },
   "source": {
      "type": "geistapi"
   },
   "transform": {
      "extractFields": [
         {
            "fields": [
               {
                  "id": "eventNameId",
                  "jsonPath": "name"
               },
               {
                  "id": "eventId",
                  "jsonPath": "eventId"
               },
               {
                  "id": "rawEventId",
                  "type": "string"
               }
            ]
         }
      ]
   },
   "sink": {
      "type": "bigquery",
      "config": {
         "tables": [
            {
               "name": "gotest_general",
               "dataset": "geisttest",
               "insertIdFromId": "eventId",
               "columns": [
                  {
                     "name": "eventName",
                     "mode": "REQUIRED",
                     "type": "STRING",
                     "description": "name of the event",
                     "valueFromId": "eventNameId"
                  },
                  {
                     "name": "eventData",
                     "mode": "NULLABLE",
                     "type": "STRING",
                     "description": "raw event data",
                     "valueFromId": "rawEventId"
                  },
                  {
                     "name": "dateIngested",
                     "mode": "NULLABLE",
                     "type": "TIMESTAMP",
                     "description": "ingestion timestamp",
                     "valueFromId": "@GeistIngestionTime"
                  }
               ]
            }
         ]
      }
   }
}
`)
