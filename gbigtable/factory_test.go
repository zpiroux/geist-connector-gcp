package gbigtable

import (
	"context"
	"testing"

	asrt "github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist"
	"github.com/zpiroux/geist/entity"
)

func TestGeistIntegration(t *testing.T) {
	ctx := context.Background()
	geistConfig := geist.NewConfig()
	btConfig := Config{}

	lf := NewMockLoaderFactory(ctx, btConfig)
	err := geistConfig.RegisterLoaderType(lf)
	asrt.NoError(t, err)

	geist, err := geist.New(ctx, geistConfig)
	asrt.NoError(t, err)

	asrt.True(t, geist.Entities()["loader"]["bigtable"])
	asrt.False(t, geist.Entities()["extractor"]["bigtable"])
	asrt.False(t, geist.Entities()["loader"]["some_other_sink"])

	go func() {
		streamId, err := geist.RegisterStream(ctx, spec)
		asrt.NoError(t, err)
		asrt.Equal(t, "my-tiny-stream", streamId)
		err = geist.Shutdown(ctx)
		asrt.NoError(t, err)
	}()

	err = geist.Run(ctx)
	asrt.NoError(t, err)
}

type MockLoaderFactory struct {
	realLoaderFactory *loaderFactory
}

func NewMockLoaderFactory(ctx context.Context, config Config) entity.LoaderFactory {
	var mlf MockLoaderFactory
	mlf.realLoaderFactory = &loaderFactory{
		config:      config,
		client:      &MockClient{},
		adminClient: &MockAdminClient{},
	}
	return &mlf
}

func (mlf *MockLoaderFactory) SinkId() string {
	return mlf.realLoaderFactory.SinkId()
}

func (mlf *MockLoaderFactory) NewLoader(ctx context.Context, c entity.Config) (entity.Loader, error) {
	realLoader, err := mlf.realLoaderFactory.NewLoader(ctx, c)
	re := realLoader.(*loader)
	return re, err
}

func (mlf *MockLoaderFactory) NewSinkExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return newExtractor(c.Spec, c.ID, mlf.realLoaderFactory.client, mlf.realLoaderFactory.adminClient)
}

func (mlf *MockLoaderFactory) Close(ctx context.Context) error {
	return nil
}

var spec = []byte(`
{
    "namespace": "my",
    "streamIdSuffix": "tiny-stream",
    "description": "Tiny test stream storing raw data from Geist API in Firestore.",
    "version": 1,
    "source": {
        "type": "geistapi"
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
        "type": "bigtable",
        "config": {
           "tables": [
              {
                 "name": "geisttest_apitobigtable",
                 "rowKey": {
                    "predefined": "timestampIso"
                 },
                 "columnFamilies": [
                    {
                       "name": "d",
                       "garbageCollectionPolicy": {
                          "type": "maxVersions",
                          "value": 2
                       },
                       "columnQualifiers": [
                          {
                             "id": "rawEvent",
                             "name": "event"
                          }
                       ]
                    }
                 ]
              }
           ]
        }
    }
}`)
