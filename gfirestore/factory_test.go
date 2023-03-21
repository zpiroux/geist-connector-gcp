package gfirestore

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
	fsConfig := Config{}

	lf := NewMockLoaderFactory(ctx, fsConfig)
	err := geistConfig.RegisterLoaderType(lf)
	assert.NoError(t, err)

	geist, err := geist.New(ctx, geistConfig)
	assert.NoError(t, err)

	assert.True(t, geist.Entities()["loader"]["firestore"])
	assert.False(t, geist.Entities()["extractor"]["firestore"])
	assert.False(t, geist.Entities()["loader"]["some_other_sink"])

	go func() {
		streamId, err := geist.RegisterStream(ctx, spec)
		assert.NoError(t, err)
		assert.Equal(t, "my-tiny-stream", streamId)
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
		client: &MockClient{},
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
	return newExtractor(c.Spec, c.ID, mlf.realLoaderFactory.client, c.Spec.Namespace)
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
        "type": "firestore",
        "config": {
            "kinds": [
                {
                    "name": "Stuff",
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
}`)
