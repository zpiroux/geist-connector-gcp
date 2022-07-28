package gfirestore

import (
	"context"
	"errors"

	"cloud.google.com/go/datastore"
	"github.com/zpiroux/geist/entity"
)

const sinkTypeId = "firestore"

// FirestoreConfig
type Config struct {

	// ProjectId (required) specifies GCP project ID for this deployment.
	ProjectId string
}

type loaderFactory struct {
	config Config
	client FirestoreClient
}

func NewLoaderFactory(ctx context.Context, config Config) (entity.LoaderFactory, error) {
	if config.ProjectId == "" {
		return nil, errors.New("no project id set")
	}
	client, err := datastore.NewClient(ctx, config.ProjectId)
	if err != nil {
		return nil, err
	}
	return &loaderFactory{
		config: config,
		client: client,
	}, nil
}

func (lf *loaderFactory) SinkId() string {
	return sinkTypeId
}

func (lf *loaderFactory) NewLoader(ctx context.Context, spec *entity.Spec, id string) (entity.Loader, error) {
	return newLoader(spec, id, lf.client, spec.Namespace)
}

func (lf *loaderFactory) NewSinkExtractor(ctx context.Context, spec *entity.Spec, id string) (entity.Extractor, error) {
	return newExtractor(spec, id, lf.client, spec.Namespace)
}

func (lf *loaderFactory) Close() error {
	return nil
}
