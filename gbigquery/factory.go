package gbigquery

import (
	"context"
	"errors"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/zpiroux/geist/entity"
)

const sinkTypeId = "bigquery"

type Config struct {

	// ProjectId (required) specifies GCP project ID for this deployment.
	ProjectId string
}

// bigQueryMetadataMutex reduces the amount of unneeded requests for certain stream setup operations.
// If a stream is configured to operate with more than one concurrent instance (ops.streamsPerPod > 1),
// certain operations might be attempted by more than one of its stream entity instances (e.g. a stream's
// BQ loaders creating tables if requested in its spec).
// The mutex scope is per pod, but this is good enough in this case.
var bigQueryMetadataMutex sync.Mutex

type loaderFactory struct {
	config Config
	client *bigquery.Client
}

func NewLoaderFactory(ctx context.Context, config Config) (entity.LoaderFactory, error) {
	var err error
	lf := &loaderFactory{
		config: config,
	}

	if config.ProjectId == "" {
		return nil, errors.New("no project id set")
	}

	if lf.client, err = bigquery.NewClient(ctx, config.ProjectId); err != nil {
		return nil, err
	}
	return lf, nil
}

func (lf *loaderFactory) SinkId() string {
	return sinkTypeId
}

func (lf *loaderFactory) NewLoader(ctx context.Context, c entity.Config) (entity.Loader, error) {
	return newLoader(ctx, c.Spec, c.ID, NewBigQueryClient(c.ID, lf.client), &bigQueryMetadataMutex)
}

func (lf *loaderFactory) NewSinkExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return nil, nil
}

func (lf *loaderFactory) Close(ctx context.Context) error {
	if lf.client != nil {
		if err := lf.client.Close(); err != nil {
			return err
		}
	}
	return nil
}
