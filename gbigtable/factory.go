package gbigtable

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/zpiroux/geist/entity"
)

const sinkTypeId = "bigtable"

type Config struct {

	// ProjectId (required) specifies GCP project ID for this deployment.
	ProjectId string

	// InstanceId (required) specifies BigTable instance ID
	InstanceId string
}

type loaderFactory struct {
	config      Config
	client      BigTableClient
	adminClient BigTableAdminClient
}

func NewLoaderFactory(ctx context.Context, config Config) (entity.LoaderFactory, error) {
	var err error
	lf := &loaderFactory{
		config: config,
	}
	if config.ProjectId == "" {
		return nil, errors.New("no project id set")
	}
	if config.InstanceId == "" {
		return nil, errors.New("no BigTable instance id set")
	}
	if lf.client, err = bigtable.NewClient(ctx, config.ProjectId, config.InstanceId); err != nil {
		return nil, err
	}
	if lf.adminClient, err = bigtable.NewAdminClient(ctx, config.ProjectId, config.InstanceId); err != nil {
		return nil, err
	}
	return lf, nil
}

func (lf *loaderFactory) SinkId() string {
	return sinkTypeId
}

func (lf *loaderFactory) NewLoader(ctx context.Context, c entity.Config) (entity.Loader, error) {
	return newLoader(ctx, c.Spec, c.ID, lf.client, lf.adminClient)
}

func (lf *loaderFactory) NewSinkExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {
	return newExtractor(c.Spec, c.ID, lf.client, lf.adminClient)
}

func (lf *loaderFactory) Close(ctx context.Context) error {
	errorStr := ""
	if lf.client != nil {
		if err := lf.client.Close(); err != nil {
			errorStr = err.Error()
		}
	}
	if lf.adminClient != nil {
		if err := lf.adminClient.Close(); err != nil {
			errorStr += err.Error()
		}
	}
	if errorStr != "" {
		return fmt.Errorf("error(s) closing BigTable client(s): %s", errorStr)
	}
	return nil
}
