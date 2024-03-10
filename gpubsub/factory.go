package gpubsub

import (
	"context"
	"errors"

	"cloud.google.com/go/pubsub"
	"github.com/zpiroux/geist/entity"
)

const entityTypeId = "pubsub"

// PubsubConfig is the external config provided by the geist client to the factory when starting up,
// which is to be used during stream creations
//
// TODO: Check if non-nil default values should be added to MaxOutstandingXxx
type PubsubConfig struct {

	// ProjectId (required) specifies GCP project ID for this deployment.
	ProjectId string

	// Env is only required to be filled in if stream specs for this use of Geist are using different
	// topic specs for different environments, typically "dev", "stage", and "prod".
	// Any string is allowed as long as it matches the ones used in the stream specs.
	Env string

	// The following fields (optional) sets the default values, if not specified in the stream spec.
	// See entity.Spec for more info.
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
}

// ExtractorFactory is a singleton enabling extractors/sources to be handled as plug-ins to Geist
type extractorFactory struct {
	config PubsubConfig
	client PubsubClient
}

// NewExtractorFactory creates a Pubsub extractory factory.
func NewExtractorFactory(ctx context.Context, config PubsubConfig) (entity.ExtractorFactory, error) {
	if config.ProjectId == "" {
		return nil, errors.New("no project id set")
	}
	client, err := pubsub.NewClient(ctx, config.ProjectId)
	if err != nil {
		return nil, err
	}
	return &extractorFactory{
		config: config,
		client: client,
	}, nil
}

func (ef *extractorFactory) SourceId() string {
	return entityTypeId
}

func (ef *extractorFactory) NewExtractor(ctx context.Context, c entity.Config) (entity.Extractor, error) {

	extractorConfig, err := ef.createPubsubExtractorConfig(c.Spec)
	if err != nil {
		return nil, err
	}
	return newExtractor(ctx, extractorConfig, c.ID)
}

func (s *extractorFactory) createPubsubExtractorConfig(spec *entity.Spec) (*extractorConfig, error) {
	sourceConfig, err := NewSourceConfig(spec)
	if err != nil {
		return nil, err
	}
	return newExtractorConfig(
		s.client,
		spec,
		s.topicNamesFromSpec(sourceConfig.Topics),
		sourceConfig.Subscription,
		s.configureReceiveSettings(sourceConfig))
}

func (s *extractorFactory) configureReceiveSettings(c SourceConfig) receiveSettings {
	var rs receiveSettings
	if c.MaxOutstandingMessages == nil {
		rs.MaxOutstandingMessages = s.config.MaxOutstandingMessages
	} else {
		rs.MaxOutstandingMessages = *c.MaxOutstandingMessages
	}

	if c.MaxOutstandingBytes == nil {
		rs.MaxOutstandingBytes = s.config.MaxOutstandingBytes
	} else {
		rs.MaxOutstandingBytes = *c.MaxOutstandingBytes
	}

	if c.Synchronous == nil {
		rs.Synchronous = false // no need to have this as deployment config
	} else {
		rs.Synchronous = *c.Synchronous
	}

	if c.NumGoroutines == nil {
		rs.NumGoroutines = 1 // no need to have this as deployment config
	} else {
		rs.NumGoroutines = *c.NumGoroutines
	}
	return rs
}

func (s *extractorFactory) topicNamesFromSpec(topicsInSpec []Topics) []string {
	var topicNames []string
	for _, topics := range topicsInSpec {
		if topics.Env == string(entity.EnvironmentAll) {
			topicNames = topics.Names
			break
		}
		if string(topics.Env) == s.config.Env {
			topicNames = topics.Names
		}
	}
	return topicNames
}

func (lf *extractorFactory) Close(ctx context.Context) error {
	return nil
}
