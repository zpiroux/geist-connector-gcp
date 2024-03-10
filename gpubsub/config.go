package gpubsub

import (
	"errors"

	"github.com/zpiroux/geist/entity"
)

var (
	ErrClienNotProvided      = errors.New("a client must be provided")
	ErrStreamSpecNotProvided = errors.New("the stream spec must be provided")
	ErrTopicNotProvided      = errors.New("a topic name is required")
	ErrSubNotProvided        = errors.New("a valid subscription must be provided")
)

// extractorConfig is the internal config used by each extractor, combining config
// from external Config with config from what is inside the stream spec a specific
// stream
type extractorConfig struct {
	client PubsubClient
	spec   *entity.Spec
	topics []string
	sub    *SubscriptionConfig
	rs     receiveSettings
}

func newExtractorConfig(
	client PubsubClient,
	spec *entity.Spec,
	topics []string,
	sub *SubscriptionConfig,
	rs receiveSettings,
) (*extractorConfig, error) {

	ec := &extractorConfig{
		client: client,
		spec:   spec,
		topics: topics,
		sub:    sub,
		rs:     rs,
	}
	return ec, ec.validate()
}

func (ec extractorConfig) validate() error {
	switch {
	case isNil(ec.client):
		return ErrClienNotProvided
	case ec.spec == nil:
		return ErrStreamSpecNotProvided
	case len(ec.topics) == 0:
		return ErrTopicNotProvided
	case ec.sub == nil:
		return ErrSubNotProvided
	default:
		return nil
	}
}

type receiveSettings struct {
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	Synchronous            bool
	NumGoroutines          int
}
