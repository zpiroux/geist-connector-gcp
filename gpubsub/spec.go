package gpubsub

import (
	"encoding/json"

	"github.com/zpiroux/geist/entity"
)

// SourceConfig specifies the schema for the "customConfig" field in the "source" section
// of the stream spec. It enables arbitrary connector specific fields to be present in
// the stream spec.
type SourceConfig struct {

	// Topics and Subscription are required for extraction/consumption from PubSub
	Topics       []Topics            `json:"topics,omitempty"`
	Subscription *SubscriptionConfig `json:"subscription,omitempty"`

	// MaxOutstandingMessages is a PubSub consumer specific property, specifying max number of fetched but not yet
	// acknowledged messages in pubsub consumer. If this is omitted the value will be set to the loaded Pubsub entity
	// config default.
	// For time consuming transform/sink streams decrease this value while increasing ops.streamsPerPod
	MaxOutstandingMessages *int `json:"maxOutstandingMessages,omitempty"`

	// MaxOutstandingBytes is a PubSub consumer specific property, specifying max size of fetched but not yet
	// acknowledged messages.
	MaxOutstandingBytes *int `json:"maxOutstandingBytes,omitempty"`

	// Synchronous can be used to tune certain type of streams (e.g. spiky input flow of messages with very heavy
	// transforms or slow sinks), where setting this to true could reduce number of expired messages. It is optional
	// for a source connector to implement.
	// Default is false.
	Synchronous *bool `json:"synchronous,omitempty"`

	// NumGoroutines is a PubSub consumer specific property used for increasing rate of incoming messages in case
	// downstream ETL is not cpu starved or blocked on sink ops, while Extractor cannot keep up with consuming
	// incoming messages. Depending on type of Sink/Loader a better/alternative approach is to increase ops.streamsPerPod.
	// If omitted it is set to 1.
	NumGoroutines *int `json:"numGoroutines,omitempty"`
}

func NewSourceConfig(spec *entity.Spec) (sc SourceConfig, err error) {
	sinkConfigIn, err := json.Marshal(spec.Sink.Config.CustomConfig)
	if err != nil {
		return sc, err
	}
	err = json.Unmarshal(sinkConfigIn, &sc)
	return sc, err
}

type Topics struct {
	// Env specifies for which environment/stage the topic names config should be used.
	// Allowed values are "all" or any string matching the config provided to registered entity factories.
	// Examples: "dev", "staging", and "prod", etc.
	Env   string   `json:"env,omitempty"`
	Names []string `json:"names,omitempty"`
}

type SubscriptionConfig struct {
	// Type can be:
	//
	// 		"shared" - meaning multiple consumers share this subscription in a competing consumer pattern.
	//				   Only one of the subscribers will receive each event.
	//				   If this is set, the name of the subscription needs to be present in the "Name" field.
	//
	//		"unique" - meaning each transloading stream instance will have its own unique subscription.
	//				   All instances will thus get all events from the topic.
	//				   If this is set, a unique subscription name will be created and the Name field is
	//				   ignored. This one is used internally by each pod's Supervisor to receive notifications
	//                 about registry updates, from other Supervisors' registry instances.
	Type string `json:"type,omitempty"`

	// Name of subscription
	Name string `json:"name,omitempty"`
}
