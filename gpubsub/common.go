package gpubsub

import (
	"context"
	"reflect"

	"cloud.google.com/go/pubsub"
)

type PubsubClient interface {
	Topic(id string) *pubsub.Topic
	CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error)
	Subscription(id string) *pubsub.Subscription
}

type Topic interface {
	Publish(ctx context.Context, msg *pubsub.Message) *pubsub.PublishResult
}

func isNil(v any) bool {
	return v == nil || (reflect.ValueOf(v).Kind() == reflect.Ptr && reflect.ValueOf(v).IsNil())
}
