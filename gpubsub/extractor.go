package gpubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
	"google.golang.org/api/googleapi"
)

const (
	SubTypeShared = "shared"
	SubTypeUnique = "unique"

	ALREADY_EXISTS = 409 // Defined here due to lack of proper other place in GCP libs
)

// Can't use normal ISO format for sub IDs. Using dots instead of colons.
const timestampLayoutMicros = "2006-01-02T15.04.05.000000Z"

var log *logger.Log

func init() {
	log = logger.New()
}

type extractor struct {
	config     *extractorConfig
	topic      Topic
	sub        Subscription
	ack        MsgAckFunc
	nack       MsgAckFunc
	id         string
	eventCount uint64
}

// The pubsub Extractor expects the pubsub topic to extract from, to already exist
func newExtractor(ctx context.Context, config *extractorConfig, id string) (*extractor, error) {

	var (
		err     error
		subName string
	)

	err = config.validate()
	if err != nil {
		return nil, err
	}

	extractor := &extractor{
		config: config,
		id:     id,
	}

	switch config.sub.Type {
	case SubTypeShared:
		subName = config.sub.Name
	case SubTypeUnique:
		subName = "geist-" + id + "-" + time.Now().UTC().Format(timestampLayoutMicros)
	default:
		return extractor, fmt.Errorf("pubsub subscription type %s not supported", config.sub.Type)
	}

	topic := config.client.Topic(config.topics[0]) // currently only supporting single topic in pubsub
	extractor.sub, err = createSubscription(ctx, config, config.sub.Type, subName, topic)
	if err != nil {
		return nil, err
	}

	receiveSettings := pubsub.ReceiveSettings{
		Synchronous:            config.rs.Synchronous,
		MaxOutstandingMessages: config.rs.MaxOutstandingMessages,
		MaxOutstandingBytes:    config.rs.MaxOutstandingBytes,
		NumGoroutines:          config.rs.NumGoroutines,
	}

	switch extractor.sub.(type) {
	case *pubsub.Subscription:
		extractor.sub.(*pubsub.Subscription).ReceiveSettings = receiveSettings
	}

	extractor.ack = extractor.ackMsg
	extractor.nack = extractor.nackMsg

	log.Infof(extractor.lgprfx()+"Pubsub Extractor created, input spec: %+v, topic: %s, subscription: %s (%+v)", config.spec, topic.String(), extractor.sub.String(), extractor.sub)

	extractor.topic = topic
	return extractor, nil
}

func createSubscription(ctx context.Context, config *extractorConfig, subType string, subName string, topic *pubsub.Topic) (*pubsub.Subscription, error) {
	// TODO: Add config and default values for sub expiration
	sub, err := config.client.CreateSubscription(
		ctx,
		subName,
		pubsub.SubscriptionConfig{Topic: topic})

	if err != nil {
		// These if/elses are caused by the not so user friendly error handling design in GCP Pubsub Go lib.
		if subType == SubTypeShared {
			if e, ok := err.(*googleapi.Error); ok {
				if e.Code == ALREADY_EXISTS {
					sub = config.client.Subscription(subName)
				}
			} else if strings.Contains(err.Error(), "AlreadyExists") {
				sub = config.client.Subscription(subName)
			} else {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return sub, nil
}

func (e *extractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	var errPubsub error

	if e.config.sub.Type == SubTypeUnique {
		defer func() {
			ctxSubDelete := context.Background() // Need fresh ctx here to avoid ctx canceled error
			err := e.sub.Delete(ctxSubDelete)
			log.Infof(e.lgprfx()+"unique sub %s deleted, err: %v", e.sub.String(), err)
		}()
	}

	switch e.sub.(type) {
	case *pubsub.Subscription:
		log.Infof(e.lgprfx()+"starting up pubsub Receive() with settings: %+v", e.sub.(*pubsub.Subscription).ReceiveSettings)
	default:
		log.Infof(e.lgprfx()+"starting up pubsub Receive() with settings: %+v (sub.type: %T)", e.sub, e.sub)
	}

	// All events from pubsub's Receive goroutines (for this Extractor's Receive() func) will be funneled through
	// this channel and processed by a single goroutine per extractor.
	// This is needed to ensure proper per-message delivery acknowledgment in GEIST sink loaders (if increasing
	// pubsub default Receive goroutines to more than one.
	// For example, using Kafka Sink/Loader, although thread-safe, if having multiple goroutines publish messages
	// via that single loader/Kafka producer, it will not be possible to determine if a specific message was
	// successful or not, since the async delivery report from Kafka client could end up in any of the calling goroutines.
	//
	// Full performance scalability is still ensured via Stream Spec config "ops.streamsPerPod", where for example setting
	// this to 4 will create 4 streams in each pod, each with its own pubsub extractor. With a shared subscription the
	// messages will be distributed among the 4 streams in a competing consumer pattern.
	msgChan := make(chan *pubsub.Message)
	defer close(msgChan)
	psReceiveCtx, cancel := context.WithCancel(ctx)
	go e.propagateEvents(ctx, reportEvent, msgChan, cancel, err, retryable)

	for {
		errPubsub = e.sub.Receive(psReceiveCtx, func(ctx context.Context, msg *pubsub.Message) {
			msgChan <- msg
		})

		// Sometimes PubSub gives deadline exceeded error, for example due to internal pubsub service
		// or network error. If so, the best way to proceed is to just re-initiate the receive operation.
		if errPubsub != nil && ctx.Err() != context.Canceled {
			if errPubsub.Error() == context.DeadlineExceeded.Error() {
				log.Warnf(e.lgprfx()+"sub.Receive() terminated, err: '%s', ctx.Err: '%s')"+
					" Re-initiating operation.", errPubsub, ctx.Err())
				continue
			}
		}
		break
	}

	exitStr := "Pubsub subscriber terminated"
	if ctx.Err() == context.Canceled {
		log.Warnf(e.lgprfx()+"%s (context.Canceled, err: '%s'). "+
			"Probable reason: graceful shutdown due to container rolling upgrade.", exitStr, errPubsub)
	} else {
		if errPubsub == nil {
			log.Warnf(e.lgprfx()+"%s (no error). Reason unknown. ctx.Err: '%v'", exitStr, ctx.Err())
		} else {
			log.Errorf(e.lgprfx()+"%s,  Error: '%s', ctx.Err: '%v'", exitStr, errPubsub, ctx.Err())
		}
	}
	log.Infof(e.lgprfx()+"Total number of events received: %d", atomic.LoadUint64(&e.eventCount))

	if errPubsub != nil {
		*err = errPubsub
	}
}

func (e *extractor) propagateEvents(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	msgChan chan *pubsub.Message,
	cancel context.CancelFunc,
	err *error,
	retryable *bool) {

	shutdownInProgress := false
	for msg := range msgChan {

		if shutdownInProgress {
			e.nack(msg)
			continue
		}

		// No support for microbatching in pubsub extractor for now
		events := []entity.Event{{
			Key:  []byte(msg.ID),
			Ts:   msg.PublishTime,
			Data: msg.Data,
		}}

		// Send event back to Executor for further downstream processing
		result := reportEvent(ctx, events)

		*err = result.Error
		*retryable = result.Retryable

		switch e.handleEventProcessingResult(ctx, msg, result, err, retryable) {

		case actionShutdown:
			log.Infof(e.lgprfx()+"shutting down extractor, reportEvent result: %+v", result)
			shutdownInProgress = true
			cancel()
			e.nack(msg)
		case actionContinue:
			e.ack(msg)
			atomic.AddUint64(&e.eventCount, 1)
		}
	}
}

func (e *extractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return errors.New("not applicable"), false
}

func (e *extractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {
	return errors.New("not applicable"), false

}

func (e *extractor) SendToSource(ctx context.Context, eventData any) (string, error) {

	var msgData []byte

	switch eventData := eventData.(type) {
	case string:
		msgData = []byte(eventData)
	case []byte:
		msgData = eventData
	default:
		return "", fmt.Errorf("invalid type for eventData (%T), only string and []byte allowed", eventData)
	}

	result := e.topic.Publish(ctx, &pubsub.Message{Data: msgData})

	// The Get method blocks until a server-generated ID or
	// an error is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		log.Errorf(e.lgprfx()+"failed to publish: %v", err)
		return "", err
	}
	log.Infof(e.lgprfx()+"Published message with ID: %v", id)

	return id, err
}

type action int

const (
	actionContinue action = iota
	actionShutdown
)

func (e *extractor) handleEventProcessingResult(
	ctx context.Context,
	msg *pubsub.Message,
	result entity.EventProcessingResult,
	err *error,
	retryable *bool) action {

	switch result.Status {

	case entity.ExecutorStatusSuccessful:
		if result.Error != nil {
			log.Errorf(e.lgprfx()+"bug in executor, shutting down, result.Error should be nil if ExecutorStatusSuccessful, result: %+v", result)
			return actionShutdown
		}
		return actionContinue

	case entity.ExecutorStatusShutdown:
		log.Warnf(e.lgprfx()+"shutting down extractor due to executor shutdown, reportEvent result: %+v", result)
		return actionShutdown

	case entity.ExecutorStatusRetriesExhausted:
		*err = fmt.Errorf(e.lgprfx()+"executor failed all retries, shutting down extractor, handing over to executor, reportEvent result: %+v", result)
		return actionShutdown

	case entity.ExecutorStatusError:
		*retryable = false
		if result.Retryable {
			*err = fmt.Errorf(e.lgprfx() + "bug, executor should handle all retryable errors, until retries exhausted, shutting down extractor")
			return actionShutdown
		}
		str := fmt.Sprintf(e.lgprfx()+"executor had an unretryable error with this event: %+v, payload: '%s', "+
			"reportEvent result: %+v", msg, string(msg.Data), result)
		log.Warn(str)

		switch e.config.spec.Ops.HandlingOfUnretryableEvents {

		case entity.HoueDefault:
			fallthrough
		case entity.HoueDiscard:
			log.Warnf(e.lgprfx()+"a pubsub event failed downstream processing with result %+v; "+
				" since this stream (%s) does not have DLQ enabled, the event will now be discarded, event ID: %s",
				result, e.config.spec.Id(), msg.ID)
			return actionContinue

		case entity.HoueDlq:
			log.Warnf(e.lgprfx()+"DLQ enabled in stream spec (%s) even though PubSub DLQ not yet implemented, "+
				"event ID: %s", e.config.spec.Id(), msg.ID)
			return actionContinue
			//return e.moveEventToDLQ(ctx, msg) // in future update

		case entity.HoueFail:
			str += " - since this stream's houe mode is set to HoueFail, the stream will now be shut down, requiring manual/external restart"
			*err = errors.New(str)
			return actionShutdown
		}
	}
	*err = fmt.Errorf("encountered a 'should not happen' error in Extractor.handleEventProcessingResult, "+
		"shutting down stream, reportEvent result %+v, event: %s, spec: %v", result, string(msg.Data), e.config.spec)
	*retryable = false
	return actionShutdown
}

func (g *extractor) SetSub(sub Subscription) {
	g.sub = sub
}

func (g *extractor) SetTopic(topic Topic) {
	g.topic = topic
}

type MsgAckFunc func(*pubsub.Message)

func (g *extractor) SetMsgAckNackFunc(ack MsgAckFunc, nack MsgAckFunc) {
	g.ack = ack
	g.nack = nack
}

func (g *extractor) ackMsg(m *pubsub.Message) {
	m.Ack()
}

func (g *extractor) nackMsg(m *pubsub.Message) {
	m.Nack()
}

func (g *extractor) lgprfx() string {
	return "[xpubsub.extractor:" + g.id + "] "
}

type Subscription interface {
	Receive(ctx context.Context, f func(context.Context, *pubsub.Message)) error
	String() string
	Delete(ctx context.Context) error
}

type SubConfigurator interface {
	Update(sub Subscription, rs pubsub.ReceiveSettings)
}

type DefaultSubConfigurator struct{}
