package gfirestore

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
)

var log *logger.Log

func init() {
	log = logger.New()
}

// The Firestore Loader supports arbitrary ingestion of data into Firestore in Datastore mode.
// It currently does not support auto generated entity IDs, but require entity name syntax
// to be provided in the GEIST spec.
type loader struct {
	client           FirestoreClient
	defaultNamespace string
	spec             *entity.Spec
	id               string
}

func newLoader(
	spec *entity.Spec,
	id string,
	client FirestoreClient,
	defaultNamespace string) (*loader, error) {

	if isNil(client) {
		return nil, errors.New("client cannot be nil")
	}

	var g = loader{
		spec:             spec,
		id:               id,
		client:           client,
		defaultNamespace: defaultNamespace,
	}

	return &g, nil
}

// The GEIST Firestore loader implementation currently only supports a single input Transformed object,
// TODO: Check if eventSplit transforms causing multiple Transformed, should be supported as input
// --> Probably no
// TODO: Remove Kind as array and keep single
func (l *loader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {

	var (
		err        error
		retryable  = true
		resourceId string
	)

	if data == nil {
		return resourceId, errors.New("streamLoad called without data to load (data == nil)"), false
	}
	if data[0] == nil {
		return resourceId, errors.New("streamLoad called without data to load (data[0] == nil)"), false
	}

	for i, kind := range l.spec.Sink.Config.Kinds {
		resourceId, err, retryable = l.put(ctx, kind, data[0])
		if err != nil {
			return resourceId, fmt.Errorf("error inserting data, error: %v", err), retryable
		}
		if i > 0 {
			// TODO: Remove this whole loop when removing possibility for multiple Kinds as input
			return resourceId, fmt.Errorf("multiple kinds not supported, error in spec, kind: %#v", kind), retryable
		}
	}
	return resourceId, nil, false
}

func (l *loader) Shutdown() {}

func (l *loader) put(ctx context.Context, kind entity.Kind, t *entity.Transformed) (string, error, bool) {

	var err error

	namespace := l.defaultNamespace
	if len(kind.Namespace) > 0 {
		namespace = kind.Namespace
	}
	entityName := l.getEntityName(kind, t)
	if len(entityName) == 0 {
		return entityName, fmt.Errorf("could not find entity name for kind: %+v, data: %+v", kind, *t), false
	}
	key := datastore.NameKey(kind.Name, entityName, nil)
	key.Namespace = namespace

	// Retrieve all properties for this Entity, where the property IDs come from the Spec
	// and the property values come from the Transformed event data.
	// TODO: Check how to handle different wanted property value types (int, string, byte, etc)
	// Currently, the types are used as they come from Transformer, which are based on the 'type'
	// field in the Transform spec (default string, but can be int, []byte, etc)
	var props datastore.PropertyList
	for _, prop := range kind.Properties {
		if t.Data[prop.Id] != nil {
			props = append(props, datastore.Property{Name: prop.Name, Value: t.Data[prop.Id], NoIndex: !prop.Index})
		}
	}
	if len(props) == 0 {
		return entityName, fmt.Errorf(l.lgprfx()+"trying to store an empty transformed event, probably a spec error, event: %s", t), false
	}

	if l.spec.Ops.LogEventData {
		log.Infof(l.lgprfx()+"Loading transformed event: %s into Firestore as props: %#v", t, props)
	}

	var outKey *datastore.Key
	outKey, err = l.client.Put(ctx, key, &props)
	if err != nil {
		return entityName, fmt.Errorf("could not insert to firestore, err: %v, key: %#v, value: %#v", err, key, props), true
	}
	if key != nil && outKey != nil {
		if *key != *outKey {
			log.Warnf(l.lgprfx()+"incomplete keys not yet supported, check if bug, key: %+v, outKey: %+v", key, outKey)
		}
	}

	return entityName, err, true
}

func (l *loader) getEntityName(kind entity.Kind, t *entity.Transformed) string {

	var entityName string

	if len(kind.EntityName) > 0 {
		return kind.EntityName
	}

	if len(kind.EntityNameFromIds.Ids) > 0 {
		var delimiter string
		for n, field := range kind.EntityNameFromIds.Ids {
			if n == 1 {
				delimiter = kind.EntityNameFromIds.Delimiter
			}
			if value, ok := t.Data[field]; ok {
				entityName = entityName + delimiter + value.(string)
			}
		}
	}
	return entityName
}

func (l *loader) lgprfx() string {
	return "[xfirestore.loader:" + l.id + "] "
}
