package gfirestore

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/zpiroux/geist/entity"
)

type extractor struct {
	id               string
	spec             *entity.Spec
	client           FirestoreClient
	defaultNamespace string
}

type QueryType int

const (
	Unknown QueryType = iota
	KeyValue
	CompositeKeyValue
	All
)

// CompositeKey works as a SQL 'WHERE key1 = value1 and key2 = value2 ...' for all props in a stream
// spec with index=true. Due to the simple nature of GEIST GET /streams/.../events?... API,
// only string values are supported. A more full-fledged query API should be provided by a separate
// query service, so no need to support complex queries in GEIST.
type Query struct {
	Type         QueryType
	Namespace    string
	Kind         string
	EntityName   string
	CompositeKey []entity.KeyValueFilter
}

func newExtractor(
	spec *entity.Spec,
	id string,
	client FirestoreClient,
	defaultNamespace string) (*extractor, error) {

	if isNil(client) {
		return nil, errors.New("client cannot be nil")
	}

	var e = extractor{
		spec:             spec,
		id:               id,
		client:           client,
		defaultNamespace: defaultNamespace,
	}

	return &e, nil
}

func (e *extractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	*err = errors.New("not applicable")
}

// Currently only supporting Sink specs with a single Datastore "table" (kind)
func (e *extractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {

	if len(e.spec.Sink.Config.Kinds) == 0 {
		return errors.New("need at least one Kind specified in Sink config"), false
	}

	return e.extractFromSink(ctx, e.convertQueryToNative(query), result)
}

func (e *extractor) convertQueryToNative(query entity.ExtractorQuery) Query {

	var nativeQuery Query

	switch query.Type {
	case entity.QueryTypeAll:
		nativeQuery.Type = All
		nativeQuery.Namespace = e.spec.Namespace
		nativeQuery.Kind = e.spec.Sink.Config.Kinds[0].Name

	case entity.QueryTypeKeyValue:
		nativeQuery.Type = KeyValue
		nativeQuery.Namespace = e.spec.Namespace
		nativeQuery.Kind = e.spec.Sink.Config.Kinds[0].Name
		nativeQuery.EntityName = query.Key

	case entity.QueryTypeCompositeKeyValue:
		nativeQuery.Type = CompositeKeyValue
		nativeQuery.Namespace = e.spec.Namespace
		nativeQuery.Kind = e.spec.Sink.Config.Kinds[0].Name
		nativeQuery.CompositeKey = query.CompositeKey
	}

	return nativeQuery
}

// Currently only supporting Sink specs with a single Datastore "table" (kind)
// TODO: check if result should be changed from the current recreated transformed or as was needed
// in BigTable extractFromSink where the actual table format was returned.
func (e *extractor) extractFromSink(ctx context.Context, query Query, result *[]*entity.Transformed) (error, bool) {

	log.Debugf(e.lgprfx()+"extractFromSink with query '%+v'", query)
	switch query.Type {
	case KeyValue:
		var props datastore.PropertyList
		key := datastore.NameKey(query.Kind, query.EntityName, nil)
		key.Namespace = query.Namespace
		if err := e.client.Get(ctx, key, &props); err != nil {
			return err, true
		}
		transformed, err, retryable := e.recreateTransformed(&props)
		if err != nil {
			return err, retryable
		}
		*result = append(*result, transformed)
		return nil, false

	case All:
		return e.getAll(ctx, createDatastoreQuery(query), result)

	case CompositeKeyValue:
		return e.getAll(ctx, createDatastoreQuery(query), result)
	}

	return errors.New("not yet supported"), false
}

func createDatastoreQuery(query Query) *datastore.Query {
	dsq := datastore.NewQuery(query.Kind).Namespace(query.Namespace)

	if query.Type == CompositeKeyValue {
		for _, filter := range query.CompositeKey {
			dsq = dsq.Filter(filter.Key+" =", filter.Value)
		}
	}
	return dsq
}

func (e *extractor) getAll(ctx context.Context, query *datastore.Query, result *[]*entity.Transformed) (error, bool) {
	var propLists []datastore.PropertyList
	log.Infof(e.lgprfx()+"GetAll query with query: %+v", *query)
	_, err := e.client.GetAll(ctx, query, &propLists)
	if err != nil {
		return err, false
	}

	for _, props := range propLists {
		transformed, err, retryable := e.recreateTransformed(&props)
		if err != nil {
			return err, retryable
		}
		*result = append(*result, transformed)
	}
	return nil, false
}

func (e *extractor) recreateTransformed(props *datastore.PropertyList) (*entity.Transformed, error, bool) {
	transformed := entity.NewTransformed()
	for _, prop := range *props {
		id, err := e.getSpecIdFromPropName(prop.Name)
		if err != nil {
			return nil, err, false
		}
		transformed.Data[id] = prop.Value
	}
	return transformed, nil, true
}

func (e *extractor) getSpecIdFromPropName(name string) (string, error) {

	// The properties are stored with Name as defined in the Sink part of the Spec.
	// Only supporting one firestore "table" for now per spec (Kinds[0]).
	for _, prop := range e.spec.Sink.Config.Kinds[0].Properties {

		if prop.Name == name {
			return prop.Id, nil
		}
	}

	return "", fmt.Errorf("specId not found for property name: %s", name)
}

// Currently, this function requires the caller to know how to handle Datastore specific storage structures.
// Instead, for generic repository usage, hiding away Datastore specifics, use ExtractFromSink().
// TODO: Add support for q := datastore.NewQuery("Entity").Filter("A =", 12).Limit(1) (added partly in ExtractFromSink())
func (e *extractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {

	q := e.convertQueryToNative(query)

	switch q.Type {
	case All:
		dsq := datastore.NewQuery(q.Kind)
		dsq = dsq.Namespace(q.Namespace)
		if _, err := e.client.GetAll(ctx, dsq, result); err != nil {
			return err, false
		}
		return nil, false
	}

	return errors.New("not yet supported"), false
}

func (e *extractor) SendToSource(ctx context.Context, eventData any) (string, error) {
	log.Error(e.lgprfx() + "not applicable")
	return "", nil
}

func (e *extractor) lgprfx() string {
	return "[xfirestore.extractor:" + e.id + "] "
}
