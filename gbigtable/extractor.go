package gbigtable

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/zpiroux/geist/entity"
)

var ErrNotApplicable = errors.New("not applicable")

type QueryType int

const (
	InvalidQueryType QueryType = iota
	KeyValue
	LatestN
	All
)

type Query struct {
	Type    QueryType
	Table   string
	RowKey  string
	LatestN int
}

type extractor struct {
	id           string
	spec         *entity.Spec
	sinkConfig   SinkConfig
	client       BigTableClient
	adminClient  BigTableAdminClient
	openedTables map[string]BigTableTable
}

func newExtractor(
	spec *entity.Spec,
	id string,
	client BigTableClient,
	adminClient BigTableAdminClient) (*extractor, error) {

	if isNil(client) || isNil(adminClient) {
		return nil, errors.New("invalid arguments, clients cannot be nil")
	}

	sinkConfig, err := NewSinkConfig(spec)
	if err != nil {
		return nil, err
	}

	var e = extractor{
		id:          id,
		spec:        spec,
		sinkConfig:  sinkConfig,
		client:      client,
		adminClient: adminClient,
	}

	e.openedTables = make(map[string]BigTableTable)

	return &e, nil
}

func (e *extractor) ExtractFromSink(ctx context.Context, query entity.ExtractorQuery, result *[]*entity.Transformed) (error, bool) {

	if len(e.sinkConfig.Tables) == 0 {
		return errors.New("need at least one Table specified in Sink config"), false
	}

	return e.extractFromSink(ctx, e.convertQueryToNative(query), result)
}

// For now this does not do much conversion but it's prepared for more complex
// queries with range filters, etc.
func (g *extractor) convertQueryToNative(query entity.ExtractorQuery) Query {

	var nativeQuery Query

	switch query.Type {
	case entity.QueryTypeAll:
		nativeQuery.Type = All

	case entity.QueryTypeKeyValue:
		nativeQuery.Type = KeyValue
		nativeQuery.RowKey = query.Key
		nativeQuery.LatestN = 1
	}

	return nativeQuery
}

func (e *extractor) extractFromSink(ctx context.Context, query Query, result *[]*entity.Transformed) (error, bool) {

	var (
		err       error
		retryable bool
	)

	// lazy init of open tables in case the BT loader is creating tables at GEIST startup
	if len(e.openedTables) == 0 {
		if err = e.openTables(ctx); err != nil {
			return err, true
		}
	}

	switch query.Type {
	case KeyValue:
		err, retryable = e.extractWithKeyValue(ctx, query, result)
	default:
		return fmt.Errorf("queryType '%v' not supported", query.Type), false
	}

	return err, retryable
}

func (e *extractor) extractWithKeyValue(ctx context.Context, query Query, result *[]*entity.Transformed) (error, bool) {
	var (
		row bigtable.Row
		err error
	)
	for _, table := range e.openedTables {
		row, err = table.ReadRow(ctx, query.RowKey, bigtable.RowFilter(bigtable.LatestNFilter(query.LatestN)))
		if err != nil {
			return err, true
		}
		if len(row) > 0 {
			break
		}
	}

	if len(row) == 0 {
		return errors.New("row with key " + query.RowKey + " not found"), false
	}
	// TODO: Keep below comment and make output type configurable - transform recreation or raw output
	// transformed, err, retryable := e.recreateTransformed(&row)
	transformed, err, retryable := e.createTransformedAsRow(query.RowKey, &row)
	if err == nil {
		*result = append(*result, transformed)
	}
	return err, retryable
}

func (e *extractor) StreamExtract(
	ctx context.Context,
	reportEvent entity.ProcessEventFunc,
	err *error,
	retryable *bool) {

	*err = ErrNotApplicable
}

func (e *extractor) SendToSource(ctx context.Context, eventData any) (string, error) {

	return "", ErrNotApplicable
}

func (e *extractor) Extract(ctx context.Context, query entity.ExtractorQuery, result any) (error, bool) {
	return ErrNotApplicable, false
}

func (e *extractor) openTables(ctx context.Context) error {

	// For now only Sink entity is available for BigTable extractor spec
	for _, table := range e.sinkConfig.Tables {

		t := e.client.Open(table.Name)
		if t == nil {
			return fmt.Errorf("could not open table %s", table.Name)
		}
		e.openedTables[table.Name] = t
	}
	return nil
}

// createTransformedAsRow flattens columnFamilies, currently assuming column qualifier names
// are unique across families.
func (e *extractor) createTransformedAsRow(rowKey string, row *bigtable.Row) (*entity.Transformed, error, bool) {

	transformed := entity.NewTransformed()
	var rowItems []*entity.RowItem

	for _, cols := range *row {
		for _, col := range cols {
			rowItem := &entity.RowItem{
				Column:    col.Column,
				Timestamp: col.Timestamp.Time(),
				Value:     string(col.Value),
				//TODO: change from converting to string to look at what was in the transform part of spec
			}
			rowItems = append(rowItems, rowItem)
		}
	}
	transformed.Data[entity.TransformedKeyKey] = rowKey
	transformed.Data[entity.TransformedValueKey] = rowItems
	return transformed, nil, true
}

func (e *extractor) setOpenTables(b map[string]BigTableTable) {
	e.openedTables = b
}
