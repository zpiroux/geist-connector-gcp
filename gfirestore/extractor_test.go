package gfirestore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

// This test uses GEIST "Spec" events for testing
func TestExtractor_ExtractFromSink(t *testing.T) {

	printTestOutput = false

	g := NewGeistTestSpecLoader(t)

	// Load two events of "Spec" type
	g.LoadEventIntoSink(t, testSpecDir+"pubsubsrc-kafkasink-foologs.json")
	assert.Equal(t, 1, g.Client.numberOfEntities())

	g.LoadEventIntoSink(t, testSpecDir+"kafkasrc-bigtablesink-user.json")
	assert.Equal(t, 2, g.Client.numberOfEntities())

	// Create Extractor to extract and validate the stored events from the Sink
	extractor, err := newExtractor(g.Spec, "mockId", g.Client, g.Loader.defaultNamespace)
	assert.NoError(t, err)
	assert.NotNil(t, extractor)

	// Test Get All
	query := entity.ExtractorQuery{
		Type: entity.QueryTypeAll,
	}

	var result []*entity.Transformed

	err, retryable := extractor.ExtractFromSink(context.Background(), query, &result)
	assert.NoError(t, err)
	assert.False(t, retryable)

	tPrintf("Result from ExtractFromSink (All):\n")
	err = printTransformed(result)
	assert.NoError(t, err)

	// Test KeyValue
	var keyValueResult []*entity.Transformed
	query = entity.ExtractorQuery{
		Type: entity.QueryTypeKeyValue,
		Key:  "foons-user",
	}

	err, retryable = extractor.ExtractFromSink(context.Background(), query, &keyValueResult)
	assert.NoError(t, err)
	assert.False(t, retryable)

	tPrintf("Result from ExtractFromSink (KeyValue):\n")
	err = printTransformed(keyValueResult)
	assert.NoError(t, err)

	// Testing filters
	query = entity.ExtractorQuery{
		Type:         entity.QueryTypeCompositeKeyValue,
		CompositeKey: []entity.KeyValueFilter{{Key: "foo", Value: "bar"}},
	}

	err, retryable = extractor.ExtractFromSink(context.Background(), query, &keyValueResult)
	assert.NoError(t, err)
	assert.False(t, retryable)

	tPrintf("Result from ExtractFromSink (KeyValue):\n")
	err = printTransformed(keyValueResult)
	assert.NoError(t, err)

}

func TestQueryCreation(t *testing.T) {

	geistQuery := entity.ExtractorQuery{
		Type:         entity.QueryTypeCompositeKeyValue,
		CompositeKey: []entity.KeyValueFilter{{Key: "foo", Value: "bar"}},
	}

	firestoreExtractorQuery := Query{
		Type:         CompositeKeyValue,
		Namespace:    "geisttest",
		Kind:         "GameRoundDetails",
		CompositeKey: geistQuery.CompositeKey,
	}

	dsq := createDatastoreQuery(firestoreExtractorQuery)
	validationString := fmt.Sprintf("%+v", dsq)
	assert.Equal(t, 243, len(validationString))
	fmt.Printf("len: %d, dsq: %s", len(validationString), validationString)
}

func printTransformed(transformed []*entity.Transformed) error {

	for i, trans := range transformed {
		tPrintf("Printing Transformed nb %d\n", i)
		tPrintf("%s\n", trans.String())
	}
	return nil
}
