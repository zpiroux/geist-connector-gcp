package gbigtable

import (
	"context"
	"testing"

	asrt "github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

func TestExtractor_ExtractFromSink(t *testing.T) {

	applicableEvents := []string{testEventDir + "platform_change_event_ex1.json"}
	g := NewGeistTestSpecLoader(t, loadSpecFromFile(t, testSpecDir+"apisrc-bigtablesink-minimal.json"))
	g.LoadEventsIntoSink(t, applicableEvents, "")

	extractor, err := newExtractor(g.Spec, "some-id", g.Client, g.AdminClient)
	extractor.setOpenTables(m)
	asrt.NoError(t, err)
	asrt.NotNil(t, extractor)

	query := entity.ExtractorQuery{
		Type: entity.QueryTypeKeyValue,
		Key:  "foo",
	}

	var result []*entity.Transformed

	err, _ = extractor.ExtractFromSink(context.Background(), query, &result)
	retryable := false
	asrt.NoErrorf(t, err, "retryable: %v", &retryable)

	tPrintf("Result from ExtractFromSink:\n")
	err = printTransformed(result)
	asrt.NoError(t, err)

	var keyValueResult []*entity.Transformed
	query = entity.ExtractorQuery{
		Type: entity.QueryTypeKeyValue,
		Key:  "some_row_key",
	}

	err, _ = extractor.ExtractFromSink(context.Background(), query, &keyValueResult)

	// For now, this is on ok result, improve later
	asrt.NoErrorf(t, err, "retryable: %v", retryable)

	tPrintf("Result from ExtractFromSink (KeyValue):\n")
	err = printTransformed(keyValueResult)
	asrt.NoError(t, err)

}

func printTransformed(transformed []*entity.Transformed) error {

	for i, trans := range transformed {
		tPrintf("Printing Transformed nb %d\n", i)
		tPrintf("%s\n", trans.String())
	}
	return nil
}
