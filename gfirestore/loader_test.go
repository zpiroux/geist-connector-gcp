package gfirestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/entity/transform"
)

const (
	testDirPath = "./test/"
	testSpecDir = testDirPath + "specs/"
)

var printTestOutput bool

func TestLoader(t *testing.T) {

	printTestOutput = false

	g := NewGeistTestSpecLoader(t)

	g.LoadEventIntoSink(t, testSpecDir+"pubsubsrc-kafkasink-foologs.json")
	assert.Equal(t, 1, g.Client.numberOfEntities())

	g.LoadEventIntoSink(t, testSpecDir+"kafkasrc-bigtablesink-user.json")
	assert.Equal(t, 2, g.Client.numberOfEntities())

	g.LoadEventIntoSink(t, testSpecDir+"apisrc-bigtablesink-fooround.json")
	assert.Equal(t, 3, g.Client.numberOfEntities())

	g.ValidateLoadedEventData(t)

}

type GeistTestSpecLoader struct {
	Spec        *entity.Spec
	Client      *MockClient
	Loader      *loader
	Transformer *transform.Transformer
}

func NewGeistTestSpecLoader(t *testing.T) *GeistTestSpecLoader {
	var (
		err error
		g   GeistTestSpecLoader
	)

	g.Client = &MockClient{}

	g.Spec, err = entity.NewSpec(loadSpecFromFile(t, testSpecDir+"pubsubsrc-firestoresink-regspec.json"))
	assert.NoError(t, err)

	g.Loader, err = newLoader(
		g.Spec,
		"mockId",
		g.Client,
		"coolDefaultNamespaceName")

	assert.NoError(t, err)
	assert.NotNil(t, g)
	log.Infof("Loader status: %#v", g)

	g.Transformer = transform.NewTransformer(g.Spec)

	return &g
}

func (g *GeistTestSpecLoader) LoadEventIntoSink(t *testing.T, eventInFile string) {
	var retryable bool
	fileBytes, err := os.ReadFile(eventInFile)
	assert.NoError(t, err)
	output, err := g.Transformer.Transform(context.Background(), fileBytes, &retryable)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	tPrintf("Transformation output, len: %d\n", len(output))

	_, err, retryable = g.Loader.StreamLoad(context.Background(), output)
	assert.NoError(t, err)
}

// Checks if the loaded spec JSONs can be retrieved and unmarshaled back into GEIST Spec Structs
func (g *GeistTestSpecLoader) ValidateLoadedEventData(t *testing.T) {

	var propLists []datastore.PropertyList

	keys, err := g.Client.GetAll(context.Background(), &datastore.Query{}, &propLists)
	assert.NoError(t, err)
	assert.NotNil(t, keys)

	tPrintf("Found the following loaded events (entities):\n")
	for i, propList := range propLists {

		printPropertyList(keys[i], &propList)

		// Get the specData value and unmarshal into Spec struct
		err := validateSpecWithModel(&propList)
		assert.NoError(t, err)
	}
}

func validateSpecWithModel(p *datastore.PropertyList) error {

	var spec entity.Spec
	for _, prop := range *p {
		pn := prop.Name
		if pn == "specData" {
			pv := prop.Value.(string) // Note that this test assumes spec data to be stored as string

			if err := json.Unmarshal([]byte(pv), &spec); err != nil {
				return err
			}
			tPrintf("property with name: '%s' with data unmarshalled into '%+v'\n", pn, spec)
		}
	}
	return nil
}

func loadSpecFromFile(t *testing.T, path string) []byte {
	fileBytes, err := os.ReadFile(path)
	assert.NoError(t, err)
	return fileBytes
}

type MockClient struct {
	Keys     []*datastore.Key
	Entities []*datastore.PropertyList
}

func (m *MockClient) Put(ctx context.Context, key *datastore.Key, src any) (*datastore.Key, error) {

	tPrintf("%s\n", "In MockClient.Put()")

	data := src.(*datastore.PropertyList)

	printPropertyList(key, data)

	m.Keys = append(m.Keys, key)
	m.Entities = append(m.Entities, data)

	return key, nil
}

func (m *MockClient) Get(ctx context.Context, key *datastore.Key, dst any) (err error) {

	if dst == nil {
		return datastore.ErrInvalidEntityType
	}
	result := dst.(*datastore.PropertyList)

	for i, storedKey := range m.Keys {
		if key.Name == storedKey.Name {
			*result = *m.Entities[i]
			return nil
		}
	}

	return errors.New("not found")
}

func (m *MockClient) GetAll(ctx context.Context, q *datastore.Query, dst any) (keys []*datastore.Key, err error) {

	dv := reflect.ValueOf(dst)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return nil, datastore.ErrInvalidEntityType
	}

	result := dst.(*[]datastore.PropertyList)

	for _, entity := range m.Entities {
		*result = append(*result, *entity)
	}
	return m.Keys, nil
}

func (m *MockClient) numberOfEntities() int {
	return len(m.Entities)
}

func printPropertyList(key *datastore.Key, p *datastore.PropertyList) {
	tPrintf("Entity with key: '%+v' has the following properties:\n", *key)
	for _, prop := range *p {
		pn := prop.Name
		pv := prop.Value.(string) // Note that this assumes the value to be of type string, which is not the case always
		tPrintf("property with name: '%s' and value '%s'\n", pn, pv)
	}
}

func tPrintf(format string, a ...any) {
	if printTestOutput {
		fmt.Printf(format, a...)
	}
}
