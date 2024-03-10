package gbigtable

import (
	"testing"

	_assert "github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

func TestNewSinkConfig(t *testing.T) {
	assert = _assert.New(t)

	specData := loadSpecFromFile(t, testSpecDir+"apisrc-bigtablesink-minimal.json")
	spec, err := entity.NewSpec(specData)
	assert.NoError(err)

	sinkConfig, err := NewSinkConfig(spec)
	assert.NoError(err)
	assert.Equal(sinkConfig.Tables[0].Name, "geisttest_apitobigtable")
}
