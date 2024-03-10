package gbigtable

import (
	"encoding/json"

	"github.com/zpiroux/geist/entity"
)

// SinkConfig specifies the schema for the "customConfig" field in the "sink" section
// of the stream spec. It enables arbitrary connector specific fields to be present in
// the stream spec.
type SinkConfig struct {
	Tables []Table `json:"tables,omitempty"`
}

func NewSinkConfig(spec *entity.Spec) (sc SinkConfig, err error) {
	sinkConfigIn, err := json.Marshal(spec.Sink.Config.CustomConfig)
	if err != nil {
		return sc, err
	}
	err = json.Unmarshal(sinkConfigIn, &sc)
	return sc, err
}

type Table struct {
	Name string `json:"name"`

	// Table spec for BigTable are built up by RowKey and ColumnFamilies
	RowKey         RowKey         `json:"rowKey"`
	ColumnFamilies []ColumnFamily `json:"columnFamilies"`

	// Only input transformations satisfying the whitelist key/value filter will be
	// processed by the sink (mostly needed in multi-table Sink specs)
	Whitelist *Whitelist `json:"whitelist,omitempty"`
}

// RowKey specifies how the row-key should be generated for BigTable sinks.
// If one of the Predefined options are set, that will be used.
// Currently available Predefined options are:
//
//	"timestampIso"
//	"invertedTimestamp"
//	"uuid"
//	"keysInMap"
//
// If Predefined is not set, the Keys array should be used to specify which extracted fields
// from the event should be used.
// TODO: Add padding config
type RowKey struct {
	Predefined string   `json:"predefined,omitempty"`
	Keys       []string `json:"keys,omitempty"`
	Delimiter  string   `json:"delimiter,omitempty"`

	// Only required when using the Predefined rowkey option "keysInMap". This id should map to the transformed
	// output map item specified in ExtractItemsFromArray.Id
	MapId string `json:"mapId,omitempty"`
}

type Whitelist struct {
	Id     string   `json:"id"`
	Type   string   `json:"type"`
	Values []string `json:"values"`
}

type ColumnFamily struct {
	Name                    string                   `json:"name"`
	GarbageCollectionPolicy *GarbageCollectionPolicy `json:"garbageCollectionPolicy"`
	ColumnQualifiers        []ColumnQualifier        `json:"columnQualifiers"`
}

// TODO: Add support for Intersection and Union policies
// The following types are supported:
// - MaxVersions: where Value takes an integer of number of old versions to keep (-1)
// - MaxAge: where Value takes an integer of number of hours before deleting the data.
type GarbageCollectionPolicy struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

// The Id field can be used directly in the Transformed map to fetch the value to be inserted
// The Name field is the actual CQ name to be used in the table.
// Either Name or NameFromId must be present, not both.
type ColumnQualifier struct {
	Id         string      `json:"id"`
	Name       string      `json:"name,omitempty"`
	NameFromId *NameFromId `json:"nameFromId,omitempty"`
}

// Creates a Column/CQ name from id outputs in transloaded event map
type NameFromId struct {
	Prefix       string `json:"prefix"`
	SuffixFromId string `json:"suffixFromId"`
}
