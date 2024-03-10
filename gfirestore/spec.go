package gfirestore

import (
	"encoding/json"

	"github.com/zpiroux/geist/entity"
)

// SinkConfig specifies the schema for the "customConfig" field in the "sink" section
// of the stream spec. It enables arbitrary connector specific fields to be present in
// the stream spec.
type SinkConfig struct {
	Kinds []Kind `json:"kinds,omitempty"` // TODO: Probably remove array and keep single object
}

func NewSinkConfig(spec *entity.Spec) (sc SinkConfig, err error) {
	sinkConfigIn, err := json.Marshal(spec.Sink.Config.CustomConfig)
	if err != nil {
		return sc, err
	}
	err = json.Unmarshal(sinkConfigIn, &sc)
	return sc, err
}

// The Kind struct is used for Firestore sinks (in datastore mode).
// Currently, one of EntityName or EntityNameFromIds needs to be present in spec.
// TODO: Add creation of UUID if EntityName/Ref not present
type Kind struct {
	// If Namespace here is present, it will override the global one.
	// If both are missing, the Kind will use native 'default'
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`

	// If set, will be used as the actual Entity Name
	EntityName string `json:"entityName,omitempty"`

	// If set, will be used to create the Entity Name from the "id" values in the Transload output map.
	// The value is currently restricted to be of type string.
	EntityNameFromIds struct {
		Ids       []string `json:"ids,omitempty"`
		Delimiter string   `json:"delimiter,omitempty"`
	} `json:"entityNameFromIds,omitempty"`

	Properties []EntityProperty `json:"properties,omitempty"`
}

type EntityProperty struct {
	Name string `json:"name"`

	// Id is the key/field ID in the Transformed output map, which contains the actual value
	// for this property. The value type is the same as the output from the Transform.
	Id string `json:"id"`

	// For most properties this should be set to true, for improved query performance, but for big event
	// fields that might exceed 1500 bytes, this should be set to false, since that is a built-in
	// Firestore limit.
	Index bool `json:"index"`
}
