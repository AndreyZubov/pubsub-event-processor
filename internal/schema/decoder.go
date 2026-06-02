package schema

import (
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
)

// Decode unmarshals an Avro-binary payload into a generic record map using the
// provided schema. The result has field-name keys and values typed by the schema:
// strings, numbers, booleans, time.Time for logical timestamp types, and nested
// map[string]any / []any for nested records and arrays.
//
// Returns an error if payload is empty or does not match the schema.
func Decode(schema avro.Schema, payload []byte) (map[string]any, error) {
	if schema == nil {
		return nil, errors.New("nil schema")
	}
	if len(payload) == 0 {
		return nil, errors.New("empty payload")
	}
	result := make(map[string]any)
	if err := avro.Unmarshal(schema, payload, &result); err != nil {
		return nil, fmt.Errorf("avro unmarshal: %w", err)
	}
	return result, nil
}
