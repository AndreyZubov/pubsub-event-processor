package schema

import (
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
)

const decoderSchemaJSON = `{
  "type": "record",
  "name": "OrderEvent",
  "fields": [
    {"name": "OrderId", "type": "string"},
    {"name": "Amount",  "type": "double"},
    {"name": "Count",   "type": "long"},
    {"name": "Active",  "type": "boolean"},
    {"name": "Note",    "type": ["null", "string"], "default": null}
  ]
}`

func mustSchema(t *testing.T, src string) avro.Schema {
	t.Helper()
	s, err := avro.Parse(src)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	return s
}

func TestDecode_Roundtrip(t *testing.T) {
	schema := mustSchema(t, decoderSchemaJSON)
	source := map[string]any{
		"OrderId": "ORD-1",
		"Amount":  19.99,
		"Count":   int64(7),
		"Active":  true,
		"Note":    map[string]any{"string": "hello"},
	}
	encoded, err := avro.Marshal(schema, source)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := Decode(schema, encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if got["OrderId"] != "ORD-1" {
		t.Errorf("OrderId: %v", got["OrderId"])
	}
	if got["Amount"] != 19.99 {
		t.Errorf("Amount: %v", got["Amount"])
	}
	if got["Count"] != int64(7) {
		t.Errorf("Count: %v (%T)", got["Count"], got["Count"])
	}
	if got["Active"] != true {
		t.Errorf("Active: %v", got["Active"])
	}
	if note, ok := got["Note"].(string); !ok || note != "hello" {
		t.Errorf("Note: %v (%T)", got["Note"], got["Note"])
	}
}

func TestDecode_NullableNull(t *testing.T) {
	schema := mustSchema(t, decoderSchemaJSON)
	source := map[string]any{
		"OrderId": "ORD-2",
		"Amount":  0.0,
		"Count":   int64(0),
		"Active":  false,
		"Note":    nil,
	}
	encoded, err := avro.Marshal(schema, source)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := Decode(schema, encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got["Note"] != nil {
		t.Errorf("Note should be nil, got %v (%T)", got["Note"], got["Note"])
	}
}

func TestDecode_NestedRecord(t *testing.T) {
	nestedJSON := `{
      "type": "record",
      "name": "OrderWithAddress",
      "fields": [
        {"name": "OrderId", "type": "string"},
        {"name": "Address", "type": {
          "type": "record",
          "name": "Address",
          "fields": [
            {"name": "City",    "type": "string"},
            {"name": "Country", "type": "string"}
          ]
        }}
      ]
    }`
	schema := mustSchema(t, nestedJSON)
	source := map[string]any{
		"OrderId": "ORD-3",
		"Address": map[string]any{
			"City":    "Prague",
			"Country": "CZ",
		},
	}
	encoded, err := avro.Marshal(schema, source)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := Decode(schema, encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	addr, ok := got["Address"].(map[string]any)
	if !ok {
		t.Fatalf("Address: %v (%T)", got["Address"], got["Address"])
	}
	if addr["City"] != "Prague" {
		t.Errorf("City: %v", addr["City"])
	}
	if addr["Country"] != "CZ" {
		t.Errorf("Country: %v", addr["Country"])
	}
}

func TestDecode_NilSchema(t *testing.T) {
	_, err := Decode(nil, []byte{0x01})
	if err == nil {
		t.Fatal("expected error for nil schema")
	}
}

func TestDecode_EmptyPayload(t *testing.T) {
	schema := mustSchema(t, decoderSchemaJSON)
	_, err := Decode(schema, nil)
	if err == nil {
		t.Fatal("expected error for empty payload")
	}
}

func TestDecode_GarbageBytes(t *testing.T) {
	schema := mustSchema(t, decoderSchemaJSON)
	_, err := Decode(schema, []byte{0xFF, 0xFF, 0xFF, 0xFF})
	if err == nil {
		t.Fatal("expected error for malformed bytes")
	}
	if !strings.Contains(err.Error(), "avro unmarshal") {
		t.Errorf("error should wrap avro: %v", err)
	}
}
