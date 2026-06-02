// Package event defines the shared in-memory representation of a Salesforce
// event after Avro decoding. Producers (subscriber + decoder) build it;
// consumers (handler, storage, sink) read it. The package has no dependencies
// beyond the standard library, so it does not couple the pipeline stages to
// each other.
package event

import "time"

// DecodedEvent is one Avro-decoded Salesforce event with all metadata needed
// for persistence, idempotency, and downstream forwarding.
type DecodedEvent struct {
	// Topic is the Salesforce channel the event arrived on, e.g. "/event/Order_Event__e".
	Topic string

	// EventID is the per-event UUID provided by Salesforce. Used as the
	// idempotency key when persisting.
	EventID string

	// SchemaID identifies the Avro schema this event was encoded with.
	SchemaID string

	// ReplayID is the opaque cursor into the topic's event stream; persisted
	// to allow resumable subscription after restart.
	ReplayID []byte

	// Payload is the decoded event body keyed by field name.
	Payload map[string]any

	// ReceivedAt is when the subscriber observed the event on the stream.
	ReceivedAt time.Time
}
