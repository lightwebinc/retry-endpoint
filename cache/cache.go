// Package cache provides a modular cache backend for storing
// multicast BSV transaction frames with configurable TTL.
package cache

import "time"

// Cache defines the interface for frame storage backends.
type Cache interface {
	// Store stores a value under the given key with the specified TTL.
	// key is a 16-byte composite: HashKey (8B, stable per-flow XXH64
	// identifier) followed by SeqNum (8B, monotonic per-flow counter).
	// value is the raw frame bytes.
	Store(key []byte, value []byte, ttl time.Duration) error

	// Retrieve retrieves the frame value for the given key.
	// Returns nil if the key does not exist or has expired.
	Retrieve(key []byte) ([]byte, error)

	// Delete removes the key from the cache.
	Delete(key []byte) error

	// Close releases any resources held by the cache backend.
	Close() error
}
