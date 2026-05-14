// Package cache provides a modular cache backend for storing
// multicast BSV transaction frames with configurable TTL.
package cache

import "time"

// Cache defines the interface for frame storage backends.
type Cache interface {
	// Store stores a value under the given key with the specified TTL.
	// key is a 41-byte composite: prefix byte (0x00 = PrevSeq secondary index,
	// 0x01 = CurSeq primary index) followed by the 32-byte SubtreeID
	// namespace and the 8-byte XXH64 sequence value.
	// For the primary index, value is the raw frame bytes.
	// For the secondary index, value is the 8-byte CurSeq of the primary entry.
	Store(key []byte, value []byte, ttl time.Duration) error

	// Retrieve retrieves the frame value for the given key.
	// Returns nil if the key does not exist or has expired.
	Retrieve(key []byte) ([]byte, error)

	// Delete removes the key from the cache.
	Delete(key []byte) error

	// Close releases any resources held by the cache backend.
	Close() error
}
