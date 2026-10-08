package types

import "time"

// StorageCapacity is a current provider measurement, never an index estimate.
// ScopeID names a filesystem/bucket quota, so separate backends sharing a disk
// share one admission reservation pool. Paths and credentials are not exposed.
type StorageCapacity struct {
	ScopeID        string    `json:"scope_id"`
	TotalBytes     int64     `json:"total_bytes"`
	AvailableBytes int64     `json:"available_bytes"`
	MeasuredAt     time.Time `json:"measured_at"`
}
