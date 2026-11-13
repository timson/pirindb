package storage

type SyncPolicy uint8

const (
	SyncPolicyStrict SyncPolicy = iota
	SyncPolicyJournal
	SyncPolicyGroup
)
