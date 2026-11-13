package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const metadataIdentityOffset = 128
const metadataIdentitySize = 44

var metadataIdentityMagic = []byte("PIRINID4")

func newDatabaseID() (id [16]byte) { _, _ = rand.Read(id[:]); return }
func writeMetadataIdentity(data []byte, m *Meta) error {
	if len(data) < metadataIdentityOffset+metadataIdentitySize || m.databaseID == ([16]byte{}) {
		return fmt.Errorf("%w: missing database identity", ErrCorruptedMeta)
	}
	b := data[metadataIdentityOffset : metadataIdentityOffset+metadataIdentitySize]
	copy(b, metadataIdentityMagic)
	binary.LittleEndian.PutUint64(b[8:], m.pageSize)
	copy(b[16:], m.databaseID[:])
	binary.LittleEndian.PutUint64(b[32:], m.commitSequence)
	binary.LittleEndian.PutUint32(b[40:], crc32.ChecksumIEEE(b[:40]))
	return nil
}
func decodeMetadataIdentity(data []byte) (*Meta, error) {
	if len(data) < metadataIdentityOffset+metadataIdentitySize {
		return nil, ErrCorruptedMeta
	}
	b := data[metadataIdentityOffset : metadataIdentityOffset+metadataIdentitySize]
	if !bytes.Equal(b[:8], metadataIdentityMagic) || binary.LittleEndian.Uint32(b[40:]) != crc32.ChecksumIEEE(b[:40]) {
		return nil, fmt.Errorf("%w: damaged database identity", ErrCorruptedMeta)
	}
	m := NewMeta(binary.LittleEndian.Uint64(b[8:]))
	copy(m.databaseID[:], b[16:32])
	m.commitSequence = binary.LittleEndian.Uint64(b[32:])
	if m.databaseID == ([16]byte{}) {
		return nil, ErrCorruptedMeta
	}
	if err := validatePageSize(m.pageSize); err != nil {
		return nil, err
	}
	return m, nil
}
func readDatabaseIdentity(file storageFile, knownPageSize ...uint64) (*Meta, error) {
	var identity *Meta
	for size := uint64(MinPageSize); size <= MaxPageSize; size *= 2 {
		if len(knownPageSize) > 0 && size != knownPageSize[0] {
			continue
		}
		for _, page := range []uint64{metaPageNumber, metaMirrorPageNumber} {
			data := readMetaPageAt(file, page, size)
			m, err := decodeMetadataIdentity(data)
			if err != nil || m.pageSize != size {
				continue
			}
			if identity != nil && (identity.databaseID != m.databaseID || identity.pageSize != m.pageSize) {
				return nil, ErrJournalIdentity
			}
			if identity == nil || m.commitSequence > identity.commitSequence {
				identity = m
			}
		}
	}
	if identity == nil {
		return nil, ErrCorruptedMeta
	}
	return identity, nil
}
func migrationMetadata(a, b *Meta) (*Meta, bool) {
	if a.dbVersion == 3 && b.dbVersion == 4 && a.databaseID == ([16]byte{}) && b.databaseID != ([16]byte{}) && a.root == b.root && a.pageSize == b.pageSize && a.freelistPageNumber == b.freelistPageNumber && a.dbName == b.dbName {
		return b, true
	}
	if b.dbVersion == 3 && a.dbVersion == 4 {
		return migrationMetadata(b, a)
	}
	return nil, false
}
func (dal *Dal) validateJournalIdentity() error {
	log := dal.txLog
	if err := log.ensureHeaderLoaded(); err != nil {
		return err
	}
	if log.header.pageSize != uint16(dal.meta.pageSize) {
		return ErrJournalIdentity
	}
	active, err := log.HasActiveJournal()
	if err != nil {
		return err
	}
	if active && log.headerVersion < txLogVersionV3 {
		return fmt.Errorf("%w: recover v1/v2 journals with the original compatible binary before upgrading", ErrLegacyRecovery)
	}
	if active && log.header.databaseID == dal.meta.databaseID && log.header.commitSequence < max(dal.meta.commitSequence, dal.recoverySequenceFloor) {
		return fmt.Errorf("%w: journal predates database checkpoint", ErrJournalIdentity)
	}
	if log.header.databaseID != ([16]byte{}) {
		if log.header.databaseID != dal.meta.databaseID {
			return ErrJournalIdentity
		}
	} else if active && !dal.opts.AllowLegacyRecovery {
		return ErrLegacyRecovery
	}
	return nil
}
func (log *TxLog) bindInactiveDatabase(id [16]byte) error {
	if err := log.ensureHeaderLoaded(); err != nil {
		return err
	}
	if log.header.databaseID != ([16]byte{}) && log.header.databaseID != id {
		return ErrJournalIdentity
	}
	if log.header.databaseID == id && log.headerVersion == txLogVersion {
		return nil
	}
	if log.header.isActive() {
		return ErrRecoveryRequired
	}
	header := newTxLogHeader(log.pageSize)
	header.databaseID = id
	// Establish the new secondary before overwriting a legacy primary.
	log.headerCopy = 0
	if err := log.writeCurrentHeader(header, true); err != nil {
		return err
	}
	return log.mirrorCurrentHeader(true)
}
