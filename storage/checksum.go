package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const pageChecksumSize = UInt32Size

var pageChecksumTable = crc32.MakeTable(crc32.Castagnoli)

func pagePayload(data []byte, checksums bool) ([]byte, error) {
	if !checksums {
		return data, nil
	}
	if len(data) <= pageChecksumSize {
		return nil, fmt.Errorf("page has %d bytes, too small for checksum", len(data))
	}
	return data[:len(data)-pageChecksumSize], nil
}

func sealPageData(data []byte, checksums bool) error {
	if !checksums {
		return nil
	}
	payload, err := pagePayload(data, true)
	if err != nil {
		return err
	}
	checksum := crc32.Checksum(payload, pageChecksumTable)
	binary.LittleEndian.PutUint32(data[len(payload):], checksum)
	return nil
}

func verifyPageData(data []byte, checksums bool) error {
	if !checksums {
		return nil
	}
	payload, err := pagePayload(data, true)
	if err != nil {
		return err
	}
	expected := binary.LittleEndian.Uint32(data[len(payload):])
	actual := crc32.Checksum(payload, pageChecksumTable)
	if actual != expected {
		return fmt.Errorf("%w: expected %08x, got %08x", ErrChecksumMismatch, expected, actual)
	}
	return nil
}
