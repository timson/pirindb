package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"
)

const maxReportedOrphanPages = 128

type IntegrityReport struct {
	PageChecksums   bool
	CheckedPages    uint64
	BTreePages      uint64
	BlobPages       uint64
	FreelistPages   uint64
	ReleasedPages   uint64
	BucketCount     uint64
	ItemCount       uint64
	OrphanPageCount uint64
	OrphanPages     []uint64
}

type integrityBucketStats struct {
	items uint64
	blobs uint64
	bytes uint64
}

type integrityChecker struct {
	tx      *Tx
	report  *IntegrityReport
	owners  map[uint64]string
	buckets []*Bucket
}

func (checker *integrityChecker) claim(pageNum uint64, owner string) error {
	dal := checker.tx.db.dal
	if pageNum >= dal.maxPages || pageNum > dal.freelist.currentPage {
		return fmt.Errorf("%w: %s references page %d outside the allocated range", ErrIntegrityCheckFailed, owner, pageNum)
	}
	if previous, exists := checker.owners[pageNum]; exists {
		return fmt.Errorf("%w: page %d is owned by both %s and %s", ErrIntegrityCheckFailed, pageNum, previous, owner)
	}
	if dal.freelist.containsReleasedPage(pageNum) {
		return fmt.Errorf("%w: live page %d (%s) is present in the freelist", ErrIntegrityCheckFailed, pageNum, owner)
	}
	checker.owners[pageNum] = owner
	checker.report.CheckedPages++
	return nil
}

func (checker *integrityChecker) inspectValue(item *Item, owner string) (int, bool, error) {
	if item == nil || len(item.Value) == 0 {
		return 0, false, ErrUnknownItemType
	}
	switch item.Value[0] {
	case ValueSimple:
		return len(item.Value) - 1, false, nil
	case ValueBlob:
		if len(item.Value) != 1+UInt64Size {
			return 0, false, ErrUnknownItemType
		}
		root := binary.LittleEndian.Uint64(item.Value[1:])
		page, err := checker.tx.getPage(root)
		if err != nil {
			return 0, false, err
		}
		_, dataLen, err := decodeBlobHeaderForSize(page, checker.tx.db.dal.usablePageSize())
		if err != nil {
			return 0, false, err
		}
		_, _, err = visitBlobPages(checker.tx, root, page, func(pageNum uint64, _ []byte) error {
			if claimErr := checker.claim(pageNum, owner+" blob"); claimErr != nil {
				return claimErr
			}
			checker.report.BlobPages++
			return nil
		})
		return dataLen, true, err
	default:
		return 0, false, ErrUnknownItemType
	}
}

func (checker *integrityChecker) walkTree(pageNum uint64, owner string, catalog bool, lower, upper []byte, depth int) (integrityBucketStats, error) {
	var stats integrityBucketStats
	if depth > 128 {
		return stats, fmt.Errorf("%w: b-tree depth exceeds 128", ErrIntegrityCheckFailed)
	}
	if err := checker.claim(pageNum, owner+" b-tree"); err != nil {
		return stats, err
	}
	checker.report.BTreePages++
	node, err := checker.tx.getNode(pageNum)
	if err != nil {
		return stats, err
	}
	if depth > 0 && len(node.items) == 0 {
		return stats, fmt.Errorf("%w: non-root b-tree page %d is empty", ErrIntegrityCheckFailed, pageNum)
	}
	for _, item := range node.items {
		if lower != nil && bytes.Compare(item.Key, lower) <= 0 {
			return stats, fmt.Errorf("%w: key %q is outside its lower bound", ErrIntegrityCheckFailed, item.Key)
		}
		if upper != nil && bytes.Compare(item.Key, upper) >= 0 {
			return stats, fmt.Errorf("%w: key %q is outside its upper bound", ErrIntegrityCheckFailed, item.Key)
		}
		if catalog {
			if err := validateBucketName(item.Key); err != nil {
				return stats, fmt.Errorf("%w: invalid bucket name", ErrIntegrityCheckFailed)
			}
			if len(item.Value) != 1+BucketTotalSize || item.Value[0] != ValueSimple {
				return stats, fmt.Errorf("%w: bucket %q has a malformed descriptor", ErrIntegrityCheckFailed, item.Key)
			}
			bucket := newBucket(item.Key)
			bucket.tx = checker.tx
			if err = bucket.deserialize(item.Value[1:]); err != nil {
				return stats, err
			}
			checker.buckets = append(checker.buckets, bucket)
		} else {
			logicalLen, blob, valueErr := checker.inspectValue(item, owner)
			if valueErr != nil {
				return stats, valueErr
			}
			stats.items++
			stats.bytes += uint64(len(item.Key) + logicalLen)
			if blob {
				stats.blobs++
			}
		}
	}
	if node.isLeaf() {
		return stats, nil
	}
	for childIndex, childPageNum := range node.childNodes {
		childLower := lower
		childUpper := upper
		if childIndex > 0 {
			childLower = node.items[childIndex-1].Key
		}
		if childIndex < len(node.items) {
			childUpper = node.items[childIndex].Key
		}
		childStats, childErr := checker.walkTree(childPageNum, owner, catalog, childLower, childUpper, depth+1)
		if childErr != nil {
			return stats, childErr
		}
		stats.items += childStats.items
		stats.blobs += childStats.blobs
		stats.bytes += childStats.bytes
	}
	return stats, nil
}

func (checker *integrityChecker) validateFreelist() error {
	f := checker.tx.db.dal.freelist
	var count uint64
	var previous *pageExtent
	for idx := range f.releasedExtents {
		extent := &f.releasedExtents[idx]
		if extent.start < f.firstDataPage || extent.start > extent.end || extent.end > f.currentPage || extent.end >= f.maxPages {
			return fmt.Errorf("%w: invalid freelist extent %d-%d", ErrIntegrityCheckFailed, extent.start, extent.end)
		}
		if previous != nil && extent.start <= previous.end+1 {
			return fmt.Errorf("%w: freelist extents overlap or are not canonical", ErrIntegrityCheckFailed)
		}
		count += extent.length()
		previous = extent
	}
	if count != f.releasedCount {
		return fmt.Errorf("%w: freelist count is %d, extents contain %d pages", ErrIntegrityCheckFailed, f.releasedCount, count)
	}
	return nil
}

func (checker *integrityChecker) findOrphans() {
	f := checker.tx.db.dal.freelist
	intervals := append([]pageExtent(nil), f.releasedExtents...)
	for pageNum := range checker.owners {
		intervals = append(intervals, pageExtent{start: pageNum, end: pageNum})
	}
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].start == intervals[j].start {
			return intervals[i].end < intervals[j].end
		}
		return intervals[i].start < intervals[j].start
	})
	next := uint64(0)
	for _, interval := range intervals {
		if interval.start > next {
			checker.addOrphanRange(next, interval.start-1)
		}
		if interval.end == ^uint64(0) {
			return
		}
		if interval.end+1 > next {
			next = interval.end + 1
		}
	}
	if next <= f.currentPage {
		checker.addOrphanRange(next, f.currentPage)
	}
}

func (checker *integrityChecker) addOrphanRange(start, end uint64) {
	if end < start {
		return
	}
	checker.report.OrphanPageCount += end - start + 1
	for pageNum := start; pageNum <= end && len(checker.report.OrphanPages) < maxReportedOrphanPages; pageNum++ {
		checker.report.OrphanPages = append(checker.report.OrphanPages, pageNum)
		if pageNum == ^uint64(0) {
			break
		}
	}
}

func (db *DB) Check() (_ *IntegrityReport, returnErr error) {
	report := &IntegrityReport{}
	defer func() {
		if returnErr != nil && !errors.Is(returnErr, ErrIntegrityCheckFailed) {
			returnErr = fmt.Errorf("%w: %w", ErrIntegrityCheckFailed, returnErr)
		}
	}()
	if db == nil {
		return report, ErrDatabaseClosed
	}
	tx, err := db.BeginE(false)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	// Integrity checks must inspect current physical bytes rather than allowing
	// a previously cached node to hide latent on-disk corruption.
	tx.bypassNodeCache = true
	checker := &integrityChecker{tx: tx, report: report, owners: make(map[uint64]string)}
	report.PageChecksums = db.dal.pageChecksums
	report.ReleasedPages = db.dal.freelist.releasedPageN()
	if err = checker.validateFreelist(); err != nil {
		return report, err
	}
	physicalMeta, physicalMetaErr := ReadMeta(db.dal)
	if physicalMetaErr != nil {
		return report, physicalMetaErr
	}
	physicalStateMismatch := *physicalMeta != *db.dal.meta
	physicalFreelist, physicalFreelistErr := ReadFreelist(db.dal)
	if physicalFreelistErr != nil {
		return report, physicalFreelistErr
	}
	physicalStateMismatch = physicalStateMismatch || physicalFreelist.currentPage != db.dal.freelist.currentPage ||
		physicalFreelist.maxPages != db.dal.freelist.maxPages ||
		physicalFreelist.releasedCount != db.dal.freelist.releasedCount ||
		!slices.Equal(physicalFreelist.releasedExtents, db.dal.freelist.releasedExtents) ||
		!slices.Equal(physicalFreelist.freelistPages, db.dal.freelist.freelistPages)
	if err = checker.claim(metaPageNumber, "metadata"); err != nil {
		return report, err
	}
	if db.dal.pageChecksums {
		if err = checker.claim(metaMirrorPageNumber, "metadata mirror"); err != nil {
			return report, err
		}
	}
	for _, pageNum := range db.dal.freelist.freelistPages {
		if err = checker.claim(pageNum, "freelist metadata"); err != nil {
			return report, err
		}
		report.FreelistPages++
	}
	if _, err = checker.walkTree(db.dal.meta.root, "bucket catalog", true, nil, nil, 0); err != nil {
		return report, err
	}
	for _, bucket := range checker.buckets {
		stats, walkErr := checker.walkTree(bucket.root, fmt.Sprintf("bucket %q", bucket.name), false, nil, nil, 0)
		if walkErr != nil {
			return report, walkErr
		}
		if stats.items != bucket.itemsN || stats.blobs != bucket.blobsN || stats.bytes != bucket.bytesInUse {
			return report, fmt.Errorf("%w: bucket %q statistics mismatch: stored=(%d,%d,%d) actual=(%d,%d,%d)",
				ErrIntegrityCheckFailed, bucket.name, bucket.itemsN, bucket.blobsN, bucket.bytesInUse,
				stats.items, stats.blobs, stats.bytes)
		}
		report.BucketCount++
		report.ItemCount += stats.items
	}
	checker.findOrphans()
	if report.OrphanPageCount > 0 {
		return report, fmt.Errorf("%w: found %d orphaned allocated pages", ErrIntegrityCheckFailed, report.OrphanPageCount)
	}
	if physicalStateMismatch {
		return report, fmt.Errorf("%w: in-memory and physical metadata or freelist disagree", ErrIntegrityCheckFailed)
	}
	return report, nil
}
