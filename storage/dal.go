package storage

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	minFileSize = 1024 * 32 // 32KB
	OneGigabyte = 1024 * 1024 * 1024
)

type Dal struct {
	file              *os.File
	osPageSize        uint64
	maxPages          uint64
	size              uint64
	MinFillPercent    float32
	MaxFillPercent    float32
	freelist          *Freelist
	meta              *Meta
	fileLock          *flock.Flock
	txLogLock         *flock.Flock
	txLog             *TxLog
	opts              *Options
	beforeSetPageHook func(p *Page) error
	pendingJournal    map[uint64]*Page
	pendingJournalTxN int
	pageOverlay       map[uint64]*Page
	recoveryPending   bool
	pageChecksums     bool
	metaNeedsRepair   bool
	metaRepairPage    uint64
	nodeCache         *nodeCache
}

func NewDal(path string, opts *Options) (*Dal, error) {
	effectiveOpts := cloneOptions(opts)
	if effectiveOpts.TxLogPath == "" {
		ext := filepath.Ext(path)
		effectiveOpts.TxLogPath = strings.TrimSuffix(path, ext) + ".tlog"
	}
	dbPath, pathErr := filepath.Abs(path)
	if pathErr != nil {
		return nil, fmt.Errorf("resolve database path: %w", pathErr)
	}
	txLogPath, pathErr := filepath.Abs(effectiveOpts.TxLogPath)
	if pathErr != nil {
		return nil, fmt.Errorf("resolve transaction log path: %w", pathErr)
	}
	if dbPath == txLogPath {
		return nil, fmt.Errorf("%w: database and transaction log paths must differ", ErrInvalidOptions)
	}

	fileExists := false
	if _, statErr := os.Stat(path); statErr == nil {
		fileExists = true
		logger.Debug("database file already exists", "path", path)
	} else if !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("could not stat dal: %w", statErr)
	}
	txLogExists := false
	var txLogSize int64
	if txLogInfo, statErr := os.Stat(effectiveOpts.TxLogPath); statErr == nil {
		txLogExists = true
		txLogSize = txLogInfo.Size()
		if fileExists {
			if dbInfo, dbStatErr := os.Stat(path); dbStatErr == nil && os.SameFile(dbInfo, txLogInfo) {
				return nil, fmt.Errorf("%w: database and transaction log refer to the same file", ErrInvalidOptions)
			}
		}
	} else if !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("could not stat transaction log: %w", statErr)
	}
	if !fileExists && txLogExists && txLogSize > 0 {
		return nil, fmt.Errorf("%w: refusing to create a database while non-empty transaction log %s exists", ErrRecoveryRequired, effectiveOpts.TxLogPath)
	}

	fileLock := flock.New(path)
	locked, lockErr := fileLock.TryLock()
	if lockErr != nil {
		return nil, fmt.Errorf("could not lock database file %s: %w", path, lockErr)
	}
	if !locked {
		return nil, fmt.Errorf("database file %s is already locked", path)
	}

	file, openErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, effectiveOpts.FileMode)
	if openErr != nil {
		_ = fileLock.Unlock()
		return nil, fmt.Errorf("could not open dal: %w", openErr)
	}
	cleanupEarly := func() {
		_ = file.Close()
		_ = fileLock.Unlock()
		if !fileExists {
			_ = os.Remove(path)
		}
	}
	fileInfo, statErr := file.Stat()
	if statErr != nil {
		cleanupEarly()
		return nil, fmt.Errorf("could not stat dal: %w", statErr)
	}

	meta := NewMeta(effectiveOpts.PageSize)
	if fileExists {
		meta, statErr = ReadMetaHeader(file)
		if statErr != nil {
			cleanupEarly()
			return nil, fmt.Errorf("could not bootstrap metadata: %w", statErr)
		}
		effectiveOpts.PageSize = meta.pageSize
	}
	if validateErr := validateOptions(effectiveOpts); validateErr != nil {
		cleanupEarly()
		return nil, validateErr
	}

	pageSize := effectiveOpts.PageSize
	var fileSize uint64
	if fileExists {
		if fileInfo.Size() < 0 || uint64(fileInfo.Size())%pageSize != 0 {
			cleanupEarly()
			return nil, fmt.Errorf("%w: file size %d is not aligned to page size %d", ErrCorruptedMeta, fileInfo.Size(), pageSize)
		}
		fileSize = uint64(fileInfo.Size())
	} else {
		minimum := max(uint64(minFileSize), pageSize*uint64(rootPageNumber+1))
		fileSize = ((minimum + pageSize - 1) / pageSize) * pageSize
	}
	minimumPageCount := uint64(legacyRootPageNumber + 1)
	if meta.usesPageChecksums() {
		minimumPageCount = uint64(rootPageNumber + 1)
	}
	if fileSize/pageSize < minimumPageCount {
		cleanupEarly()
		return nil, fmt.Errorf("%w: database contains fewer than %d pages", ErrCorruptedMeta, minimumPageCount)
	}

	txLogLock := flock.New(effectiveOpts.TxLogPath)
	txLogLocked, txLogLockErr := txLogLock.TryLock()
	if txLogLockErr != nil {
		cleanupEarly()
		return nil, fmt.Errorf("could not lock transaction log %s: %w", effectiveOpts.TxLogPath, txLogLockErr)
	}
	if !txLogLocked {
		cleanupEarly()
		return nil, fmt.Errorf("transaction log %s is already locked", effectiveOpts.TxLogPath)
	}
	tlog := NewTxLog(effectiveOpts.TxLogPath, 0600, pageSize)
	if tlog.openErr != nil {
		_ = txLogLock.Unlock()
		cleanupEarly()
		if !txLogExists {
			_ = os.Remove(effectiveOpts.TxLogPath)
		}
		return nil, fmt.Errorf("could not open transaction log: %w", tlog.openErr)
	}

	dal := &Dal{
		fileLock:       fileLock,
		file:           file,
		meta:           meta,
		osPageSize:     uint64(os.Getpagesize()),
		freelist:       newFreelist(pageSize, fileSize/pageSize, meta.usesPageChecksums()),
		MinFillPercent: 0.45,
		MaxFillPercent: 0.95,
		txLog:          tlog,
		txLogLock:      txLogLock,
		opts:           effectiveOpts,
		pendingJournal: make(map[uint64]*Page),
		pageOverlay:    make(map[uint64]*Page),
		size:           fileSize,
		maxPages:       fileSize / pageSize,
		pageChecksums:  meta.usesPageChecksums(),
		nodeCache:      newNodeCache(pageSize, effectiveOpts.NodeCacheBytes),
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = dal.Close()
			if !fileExists {
				_ = os.Remove(path)
			}
			if !txLogExists {
				_ = os.Remove(effectiveOpts.TxLogPath)
			}
		}
	}()

	if !fileExists {
		if allocateErr := dal.allocateFile(fileSize); allocateErr != nil {
			return nil, allocateErr
		}
	} else if meta.root >= dal.maxPages || meta.freelistPageNumber >= dal.maxPages {
		return nil, fmt.Errorf("%w: metadata references pages outside the file", ErrCorruptedMeta)
	}

	logger.Info("open database file", "path", path, "size", fileSize,
		"page_size", pageSize, "tx_log", effectiveOpts.TxLogPath)

	if fileExists {
		if effectiveOpts.EnableRecovery {
			recoveredPages := 0
			recoverErr := tlog.Recover(func(_ uint64, page *Page) error {
				if setErr := dal.SetPage(page); setErr != nil {
					return setErr
				}
				recoveredPages++
				return nil
			})
			if recoverErr != nil {
				return nil, fmt.Errorf("could not apply tx log: %w", recoverErr)
			}
			if recoveredPages > 0 {
				if syncErr := dal.Sync(); syncErr != nil {
					return nil, fmt.Errorf("could not sync recovered pages: %w", syncErr)
				}
				logger.Info("tx log applied", "recovered_pages", recoveredPages)
			}
			if clearErr := tlog.Clear(); clearErr != nil {
				return nil, fmt.Errorf("could not clear tx log: %w", clearErr)
			}
		} else if active, activeErr := tlog.HasActiveJournal(); activeErr != nil {
			return nil, fmt.Errorf("could not inspect transaction log: %w", activeErr)
		} else {
			dal.recoveryPending = active
		}

		readMeta, readMetaErr := ReadMeta(dal)
		if readMetaErr != nil {
			return nil, fmt.Errorf("could not read meta: %w", readMetaErr)
		}
		if readMeta.pageSize != pageSize || readMeta.root >= dal.maxPages || readMeta.freelistPageNumber >= dal.maxPages {
			return nil, fmt.Errorf("%w: recovered metadata is inconsistent with the database file", ErrCorruptedMeta)
		}
		dal.meta = readMeta
		freelist, readFreelistErr := ReadFreelist(dal)
		if readFreelistErr != nil {
			return nil, fmt.Errorf("could not read freelist: %w", readFreelistErr)
		}
		dal.freelist = freelist
		if dal.meta.root > dal.freelist.currentPage || dal.freelist.containsReleasedPage(dal.meta.root) || dal.freelist.isFreelistStoragePage(dal.meta.root) {
			return nil, fmt.Errorf("%w: catalog root %d is not a live data page", ErrCorruptedMeta, dal.meta.root)
		}
		if (dal.metaNeedsRepair || dal.freelist.dirty) && !dal.recoveryPending {
			if dal.metaNeedsRepair {
				repairPage, buildErr := buildMetaPageAt(dal.metaRepairPage, dal.meta.pageSize, dal.meta)
				if buildErr != nil {
					return nil, fmt.Errorf("could not build metadata repair: %w", buildErr)
				}
				if writeErr := dal.SetPage(repairPage); writeErr != nil {
					return nil, fmt.Errorf("could not repair metadata mirror: %w", writeErr)
				}
			}
			if dal.freelist.dirty {
				if writeErr := WriteFreelist(dal, dal.freelist); writeErr != nil {
					return nil, fmt.Errorf("could not repair freelist: %w", writeErr)
				}
			}
			if syncErr := dal.Sync(); syncErr != nil {
				return nil, fmt.Errorf("could not sync repaired metadata: %w", syncErr)
			}
			dal.metaNeedsRepair = false
		}
	} else {
		if writeErr := WriteMeta(dal, dal.meta); writeErr != nil {
			return nil, fmt.Errorf("could not write meta: %w", writeErr)
		}
		dal.freelist.dirty = true
		if writeErr := WriteFreelist(dal, dal.freelist); writeErr != nil {
			return nil, fmt.Errorf("could not write freelist: %w", writeErr)
		}
		root := NewBNode()
		root.PageNum = rootPageNumber
		if _, writeErr := dal.setNode(root); writeErr != nil {
			return nil, fmt.Errorf("could not initialize root node: %w", writeErr)
		}
		if syncErr := dal.Sync(); syncErr != nil {
			return nil, fmt.Errorf("could not sync new database: %w", syncErr)
		}
	}
	if !txLogExists {
		if syncErr := dal.txLog.Sync(); syncErr != nil {
			return nil, fmt.Errorf("could not sync new transaction log: %w", syncErr)
		}
	}
	if !fileExists || !txLogExists {
		if syncErr := syncParentDirectories(path, effectiveOpts.TxLogPath); syncErr != nil {
			return nil, syncErr
		}
	}

	cleanup = false
	return dal, nil
}

func clonePage(page *Page) *Page {
	if page == nil {
		return nil
	}
	return &Page{
		PageNumber: page.PageNumber,
		Data:       append([]byte(nil), page.Data...),
	}
}

func (dal *Dal) usablePageSize() int {
	if dal == nil || dal.meta == nil {
		return 0
	}
	size := int(dal.meta.pageSize)
	if dal.pageChecksums {
		size -= pageChecksumSize
	}
	return size
}

func (dal *Dal) preparePage(page *Page) error {
	if page == nil {
		return fmt.Errorf("cannot prepare nil page")
	}
	if uint64(len(page.Data)) != dal.meta.pageSize {
		return fmt.Errorf("page %d has %d bytes, expected %d", page.PageNumber, len(page.Data), dal.meta.pageSize)
	}
	return sealPageData(page.Data, dal.pageChecksums)
}

func (dal *Dal) mergePendingJournalPages(pages []*Page) {
	for _, page := range pages {
		// Materialized commit pages are immutable after construction and remain
		// owned by the DAL until checkpoint, so pending/overlay state can share
		// them without full-page copies.
		dal.pendingJournal[page.PageNumber] = page
	}
	dal.pendingJournalTxN++
}

func (dal *Dal) stagePendingJournalPages(pages []*Page) (map[uint64]*Page, int, []*Page) {
	candidate := make(map[uint64]*Page, len(dal.pendingJournal)+len(pages))
	for pageNum, page := range dal.pendingJournal {
		candidate[pageNum] = page
	}
	for _, page := range pages {
		candidate[page.PageNumber] = page
	}
	return candidate, dal.pendingJournalTxN + 1, pageMapValues(candidate)
}

func (dal *Dal) installPendingJournal(candidate map[uint64]*Page, txN int) {
	dal.pendingJournal = candidate
	dal.pendingJournalTxN = txN
}

func pageMapValues(pagesByNum map[uint64]*Page) []*Page {
	pageNums := make([]uint64, 0, len(pagesByNum))
	for pageNum := range pagesByNum {
		pageNums = append(pageNums, pageNum)
	}
	sort.Slice(pageNums, func(i, j int) bool { return pageNums[i] < pageNums[j] })
	pages := make([]*Page, 0, len(pageNums))
	for _, pageNum := range pageNums {
		pages = append(pages, pagesByNum[pageNum])
	}
	return pages
}

func (dal *Dal) pendingJournalPages() []*Page {
	return pageMapValues(dal.pendingJournal)
}

func (dal *Dal) maxJournalPages() int {
	if dal == nil || dal.meta == nil || dal.meta.pageSize == 0 || dal.opts == nil {
		return 0
	}
	limit := dal.opts.MaxTransactionBytes / int64(dal.meta.pageSize)
	if limit > int64(^uint(0)>>1)-4 {
		return int(^uint(0) >> 1)
	}
	// Metadata copies and the first freelist page are commit bookkeeping,
	// rather than caller-owned dirty data. Leave a small fixed allowance.
	return int(limit) + 4
}

func (dal *Dal) projectedPendingJournalPages(pages []*Page) int {
	count := len(dal.pendingJournal)
	for _, page := range pages {
		if page == nil {
			continue
		}
		if _, exists := dal.pendingJournal[page.PageNumber]; !exists {
			count++
		}
	}
	return count
}

func (dal *Dal) resetPendingJournal() {
	clear(dal.pendingJournal)
	dal.pendingJournalTxN = 0
}

func (dal *Dal) mergeOverlayPages(pages []*Page) {
	for _, page := range pages {
		dal.pageOverlay[page.PageNumber] = page
	}
}

func (dal *Dal) overlayPages() []*Page {
	pages := make([]*Page, 0, len(dal.pageOverlay))
	for _, page := range dal.pageOverlay {
		pages = append(pages, page)
	}
	return pages
}

func (dal *Dal) resetOverlay() {
	clear(dal.pageOverlay)
}

func (dal *Dal) checkpointDue() bool {
	if dal.opts.SyncPolicy != SyncPolicyJournal {
		return false
	}
	if dal.opts.CheckpointTxThreshold <= 0 {
		return true
	}
	if dal.pendingJournalTxN >= dal.opts.CheckpointTxThreshold {
		return true
	}
	liveBytes := int64(len(dal.pendingJournal)) * int64(txLogPageHeaderSize+int(dal.meta.pageSize))
	if liveBytes > 0 {
		if logSize, err := dal.txLog.Size(); err == nil && logSize > int64(txLogDataOffset)+4*liveBytes {
			return true
		}
	}
	return false
}

func (dal *Dal) allocateFile(size uint64) error {
	if dal == nil || dal.file == nil {
		return ErrDatabaseClosed
	}
	if dal.meta == nil || dal.meta.pageSize == 0 || size%dal.meta.pageSize != 0 {
		return fmt.Errorf("invalid allocation size %d for page size %d", size, dal.meta.pageSize)
	}
	if size > uint64(^uint64(0)>>1) {
		return fmt.Errorf("database size %d exceeds supported file offset", size)
	}
	if err := dal.file.Truncate(int64(size)); err != nil {
		return fmt.Errorf("expand database file to %d bytes: %w", size, err)
	}
	dal.size = size
	dal.maxPages = size / dal.meta.pageSize
	if dal.freelist.maxPages != dal.maxPages {
		dal.freelist.maxPages = dal.maxPages
		dal.freelist.dirty = true
	}
	logger.Info("allocateFile", "size", dal.size, "max_pages", dal.maxPages)
	return nil
}

func (dal *Dal) expandAllocationToPages(requiredPages uint64) error {
	if requiredPages <= dal.maxPages {
		return nil
	}
	if requiredPages > ^uint64(0)/dal.meta.pageSize {
		return ErrNoPagesLeft
	}
	requiredSize := requiredPages * dal.meta.pageSize
	newSize := dal.size
	for newSize < requiredSize {
		if newSize < OneGigabyte {
			if newSize > ^uint64(0)/2 {
				return ErrNoPagesLeft
			}
			newSize *= 2
		} else {
			if newSize > ^uint64(0)-OneGigabyte {
				return ErrNoPagesLeft
			}
			newSize += OneGigabyte
		}
	}
	logger.Info("expand allocateFile", "size", newSize)
	if err := dal.allocateFile(newSize); err != nil {
		return err
	}
	// The journal may durably reference a newly allocated page before the
	// main-file checkpoint. Persist the file length first so recovery can
	// always address that page after a crash.
	if err := dal.file.Sync(); err != nil {
		return fmt.Errorf("sync expanded database file: %w", err)
	}
	return nil
}

func syncParentDirectories(paths ...string) error {
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		dir := filepath.Dir(path)
		absDir, err := filepath.Abs(dir)
		if err != nil {
			return fmt.Errorf("resolve parent directory %s: %w", dir, err)
		}
		if _, exists := seen[absDir]; exists {
			continue
		}
		seen[absDir] = struct{}{}
		dirFile, err := os.Open(absDir)
		if err != nil {
			return fmt.Errorf("open parent directory %s: %w", absDir, err)
		}
		syncErr := dirFile.Sync()
		closeErr := dirFile.Close()
		if syncErr != nil {
			return fmt.Errorf("sync parent directory %s: %w", absDir, syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close parent directory %s: %w", absDir, closeErr)
		}
	}
	return nil
}

func (dal *Dal) AllocatePage() (*Page, error) {
	pageNums, err := dal.AllocateConsecutivePageNumbers(1)
	if err != nil {
		return nil, err
	}
	return &Page{
		PageNumber: pageNums[0],
		Data:       make([]byte, dal.meta.pageSize),
	}, nil
}

func (dal *Dal) AllocateConsecutivePageNumbers(count int) ([]uint64, error) {
	if count <= 0 {
		return nil, fmt.Errorf("count must be greater than zero")
	}
	pageNums, err := dal.freelist.GetConsecutivePageNumbers(count)
	for errors.Is(err, ErrNoPagesLeft) {
		logger.Debug("trying allocate new page range, but no pages left", "count", count)
		prevSize := dal.size
		if dal.freelist.currentPage > ^uint64(0)-uint64(count)-1 {
			return nil, ErrNoPagesLeft
		}
		requiredPages := dal.freelist.currentPage + uint64(count) + 1
		if expandErr := dal.expandAllocationToPages(requiredPages); expandErr != nil {
			return nil, expandErr
		}
		if dal.size <= prevSize {
			return nil, ErrNoPagesLeft
		}
		pageNums, err = dal.freelist.GetConsecutivePageNumbers(count)
	}
	if err != nil {
		logger.Error("could not allocate page range", "count", count, "err", err)
		return nil, err
	}
	if count == 1 {
		logger.Debug("allocating pageNum number", "page_number", pageNums[0])
	} else {
		logger.Debug("allocating page range", "count", count, "start_page", pageNums[0], "end_page", pageNums[len(pageNums)-1])
	}
	return pageNums, nil
}

func (dal *Dal) ReleasePage(pageNumber uint64) error {
	return dal.ReleasePages([]uint64{pageNumber})
}

func (dal *Dal) ReleasePages(pageNumbers []uint64) error {
	if dal == nil || dal.freelist == nil || dal.meta == nil {
		return ErrDatabaseClosed
	}
	for _, pageNumber := range pageNumbers {
		if pageNumber < dal.freelist.firstDataPage || pageNumber == dal.meta.freelistPageNumber {
			return fmt.Errorf("cannot release reserved page %d", pageNumber)
		}
		if pageNumber == dal.meta.root {
			return fmt.Errorf("cannot release live catalog root page %d", pageNumber)
		}
		if pageNumber >= dal.maxPages {
			return fmt.Errorf("page number %d is greater than max page number %d", pageNumber, dal.maxPages)
		}
		if pageNumber > dal.freelist.currentPage {
			return fmt.Errorf("cannot release non-allocated page %d (current page %d)", pageNumber, dal.freelist.currentPage)
		}
		if dal.freelist.isFreelistStoragePage(pageNumber) {
			return fmt.Errorf("cannot release active freelist page %d", pageNumber)
		}
	}
	if dal.freelist.addReleasedPages(pageNumbers) {
		logger.Debug("releasing page batch", "page_count", len(pageNumbers))
	}
	return nil
}

func (dal *Dal) Close() error {
	if dal == nil {
		return nil
	}
	var closeErrs []error
	if dal.file != nil {
		if err := dal.file.Close(); err != nil && !errors.Is(err, fs.ErrClosed) {
			closeErrs = append(closeErrs, fmt.Errorf("failed to close file: %w", err))
		}
		dal.file = nil
	}
	if dal.txLog != nil {
		if err := dal.txLog.Close(); err != nil && !errors.Is(err, fs.ErrClosed) {
			closeErrs = append(closeErrs, fmt.Errorf("failed to close transaction log: %w", err))
		}
	}
	if dal.txLogLock != nil {
		if err := dal.txLogLock.Unlock(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("failed to unlock transaction log: %w", err))
		}
		dal.txLogLock = nil
	}
	if dal.fileLock != nil {
		if err := dal.fileLock.Unlock(); err != nil {
			closeErrs = append(closeErrs, fmt.Errorf("failed to unlock db file: %w", err))
		}
		dal.fileLock = nil
	}
	return errors.Join(closeErrs...)
}

func (dal *Dal) GetPage(pageNumber uint64) (*Page, error) {
	if dal == nil || dal.file == nil {
		return nil, ErrDatabaseClosed
	}
	if pageNumber >= dal.maxPages {
		return nil, fmt.Errorf("page number %d is greater than max page number %d", pageNumber, dal.maxPages)
	}
	if overlayPage, ok := dal.pageOverlay[pageNumber]; ok {
		page := clonePage(overlayPage)
		if err := verifyPageData(page.Data, dal.pageChecksums); err != nil {
			return nil, fmt.Errorf("verify overlay page %d: %w", pageNumber, err)
		}
		return page, nil
	}

	pageSize := dal.meta.pageSize
	if pageNumber > uint64(^uint64(0)>>1)/pageSize {
		return nil, fmt.Errorf("page number %d overflows file offset", pageNumber)
	}
	offset := int64(pageNumber * pageSize)

	data := make([]byte, pageSize)
	_, err := dal.file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageNumber, err)
	}
	if err = verifyPageData(data, dal.pageChecksums); err != nil {
		return nil, fmt.Errorf("verify page %d: %w", pageNumber, err)
	}

	page := &Page{
		PageNumber: pageNumber,
		Data:       data,
	}

	return page, nil
}

func (dal *Dal) SetPage(page *Page) error {
	return dal.setPage(page, true)
}

// setPage can borrow a materialized page while a transaction-log callback is
// active. The callback flushes before returning, so borrowing is safe and
// avoids one page-sized copy per journal entry.
func (dal *Dal) setPage(page *Page, copyForJournal bool) error {
	if dal == nil || dal.file == nil {
		return ErrDatabaseClosed
	}
	if page == nil {
		return fmt.Errorf("cannot write nil page")
	}
	if page.PageNumber >= dal.maxPages {
		return fmt.Errorf("page number %d is greater than max page number %d", page.PageNumber, dal.maxPages)
	}
	if uint64(len(page.Data)) != dal.meta.pageSize {
		return fmt.Errorf("page %d has %d bytes, expected %d", page.PageNumber, len(page.Data), dal.meta.pageSize)
	}
	if page.PageNumber > uint64(^uint64(0)>>1)/dal.meta.pageSize {
		return fmt.Errorf("page number %d overflows file offset", page.PageNumber)
	}
	if err := dal.preparePage(page); err != nil {
		return err
	}
	if dal.beforeSetPageHook != nil {
		if err := dal.beforeSetPageHook(page); err != nil {
			return err
		}
	}

	offset := page.PageNumber * dal.meta.pageSize

	if dal.txLog.active {
		if err := dal.txLog.writePageInternal(offset, page, copyForJournal); err != nil {
			return fmt.Errorf("failed to write pageNum %d to recovery log: %w", page.PageNumber, err)
		}
		return nil
	}

	_, err := dal.file.WriteAt(page.Data, int64(offset))
	if err != nil {
		return fmt.Errorf("failed to write pageNum %d to file: %w", page.PageNumber, err)
	}
	dal.nodeCache.invalidate(page.PageNumber)

	return nil
}

func (dal *Dal) Sync() error {
	if dal == nil || dal.file == nil {
		return ErrDatabaseClosed
	}
	return dal.file.Sync()
}

func (dal *Dal) getNode(pageNumber uint64) (*BNode, error) {
	return dal.getNodeWithCache(pageNumber, true)
}

func (dal *Dal) getNodeUncached(pageNumber uint64) (*BNode, error) {
	return dal.getNodeWithCache(pageNumber, false)
}

func (dal *Dal) getNodeWithCache(pageNumber uint64, useCache bool) (*BNode, error) {
	if err := dal.validateLiveDataPage(pageNumber); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorruptedNode, err)
	}
	_, overlay := dal.pageOverlay[pageNumber]
	if useCache && !overlay {
		if node, found := dal.nodeCache.get(pageNumber); found {
			return node, nil
		}
	}
	page, err := dal.GetPage(pageNumber)
	if err != nil {
		return nil, err
	}
	node := NewBNode()
	if err = node.deserializePage(page.Data, dal.pageChecksums); err != nil {
		return nil, fmt.Errorf("decode node page %d: %w", pageNumber, err)
	}
	node.PageNum = pageNumber
	for _, childPage := range node.childNodes {
		if childPage == pageNumber {
			return nil, fmt.Errorf("%w: node page %d references itself", ErrCorruptedNode, pageNumber)
		}
	}
	if useCache && !overlay {
		dal.nodeCache.put(pageNumber, node)
	}
	return node, nil
}

func (dal *Dal) validateLiveDataPage(pageNumber uint64) error {
	if dal == nil || dal.freelist == nil {
		return ErrDatabaseClosed
	}
	if pageNumber < dal.freelist.firstDataPage || pageNumber >= dal.maxPages || pageNumber > dal.freelist.currentPage ||
		dal.freelist.containsReleasedPage(pageNumber) || dal.freelist.isFreelistStoragePage(pageNumber) {
		return fmt.Errorf("page %d is not a live data page", pageNumber)
	}
	return nil
}

func (dal *Dal) setNode(node *BNode) (*Page, error) {
	page, err := dal.marshalNodePage(node)
	if err != nil {
		return nil, err
	}
	return page, dal.SetPage(page)
}

func (dal *Dal) marshalNodePage(node *BNode) (*Page, error) {
	var page *Page
	var err error
	if node.PageNum == 0 {
		page, err = dal.AllocatePage()
		if err != nil {
			return nil, err
		}
	} else {
		page = &Page{
			PageNumber: node.PageNum,
			Data:       make([]byte, dal.meta.pageSize),
		}
	}
	err = node.serializePage(page.Data, dal.pageChecksums)
	if err != nil {
		return nil, err
	}
	if err = dal.preparePage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// func (dal *Dal) deletePage(pageNumber uint64) {
// 	err := dal.ReleasePage(pageNumber)
// 	if err != nil {
// 		return
// 	}
// }

func (dal *Dal) maxThreshold() float32 {
	return dal.MaxFillPercent * float32(dal.usablePageSize())
}

func (dal *Dal) minThreshold() float32 {
	return dal.MinFillPercent * float32(dal.usablePageSize())
}
