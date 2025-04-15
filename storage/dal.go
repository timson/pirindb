package storage

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io/fs"
	"os"
	"path/filepath"
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
	txLog             *TxLog
	opts              *Options
	beforeSetPageHook func(p *Page) error
}

func NewDal(path string, opts *Options) (*Dal, error) {
	var fileExists bool

	if opts.TxLogPath == "" {
		ext := filepath.Ext(path)
		opts.TxLogPath = strings.TrimSuffix(path, ext) + ".tlog"
	}

	if _, statErr := os.Stat(path); statErr == nil {
		fileExists = true
		logger.Debug("database file already exists", "path", path)
	} else if os.IsNotExist(statErr) {
		fileExists = false
		logger.Debug("database file not exists", "path", path)
	} else {
		return nil, fmt.Errorf("could not stat dal: %v", statErr)
	}

	fileLock := flock.New(path)
	locked, err := fileLock.TryLock()
	if err != nil || !locked {
		return nil, fmt.Errorf("could not lock database file: %s", path)
	}

	file, openErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, opts.FileMode)
	if openErr != nil {
		return nil, fmt.Errorf("could not open dal: %v", openErr)
	}

	fileInfo, fileStatErr := file.Stat()
	if fileStatErr != nil {
		_ = file.Close()
		return nil, fmt.Errorf("could not stat dal: %v", fileStatErr)
	}

	fileSize := fileInfo.Size()
	if fileSize < minFileSize {
		fileSize = minFileSize
	}

	tlog := NewTxLog(opts.TxLogPath, 0600)
	logger.Info("open database file", "path", path, "size", fileSize,
		"tx_log", opts.TxLogPath)

	dal := &Dal{
		fileLock:       fileLock,
		file:           file,
		meta:           NewMeta(opts.PageSize),
		osPageSize:     uint64(os.Getpagesize()),
		freelist:       NewFreelist(BTreePageSize, 0),
		MinFillPercent: 0.45,
		MaxFillPercent: 0.95,
		txLog:          tlog,
		opts:           opts,
	}
	dal.allocateFile(uint64(fileSize))

	if fileExists {
		if opts.EnableRecovery {
			recoveredPages := 0
			err = tlog.Recover(func(offset uint64, page *Page) error {
				err = dal.SetPage(page)
				if err != nil {
					return err
				}
				recoveredPages++
				return nil
			})
			if err != nil {
				logger.Error("unable to apply tx log", "error", err)
			} else {
				logger.Info("tx log applied", "recovered_pages", recoveredPages)
			}
		}
		meta, readMetaErr := ReadMeta(dal)
		if readMetaErr != nil {
			_ = dal.file.Close()
			return nil, fmt.Errorf("could not read meta: %v", readMetaErr)
		}
		dal.meta = meta
		freelist, readFreelistErr := ReadFreelist(dal)
		if readFreelistErr != nil {
			_ = dal.file.Close()
			return nil, fmt.Errorf("could not read freelist: %v", readFreelistErr)
		}
		dal.freelist = freelist
	} else {
		writeMetaErr := WriteMeta(dal, dal.meta)
		if writeMetaErr != nil {
			_ = dal.file.Close()
			return nil, fmt.Errorf("could not write meta: %v", writeMetaErr)
		}
		writeFreelistErr := WriteFreelist(dal, dal.freelist)
		if writeFreelistErr != nil {
			_ = dal.file.Close()
			return nil, fmt.Errorf("could not write freelist: %v", writeFreelistErr)
		}
	}

	return dal, nil
}

func (dal *Dal) allocateFile(size uint64) {
	err := dal.file.Truncate(int64(size))
	if err != nil {
		panic(err)
	}
	dal.size = size
	dal.maxPages = size / dal.meta.pageSize
	dal.freelist.maxPages = dal.maxPages
	logger.Info("allocateFile", "size", dal.size, "max_pages", dal.maxPages)
}

func (dal *Dal) expandAllocation() {
	var newSize uint64
	if dal.size < OneGigabyte {
		newSize = dal.size * 2
	} else {
		newSize = dal.size + OneGigabyte
	}
	logger.Info("expand allocateFile", "size", newSize)
	dal.allocateFile(newSize)
}

func (dal *Dal) AllocatePage() (*Page, error) {
	var page *Page
	newPageNum, err := dal.freelist.GetNextPageNumber()
	if err != nil {
		if errors.Is(err, ErrNoPagesLeft) {
			logger.Debug("trying allocate new pageNum, but no pages left")
			// if no free pages left, we should allocate new pages
			// by expand database file and expand mapping
			dal.expandAllocation()
			newPageNum, err = dal.freelist.GetNextPageNumber()
			if err != nil {
				return nil, err
			}
		} else {
			logger.Error("could not get next pageNum number", "err", err)
			return nil, err
		}
	} else {
		logger.Debug("allocating pageNum number", "page_number", newPageNum)
	}
	page, err = dal.GetPage(newPageNum)
	if err != nil {
		return nil, err
	}
	page.Clear()
	return page, nil
}

func (dal *Dal) ReleasePage(pageNumber uint64) error {
	if pageNumber == 0 {
		return fmt.Errorf("cannot release pageNum 0")
	}
	dal.freelist.ReleasePage(pageNumber)
	return nil
}

func (dal *Dal) Close() error {
	if dal.file == nil {
		return nil
	}

	if err := dal.file.Close(); err != nil && !errors.Is(err, fs.ErrClosed) {
		return fmt.Errorf("failed to close file: %w", err)
	}
	err := dal.fileLock.Unlock()
	if err != nil {
		return fmt.Errorf("failed to unlock db file: %w", err)
	}
	dal.file = nil
	return nil
}

func (dal *Dal) GetPage(pageNumber uint64) (*Page, error) {
	if pageNumber >= dal.maxPages {
		return nil, fmt.Errorf("page number %d is greater than max page number %d", pageNumber, dal.maxPages)
	}

	pageSize := dal.meta.pageSize
	offset := int64(pageNumber * pageSize)

	data := make([]byte, pageSize)
	_, err := dal.file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageNumber, err)
	}

	page := &Page{
		PageNumber: pageNumber,
		Data:       data,
	}

	return page, nil
}

func (dal *Dal) SetPage(page *Page) error {
	if dal.beforeSetPageHook != nil {
		if err := dal.beforeSetPageHook(page); err != nil {
			return err
		}
	}

	offset := page.PageNumber * dal.meta.pageSize

	if dal.txLog.active {
		if err := dal.txLog.writePage(offset, page); err != nil {
			return fmt.Errorf("failed to write pageNum %d to recovery log: %w", page.PageNumber, err)
		}
		return nil
	}

	_, err := dal.file.WriteAt(page.Data, int64(offset))
	if err != nil {
		return fmt.Errorf("failed to write pageNum %d to file: %w", page.PageNumber, err)
	}

	return nil
}

func (dal *Dal) Sync() error {
	return dal.file.Sync()
}

func (dal *Dal) getNode(pageNumber uint64) (*BNode, error) {
	page, err := dal.GetPage(pageNumber)
	if err != nil {
		return nil, err
	}
	node := NewBNode()
	node.Deserialize(page.Data)
	node.PageNum = pageNumber
	return node, nil
}

func (dal *Dal) setNode(node *BNode) (*Page, error) {
	var page *Page
	var err error
	if node.PageNum == 0 {
		page, err = dal.AllocatePage()
		if err != nil {
			return nil, err
		}
	} else {
		page, err = dal.GetPage(node.PageNum)
		if err != nil {
			return nil, err
		}
	}
	err = node.Serialize(page.Data)
	if err != nil {
		return nil, err
	}
	return page, dal.SetPage(page)
}

func (dal *Dal) deletePage(pageNumber uint64) {
	err := dal.ReleasePage(pageNumber)
	if err != nil {
		return
	}
}

func (dal *Dal) maxThreshold() float32 {
	return dal.MaxFillPercent * float32(dal.meta.pageSize)
}

func (dal *Dal) minThreshold() float32 {
	return dal.MinFillPercent * float32(dal.meta.pageSize)
}
