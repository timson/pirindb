package storage

import (
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"golang.org/x/sys/unix"
	"io/fs"
	"os"
)

const (
	minFileSize = 1024 * 32 // 32KB
	OneGigabyte = 1024 * 1024 * 1024
)

type Dal struct {
	file           *os.File
	data           []byte
	osPageSize     uint64
	maxPages       uint64
	size           uint64
	MinFillPercent float32
	MaxFillPercent float32
	freelist       *Freelist
	meta           *Meta
	fileLock       *flock.Flock
}

func NewDal(path string, mode os.FileMode, pageSize uint64) (*Dal, error) {
	var fileExists bool

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

	file, openErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, mode)
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

	logger.Info("open database file", "path", path, "size", fileSize)

	dal := &Dal{
		fileLock:       fileLock,
		file:           file,
		meta:           NewMeta(pageSize),
		osPageSize:     uint64(os.Getpagesize()),
		freelist:       NewFreelist(BTreePageSize, 0),
		MinFillPercent: 0.45,
		MaxFillPercent: 0.95,
	}
	dal.mmap(uint64(fileSize))

	if fileExists {
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

func (dal *Dal) mmap(size uint64) {
	if dal.size != 0 {
		logger.Info("unmapping dal", "old_size", dal.size, "addr", fmt.Sprintf("%p", dal.data), "new_size", size)
		err := unix.Munmap(dal.data)
		if err != nil {
			panic(err)
		}
	}
	err := dal.file.Truncate(int64(size))
	if err != nil {
		panic(err)
	}
	data, mmapErr := unix.Mmap(int(dal.file.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if mmapErr != nil {
		panic(mmapErr)
	}
	dal.data = data
	dal.size = size
	dal.maxPages = size / dal.meta.pageSize
	dal.freelist.maxPages = dal.maxPages
	logger.Info("mmap", "size", dal.size, "max_pages", dal.maxPages, "addr", fmt.Sprintf("%p", dal.data))
}

func (dal *Dal) expandMapping() {
	var newSize uint64
	if dal.size < OneGigabyte {
		newSize = dal.size * 2
	} else {
		newSize = dal.size + OneGigabyte
	}
	logger.Info("expand mmap", "size", newSize)
	dal.mmap(newSize)
}

func (dal *Dal) AllocatePage() (*Page, error) {
	var page *Page
	newPageNum, err := dal.freelist.GetNextPageNumber()
	if err != nil {
		if errors.Is(err, ErrNoPagesLeft) {
			logger.Debug("trying allocate new pageNum, but no pages left")
			// if no free pages left, we should allocate new pages
			// by expand database file and expand mapping
			dal.expandMapping()
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
	err := unix.Munmap(dal.data)
	if err != nil {
		fmt.Printf("failed to unmap file: %v\n", err)
	}
	if err = dal.file.Close(); err != nil && !errors.Is(err, fs.ErrClosed) {
		return fmt.Errorf("failed to close file: %w", err)
	}
	err = dal.fileLock.Unlock()
	if err != nil {
		return fmt.Errorf("failed to unlock db file: %w", err)
	}
	dal.file = nil
	return nil
}

func (dal *Dal) GetPage(pageNumber uint64) (*Page, error) {
	if pageNumber >= dal.maxPages {
		return nil, fmt.Errorf("pageNum number %d is greater than max pageNum number %d", pageNumber, dal.maxPages)
	}

	offset := pageNumber * dal.meta.pageSize
	end := offset + dal.meta.pageSize

	page := Page{
		PageNumber: pageNumber,
		Data:       dal.data[offset:end],
	}

	return &page, nil
}

func (dal *Dal) SetPage(page *Page) error {
	offset := page.PageNumber * dal.meta.pageSize
	alignedOffset := offset - (offset % dal.osPageSize)
	err := unix.Msync(dal.data[alignedOffset:alignedOffset+dal.osPageSize], unix.MS_SYNC)
	if err != nil {
		return fmt.Errorf("failed to sync pageNum %d: %w", page.PageNumber, err)
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
