package storage

const (
	MetaPage     = 0
	FreeListPage = 1
	NodePage     = 2
	BlobPage     = 3
)

type Page struct {
	PageNumber uint64
	Data       []byte
}

func (p *Page) Clear() {
	for i := range p.Data {
		p.Data[i] = 0
	}
}
