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

func (p *Page) GetPageType() string {
	if len(p.Data) == 0 {
		return "Unknown"
	}

	switch p.Data[0] {
	case MetaPage:
		return "Meta"
	case FreeListPage:
		return "Freelist"
	case NodePage:
		return "Node"
	case BlobPage:
		return "Blob"
	default:
		return "Unknown"
	}
}
