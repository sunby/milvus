package catalog

type CatalogSet interface {
	GetCatalogEntry(name string) CatalogEntry
	AddCatalogEntry(entry CatalogEntry)
}

var _ CatalogSet = &CatalogSetImpl{}

type CatalogSetImpl struct {
	entries []CatalogEntry
}

func (s *CatalogSetImpl) GetCatalogEntry(name string) CatalogEntry {
	for _, entry := range s.entries {
		if entry.Name() == name {
			return entry
		}
	}
	return nil
}

func (s *CatalogSetImpl) AddCatalogEntry(entry CatalogEntry) {
	s.entries = append(s.entries, entry)
}
