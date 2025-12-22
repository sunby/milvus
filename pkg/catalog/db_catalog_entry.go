package catalog

var _ CatalogEntry = &DBCatalogEntry{}

type DBCatalogEntry struct {
	name string
}

func (e *DBCatalogEntry) Name() string {
	return e.name
}

func (e *DBCatalogEntry) Type() CatalogEntryType {
	return CatalogEntryTypeDatabase
}
