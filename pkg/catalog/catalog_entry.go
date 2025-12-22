package catalog

type CatalogEntryType uint16

const (
	CatalogEntryTypeDatabase CatalogEntryType = iota
	CatalogEntryTypeCollection
	CatalogEntryTypePartition
)

type CatalogEntry interface {
	Name() string
	Type() CatalogEntryType
}
