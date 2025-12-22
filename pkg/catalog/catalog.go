package catalog

type Catalog interface{}

var _ Catalog = &CatalogImpl{}

type CatalogImpl struct{}
