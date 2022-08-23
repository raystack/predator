package metadata

import (
	"errors"
	"fmt"
	"github.com/coocood/freecache"
	gocache "github.com/eko/gocache/cache"
	"github.com/eko/gocache/store"
	"github.com/odpf/predator/protocol"
	"github.com/odpf/predator/protocol/meta"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"os"
	"sort"
	"time"
)

const defaultCacheSize = 100 * 1024 * 1024 //100 MB

var logger = log.New(os.Stdout, "INFO: ", log.Lshortfile|log.LstdFlags)

type FieldCache struct {
	Name      string
	ID        string
	FieldType meta.FieldType
	Mode      meta.Mode

	Level    int
	ParentID string
}

func newFieldCache(fs *meta.FieldSpec) *FieldCache {
	field := FieldCache{
		Name:      fs.Name,
		ID:        fs.ID(),
		FieldType: fs.FieldType,
		Mode:      fs.Mode,
		Level:     fs.Level,
	}
	if fs.Parent != nil {
		field.ParentID = fs.Parent.ID()
	}
	return &field
}

type TableCache struct {
	ProjectName            string
	DatasetName            string
	TableName              string
	PartitionField         string
	RequirePartitionFilter bool

	TimePartitioningType meta.TimePartitioning

	Labels map[string]string
	Fields []*FieldCache
}

func newTableCache(tableSpec *meta.TableSpec) *TableCache {
	fieldSpecs := tableSpec.FieldsFlatten()

	var fs []*FieldCache
	for _, spec := range fieldSpecs {
		s := newFieldCache(spec)
		fs = append(fs, s)
	}

	return &TableCache{
		ProjectName:            tableSpec.ProjectName,
		DatasetName:            tableSpec.DatasetName,
		TableName:              tableSpec.TableName,
		PartitionField:         tableSpec.PartitionField,
		RequirePartitionFilter: tableSpec.RequirePartitionFilter,
		TimePartitioningType:   tableSpec.TimePartitioningType,
		Labels:                 tableSpec.Labels,
		Fields:                 fs,
	}
}

func (s *TableCache) toTableSpec() *meta.TableSpec {
	parentMap := make(map[string]string)
	for _, field := range s.Fields {
		if field.ParentID != "" {
			parentMap[field.ID] = field.ParentID
		}
	}

	childMap := make(map[string][]string)
	for _, field := range s.Fields {
		if field.ParentID != "" {
			childMap[field.ParentID] = append(childMap[field.ParentID], field.ID)
		}
	}

	fsMap := make(map[string]*meta.FieldSpec)
	for _, field := range s.Fields {
		fs := &meta.FieldSpec{
			Name:      field.Name,
			FieldType: field.FieldType,
			Mode:      field.Mode,
			Level:     field.Level,
			Parent:    nil,
			Fields:    nil,
		}
		fsMap[field.ID] = fs
	}

	for ID, fieldSpec := range fsMap {
		var p *meta.FieldSpec
		parentID := parentMap[ID]
		p = fsMap[parentID]
		fieldSpec.Parent = p

		var children []*meta.FieldSpec
		childIDs := childMap[ID]
		for _, childID := range childIDs {
			fs := fsMap[childID]
			if fs != nil {
				children = append(children, fs)
			}
		}
		fieldSpec.Fields = children
	}

	var flattenFS []*meta.FieldSpec
	for _, fieldSpec := range fsMap {
		flattenFS = append(flattenFS, fieldSpec)
	}

	sort.Sort(byID(flattenFS))

	var nestedFS []*meta.FieldSpec
	for _, fieldSpec := range flattenFS {
		if fieldSpec.Parent == nil {
			nestedFS = append(nestedFS, fieldSpec)
		}
	}

	return &meta.TableSpec{
		ProjectName:            s.ProjectName,
		DatasetName:            s.DatasetName,
		TableName:              s.TableName,
		PartitionField:         s.PartitionField,
		RequirePartitionFilter: s.RequirePartitionFilter,
		TimePartitioningType:   s.TimePartitioningType,
		Labels:                 s.Labels,
		Fields:                 nestedFS,
	}
}

type byID []*meta.FieldSpec

func (b byID) Len() int {
	return len(b)
}

func (b byID) Less(i, j int) bool {
	return b[i].Name < b[j].Name
}

func (b byID) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

//CachedStore store that get metadata from cache
type CachedStore struct {
	cache  gocache.CacheInterface
	source protocol.MetadataStore
}

func (c *CachedStore) GetMetadata(urn string) (*meta.TableSpec, error) {
	cacheValue, err := c.cache.Get(urn)
	if err != nil {
		return nil, err
	}

	var table TableCache

	err = msgpack.Unmarshal(cacheValue.([]byte), &table)
	if err != nil {
		return nil, err
	}

	return table.toTableSpec(), nil
}

func (c *CachedStore) GetUniqueConstraints(urn string) ([]string, error) {
	return c.source.GetUniqueConstraints(urn)
}

func NewCachedStore(cacheExpirationSeconds int, source protocol.MetadataStore) *CachedStore {
	freeCacheClient := freecache.NewCache(defaultCacheSize)
	freeCacheStore := store.NewFreecache(freeCacheClient, &store.Options{
		Expiration: time.Duration(cacheExpirationSeconds) * time.Second,
	})

	loadFunc := func(key interface{}) (interface{}, error) {
		urn, ok := key.(string)
		if !ok {
			return nil, errors.New("wrong data type of cache key")
		}

		tableSpec, err := source.GetMetadata(urn)
		if err != nil {
			return nil, err
		}

		logger.Println(fmt.Sprintf("load table metadata: %s", urn))
		tbl := newTableCache(tableSpec)
		return msgpack.Marshal(tbl)
	}

	cacheManager := gocache.New(freeCacheStore)
	return &CachedStore{
		source: source,
		cache:  gocache.NewLoadable(loadFunc, cacheManager),
	}
}
