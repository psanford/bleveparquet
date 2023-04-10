package parquet

import (
	"fmt"
	"io"
	"os"

	"github.com/blevesearch/bleve/v2/registry"
	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/segmentio/parquet-go"
)

const Name = "parquet"

type Store struct {
	mo store.MergeOperator
	pf *parquet.File
}

func ConfigForReader(r io.ReaderAt, size int) map[string]interface{} {
	conf := make(map[string]interface{})
	conf["parquet_reader"] = r
	conf["parquet_size"] = size
	return conf
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	var (
		r    io.ReaderAt
		size int64
	)

	confReaderI, ok := config["parquet_reader"]
	if ok {
		r = confReaderI.(io.ReaderAt)
		size = int64(config["parquet_size"].(int))
	} else {
		path, ok := config["path"].(string)
		if !ok {
			return nil, fmt.Errorf("must specify path")
		}
		if path == "" {
			return nil, fmt.Errorf("must specify path")
		}

		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open file err: %w", err)
		}

		size, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("seek end err: %w", err)
		}
		f.Seek(0, io.SeekStart)
		r = f
	}

	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("parquet open file err: %w", err)
	}

	s := &Store{
		mo: mo,
		pf: file,
	}
	return s, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Reader() (store.KVReader, error) {
	return &Reader{s: s}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	return &Writer{s: s}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
