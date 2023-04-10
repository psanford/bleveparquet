package parquet

import (
	"errors"

	store "github.com/blevesearch/upsidedown_store_api"
)

type Writer struct {
	s *Store
}

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.s.mo)
}

var notImplementedErr = errors.New("not implemented")

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return nil, nil, nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) error {
	return notImplementedErr
}

func (w *Writer) Close() error {
	return nil
}
