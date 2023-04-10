package parquet

import (
	"bytes"
	"errors"
	"io"
	"time"

	store "github.com/blevesearch/upsidedown_store_api"
	"github.com/segmentio/parquet-go"
)

type Reader struct {
	s *Store
}

// prefixCompare will return 0 if a is a prefix of b,
// otherwise it will perform a normal byte compare.
func prefixCompare(a, b parquet.Value) int {
	aa := a.ByteArray()
	bb := b.ByteArray()

	cmp := bytes.Compare(aa, bb)
	if cmp < 0 {
		if bytes.HasPrefix(bb, aa) {
			return 0
		}
	}

	return cmp
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	// we assume there's just one rowgroup
	rowGroup := r.s.pf.RowGroups()[0]
	keyColChunk := rowGroup.ColumnChunks()[0]

	prefixCompare := func(a, b parquet.Value) int {
		aa := a.ByteArray()
		bb := b.ByteArray()

		cmp := bytes.Compare(aa, bb)
		if cmp < 0 {
			if bytes.HasPrefix(bb, aa) {
				return 0
			}
		}

		return cmp
	}

	found := parquet.Find(keyColChunk.ColumnIndex(), parquet.ValueOf(key), prefixCompare)
	offsetIndex := keyColChunk.OffsetIndex()
	if found >= offsetIndex.NumPages() {
		return nil, nil
	}
	reader := parquet.NewGenericReader[KV](r.s.pf)
	defer reader.Close()
	// Seek to the first row in the page the result was found
	reader.SeekToRow(offsetIndex.FirstRowIndex(found))
	result := make([]KV, 1)
	for {
		_, err := reader.Read(result)
		if err == io.EOF {
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		cmp := bytes.Compare(result[0].K, key)
		if cmp == 0 {
			return result[0].V, nil
		} else if cmp > 0 {
			return nil, nil
		}
	}
}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	result := make([][]byte, len(keys))
	for i, key := range keys {
		v, err := r.Get(key)
		if err != nil {
			return nil, err
		}
		result[i] = v
	}
	return result, nil
}

func (r *Reader) PrefixIterator(prefix []byte) store.KVIterator {
	rv := Iterator{
		s:      r.s,
		prefix: prefix,
	}
	rv.Seek(prefix)
	return &rv
}

func (r *Reader) RangeIterator(start, end []byte) store.KVIterator {
	rv := Iterator{
		s:     r.s,
		start: start,
		end:   end,
	}
	rv.Seek(start)
	return &rv
}

func (r *Reader) Close() error {
	return nil
}

type Iterator struct {
	s             *Store
	reader        *parquet.GenericReader[KV]
	seekCallCount int
	nextCount     int
	t0            time.Time

	currK []byte
	currV []byte
	err   error

	prefix []byte
	start  []byte
	end    []byte
}

var errIterDone = errors.New("iter done")

func (i *Iterator) Seek(key []byte) {
	t0 := time.Now()
	i.t0 = t0
	i.seekCallCount++
	if i.err != nil {
		return
	}

	// if key < start, advance key to start
	if i.start != nil && bytes.Compare(key, i.start) < 0 {
		key = i.start
	}
	if i.prefix != nil && !bytes.HasPrefix(key, i.prefix) {
		if bytes.Compare(key, i.prefix) < 0 {
			// if key < prefix; advance key to prefix
			key = i.prefix
		} else {
			var end []byte
			for x := len(i.prefix) - 1; x >= 0; x-- {
				c := i.prefix[x]
				if c < 0xff {
					end = make([]byte, x+1)
					copy(end, i.prefix)
					end[x] = c + 1
					break
				}
			}
			key = end
		}
	}

	// we assume there's just one rowgroup
	rowGroup := i.s.pf.RowGroups()[0]
	keyColChunk := rowGroup.ColumnChunks()[0]

	found := parquet.Find(keyColChunk.ColumnIndex(), parquet.ValueOf(key), prefixCompare)
	offsetIndex := keyColChunk.OffsetIndex()
	if found >= offsetIndex.NumPages() {
		i.err = errIterDone
		return
	}
	reader := parquet.NewGenericReader[KV](i.s.pf)
	i.reader = reader
	reader.SeekToRow(offsetIndex.FirstRowIndex(found))
	result := make([]KV, 1)
	for {
		_, err := reader.Read(result)
		if err == io.EOF {
			i.err = errIterDone
			return
		} else if err != nil {
			i.err = err
			return
		}

		cmp := bytes.Compare(result[0].K, key)
		if cmp < 0 {
			continue
		}

		i.currK = result[0].K
		i.currV = result[0].V
		break
	}
}

func (i *Iterator) Next() {
	if i.err != nil {
		return
	}
	i.nextCount++
	result := make([]KV, 1)
	_, err := i.reader.Read(result)
	if err == io.EOF {
		i.err = errIterDone
		return
	} else if err != nil {
		i.err = err
		return
	}

	if i.prefix != nil && !bytes.HasPrefix(result[0].K, i.prefix) {
		i.currK = []byte{}
		i.currV = []byte{}
	} else if i.end != nil && bytes.Compare(result[0].K, i.end) >= 0 {
		i.currK = []byte{}
		i.currV = []byte{}
	} else {
		i.currK = result[0].K
		i.currV = result[0].V
	}
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *Iterator) Key() []byte {
	return i.currK
}

func (i *Iterator) Value() []byte {
	return i.currV
}

func (i *Iterator) Valid() bool {
	if i.err != nil {
		return false
	} else if i.prefix != nil && !bytes.HasPrefix(i.currK, i.prefix) {
		return false
	} else if i.end != nil && bytes.Compare(i.currK, i.end) >= 0 {
		return false
	}
	return true
}

func (i *Iterator) Close() error {
	if i.reader != nil {
		i.reader.Close()
		i.reader = nil
	}
	return nil
}
