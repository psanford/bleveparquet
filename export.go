package parquet

import (
	"fmt"
	"io"

	"github.com/blevesearch/bleve/v2/index/upsidedown"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/segmentio/parquet-go"
)

type KV struct {
	K []byte
	V []byte
}

func Export(i index.Index, output io.Writer) error {
	writer := parquet.NewGenericWriter[KV](output)

	r, err := i.Reader()
	if err != nil {
		return fmt.Errorf("error getting reader: %v", err)
	}

	upsideDownReader, ok := r.(*upsidedown.IndexReader)
	if !ok {
		return fmt.Errorf("dump is only supported by index type upsidedown")
	}

	// var prevByte byte

	var dumpChan chan interface{}
	dumpChan = upsideDownReader.DumpAll()

	for dumpValue := range dumpChan {
		switch dumpValue := dumpValue.(type) {
		case upsidedown.IndexRow:
			k := dumpValue.Key()
			// instead of 1 row group we could split by leading byte
			// if len(k) > 0 && k[0] != prevByte {
			// 	writer.Flush()
			// }
			p := KV{K: k, V: dumpValue.Value()}
			_, err := writer.Write([]KV{p})
			if err != nil {
				return fmt.Errorf("error writing row: %v", err)
			}

		case error:
			return fmt.Errorf("error dumping row: %v", dumpValue)
		}
	}

	return writer.Close()
}
