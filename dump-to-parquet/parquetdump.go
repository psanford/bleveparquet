package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/blevesearch/bleve/v2"
	_ "github.com/blevesearch/bleve/v2/index/upsidedown/store/goleveldb"
	parquet "github.com/psanford/bleveparquet"
)

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		log.Fatalf("usage: %s <db/path> <dst>", os.Args[0])
	}

	index, err := bleve.Open(args[0])
	if err != nil {
		panic(err)
	}

	err = os.Mkdir(args[1], 0755)
	if err != nil {
		panic(err)
	}

	outFile, err := os.Create(filepath.Join(args[1], "store"))
	if err != nil {
		panic(err)
	}

	ii, err := index.Advanced()
	if err != nil {
		panic(err)
	}

	err = parquet.Export(ii, outFile)
	if err != nil {
		panic(err)
	}

	indexMeta := `{"storage":"parquet","index_type":"upside_down"}`
	err = ioutil.WriteFile(filepath.Join(args[1], "index_meta.json"), []byte(indexMeta), 0644)
	if err != nil {
		panic(err)
	}
}
