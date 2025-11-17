package main

import (
	"opti-sql-go/config"
	QueryExecuter "opti-sql-go/substrait"
	"os"
)

// TODO: in the project operators make sure the record batches account for the RowCount field properly.

func main() {
	if len(os.Args) > 1 {
		if err := config.Decode(os.Args[1]); err != nil {
			panic(err)
		}
	}
	<-QueryExecuter.Start()
	os.Exit(0)
}
