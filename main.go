package main

/*
To Do:

maho/sql/stmt:

type Executer { // implemented by Engine
    CreateTable(stmt stmt.CreateTable) (interface{}, error)
    InsertValues(stmt stmt.InsertValues) (Result, error)
    SelectAll(stmt stmt.SelectAll) (store.Rows, error)
}

type Dispatcher { // implemented by Statements
    Dispatch(e Executer) (interface{}, error)
}

func (stmt *CreateTable) Dispatch(e Executer) {
    return e.CreateTable(stmt)
}

maho/main:

    stmt,err := p.Parse()
    ret, err := stmt.Dispatch(eng)
    if rows, ok := ret.(store.Rows); ok {
        // display rows
    } else if res, ok := ret.(store.Result); ok {
        // display result
    }

maho/engine:

func (e *Engine) CreateTable(stmt stmt.CreateTable) (interface{}, error)

*/

import (
	"bufio"
	"fmt"
	"io"
	"maho/engine"
	"maho/sql"
	"maho/sql/parser"
	"os"
	"strings"
	"text/tabwriter"
)

func parse(rr io.RuneReader, fn string) {
	var p parser.Parser
	p.Init(rr, fn)

	for {
		stmt, err := p.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}

		err = engine.Execute(stmt)
		if err == engine.CanNotBeExecuted {
			rows, err := engine.Query(stmt)
			if err != nil {
				fmt.Println(err)
				break
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.AlignRight)

			cols := rows.Columns()
			fmt.Fprint(w, "\t")
			for _, col := range cols {
				fmt.Fprintf(w, "%s\t", col.Name)
			}
			fmt.Fprint(w, "\n\t")
			for _, col := range cols {
				fmt.Fprintf(w, "%s\t", strings.Repeat("-", len(col.Name.String())))

			}
			fmt.Fprintln(w)

			dest := make([]sql.Value, len(cols))
			for i := 1; rows.Next(dest) == nil; i += 1 {
				fmt.Fprintf(w, "%d\t", i)
				for _, v := range dest {
					fmt.Fprintf(w, "%s\t", sql.FormatValue(v))
				}
				fmt.Fprintln(w)
			}
			w.Flush()
		} else if err != nil {
			fmt.Println(err)
			break
		}
	}
}

func main() {
	err := engine.Start(sql.QuotedId("maho"), "maho")
	if err != nil {
		fmt.Println(err)
		return
	}

	if len(os.Args) == 1 {
		parse(bufio.NewReader(os.Stdin), "[Stdin]")
	} else {
		for idx := 1; idx < len(os.Args); idx++ {
			/*			f, err := os.Open(os.Args[idx])
						if err != nil {
							log.Fatal(err)
						}
						parse(e, bufio.NewReader(f), os.Args[idx])*/
			parse(strings.NewReader(os.Args[idx]), fmt.Sprintf("os.Args[%d]", idx))
		}
	}
}
