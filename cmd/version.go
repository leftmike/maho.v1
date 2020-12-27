package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/leftmike/maho/sql"
)

func init() {
	mahoCmd.AddCommand(
		&cobra.Command{
			Use:   "version",
			Short: "Print the version number of Maho",
			Run: func(cmd *cobra.Command, args []string) {
				fmt.Println(sql.Version())
			},
		})
}
