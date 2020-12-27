package cmd

import (
	"github.com/leftmike/maho/repl"
	"github.com/spf13/cobra"
)

var (
	replCmd = &cobra.Command{
		Use:   "repl",
		Short: "Run with an interactive console session",
		RunE:  replRun,
	}
)

func init() {
	initServerFlags(replCmd.Flags())

	mahoCmd.AddCommand(replCmd)
}

func replRun(cmd *cobra.Command, args []string) error {
	svr, err := newServer(args)
	if err != nil {
		return err
	}

	if len(args) == 0 && len(sqlArgs) == 0 {
		svr.HandleSession(repl.Interact(), "startup", "console", "")
	}
	return nil
}
