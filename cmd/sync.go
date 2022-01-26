package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	sync "github.com/upmio/proxysql-initializer/apps/sync"
)

var (
	servertype, username, password, host string
	port                                 int
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "use for sync mysql server",
	Long:  "use for sync mysql server",
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			syncObj     *sync.ServerSync
			err         error
			ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
		)
		defer cancel()
		switch servertype {
		case "mysql":
			syncObj, err = sync.NewServerSync(username, password, host, "mysql", port)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("not support %s", servertype)
		}

		serverList, err := syncObj.GetMysqlServerFromKubeByLabel(ctx)
		if err != nil {
			return err
		}

		err = syncObj.LoadMysqlServerToRuntime(ctx, serverList)
		if err != nil {
			return err
		}

		fmt.Println("sync success!")
		return nil
	},
}

func init() {
	syncCmd.PersistentFlags().StringVarP(&servertype, "server-type", "t", "mysql", "the proxysql-initializer sync type")
	syncCmd.PersistentFlags().StringVarP(&username, "proxysql-username", "u", "admin", "the proxysql-initializer sync user")
	syncCmd.PersistentFlags().StringVarP(&password, "proxysql-password", "p", "", "the proxysql-initializer sync password")
	syncCmd.PersistentFlags().StringVarP(&host, "proxysql-server", "s", "127.0.0.1", "the proxysql-initializer sync host")
	syncCmd.PersistentFlags().IntVarP(&port, "proxysql-port", "P", 6032, "the proxysql-initializer sync port")
	RootCmd.AddCommand(syncCmd)
}
