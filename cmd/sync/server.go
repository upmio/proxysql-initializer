package sync

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/upmio/proxysql-initializer/apps/server"
)

var (
	serverType string
)

const (
	svcGroupNameEnvKey = "SERVICE_GROUP_NAME"
	namespaceEnvKey    = "NAMESPACE"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "sync mysql server",
	Long:  "sync mysql server",
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			svcGroupName, namespace string
			syncObj                 *server.ServerSync
			err                     error
			ctx, cancel             = context.WithTimeout(context.Background(), time.Second*10)
		)

		defer cancel()

		if svcGroupName = os.Getenv(svcGroupNameEnvKey); svcGroupName == "" {
			return fmt.Errorf("get %s environment variables fail", svcGroupNameEnvKey)
		}

		if namespace = os.Getenv(namespaceEnvKey); namespace == "" {
			return fmt.Errorf("get %s environment variables fail", namespaceEnvKey)
		}

		proxysqlDB, err := newDB(username, password, host, "main", port)
		if err != nil {
			return fmt.Errorf("create db connect fail, err: %v", err)
		}

		defer proxysqlDB.Close()

		switch serverType {
		case "mysql":
			syncObj, err = server.NewServerSync(proxysqlDB, namespace, svcGroupName)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("not support %s", serverType)
		}

		serverList, err := syncObj.GetServer(ctx, serverType)
		if err != nil {
			return err
		}

		fmt.Printf("Found mysql endpoint count %d\n", len(serverList))

		for i, v := range serverList {
			fmt.Printf("endpoint %d %v\n", i, v)
		}

		err = syncObj.CleanServer(ctx)
		if err != nil {
			return err
		}

		err = syncObj.LoadServer(ctx, serverList)
		if err != nil {
			return err
		}

		fmt.Println("sync success!")
		return nil
	},
}

func init() {
	serverCmd.PersistentFlags().StringVarP(&serverType, "server-type", "t", "mysql", "the proxysql-initializer sync type")
}
