package sync

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/upmio/proxysql-initializer/apps/server"
	"go.uber.org/zap"
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
			return fmt.Errorf("get environment variables %s failed", svcGroupNameEnvKey)
		}

		if namespace = os.Getenv(namespaceEnvKey); namespace == "" {
			return fmt.Errorf("get environment variables %s failed", namespaceEnvKey)
		}

		proxysqlDB, err := newDB(username, password, host, "main", port)
		if err != nil {
			return fmt.Errorf("generate db connect fail, err: %v", err)
		}

		defer proxysqlDB.Close()

		logger, _ := zap.NewDevelopment()
		slogger := logger.Sugar()

		switch serverType {
		case "mysql":
			syncObj, err = server.NewServerSync(proxysqlDB, slogger, namespace, svcGroupName)
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

		err = syncObj.CleanServer(ctx)
		if err != nil {
			return err
		}

		return syncObj.LoadServer(ctx, serverList)
	},
}

func init() {
	serverCmd.PersistentFlags().StringVarP(&serverType, "server-type", "t", "mysql", "the proxysql-initializer sync type")
}
