package sync

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/upmio/proxysql-initializer/apps/server"
	"go.uber.org/zap"
)

var (
	serverType                   string
	rwHostGroupId, roHostGroupId int
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

		defer func(proxysqlDB *sql.DB) {
			err := proxysqlDB.Close()
			if err != nil {
				fmt.Printf("close proxysql db fail, err: %v", err)
			}
		}(proxysqlDB)

		logger, _ := zap.NewDevelopment()
		slogger := logger.Sugar()

		switch serverType {
		case "mysql":
			syncObj, err = server.NewServerSync(proxysqlDB, slogger, namespace, svcGroupName, rwHostGroupId, roHostGroupId)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("not support %s", serverType)
		}

		serverList, err := syncObj.GetServerFromK8s(ctx, serverType)
		if err != nil {
			return err
		}

		return syncObj.SyncServerToProxy(ctx, serverList)
	},
}

func init() {
	serverCmd.PersistentFlags().StringVarP(&serverType, "server-type", "t", "mysql", "the proxysql-initializer sync type")
	serverCmd.PersistentFlags().IntVarP(&rwHostGroupId, "rw-hostgroup-id", "", 10, "the proxysql read hostgroup id")
	serverCmd.PersistentFlags().IntVarP(&roHostGroupId, "ro-hostgroup-id", "", 20, "the proxysql write hostgroup id")
}
