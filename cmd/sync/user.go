package sync

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/upmio/proxysql-initializer/apps/user"
	"go.uber.org/zap"
)

var (
	mysqlHost, mysqlUser, mysqlPassword           string
	mysqlPort, defaultHostGroupId, maxConnections int
)

const (
	internalIpEnvKey = "INTERNAL_IP"
)

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "sync mysql user",
	Long:  "sync mysql user",
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			hostIP      string
			syncObj     *user.UserSync
			err         error
			ctx, cancel = context.WithTimeout(context.Background(), time.Second*10)
		)

		defer cancel()

		if hostIP = os.Getenv(internalIpEnvKey); hostIP == "" {
			return fmt.Errorf("get %s environment variables fail", internalIpEnvKey)
		}

		proxysqlDB, err := newDB(username, password, host, "main", port)
		if err != nil {
			return fmt.Errorf("create proxysql db connect fail, err: %v", err)
		}

		defer func(proxysqlDB *sql.DB) {
			err := proxysqlDB.Close()
			if err != nil {
				fmt.Printf("close proxysql db fail, err: %v", err)
			}
		}(proxysqlDB)

		mysqlDB, err := newDB(mysqlUser, mysqlPassword, mysqlHost, "mysql", mysqlPort)
		if err != nil {
			return fmt.Errorf("create mysql db connect fail, err: %v", err)
		}

		defer func(mysqlDB *sql.DB) {
			err := mysqlDB.Close()
			if err != nil {
				fmt.Printf("close mysql db fail, err: %v", err)
			}
		}(mysqlDB)

		logger, _ := zap.NewDevelopment()
		slogger := logger.Sugar()

		syncObj, err = user.NewUserSync(mysqlDB, proxysqlDB, slogger, defaultHostGroupId, maxConnections)
		if err != nil {
			return err
		}

		userList, err := syncObj.GetUser(ctx, hostIP)
		if err != nil {
			return err
		}

		if len(userList) != 0 {
			err = syncObj.CleanUser(ctx)
			if err != nil {
				return err
			}

			return syncObj.LoadUser(ctx, userList)
		}

		slogger.Info("not found user to sync")
		return nil
	},
}

func init() {
	userCmd.PersistentFlags().StringVarP(&mysqlHost, "mysql-host", "", "127.0.0.1", "the proxysql-initializer backend mysql host")
	userCmd.PersistentFlags().StringVarP(&mysqlUser, "mysql-user", "", "check", "the proxysql-initializer backend mysql username")
	userCmd.PersistentFlags().StringVarP(&mysqlPassword, "mysql-password", "", "", "the proxysql-initializer backend mysql password")
	userCmd.PersistentFlags().IntVarP(&mysqlPort, "mysql-port", "", 6033, "the proxysql-initializer backend mysql port")
	userCmd.PersistentFlags().IntVarP(&defaultHostGroupId, "default-hostgroup-id", "", 10, "the proxysql user default hostgroup")
	userCmd.PersistentFlags().IntVarP(&maxConnections, "max-connections", "", 1024, "the proxysql user max connections")
}
