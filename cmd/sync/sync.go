package sync

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

var (
	username, password, host string
	port                     int
	helpFlag                 bool
)

var SyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "use for sync",
	Long:  "use for sync",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errors.New("no command find")
	},
}

func newDB(username, password, host, database string, port int) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", username, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %v mysql fail, error: %v", dsn, err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping %v mysql fail, error: %v", dsn, err)
	}

	return db, nil
}

func init() {
	SyncCmd.PersistentFlags().BoolVarP(&helpFlag, "help", "", false, "Help default flag")
	SyncCmd.PersistentFlags().StringVarP(&username, "admin-username", "u", "admin", "the proxysql-initializer sync user")
	SyncCmd.PersistentFlags().StringVarP(&password, "admin-password", "p", "", "the proxysql-initializer sync password")
	SyncCmd.PersistentFlags().StringVarP(&host, "admin-host", "h", "127.0.0.1", "the proxysql-initializer sync host")
	SyncCmd.PersistentFlags().IntVarP(&port, "admin-port", "P", 6032, "the proxysql-initializer sync port")
	SyncCmd.AddCommand(serverCmd)
	SyncCmd.AddCommand(userCmd)
}
