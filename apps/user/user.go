package user

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/zap"
)

func newUser() *User {
	return &User{}
}

type User struct {
	username string
	password string
}

func NewUserSync(mysqlDB, proxysqlDB *sql.DB, logger *zap.SugaredLogger, defaultHostGroupId, maxConnections int) (*UserSync, error) {
	if mysqlDB == nil {
		return nil, fmt.Errorf("mysqlDB is nil")
	}

	if proxysqlDB == nil {
		return nil, fmt.Errorf("proxysqlDB is nil")
	}

	return &UserSync{
		mysqlDB:            mysqlDB,
		proxysqlDB:         proxysqlDB,
		logger:             logger,
		defaultHostGroupId: defaultHostGroupId,
		maxConnections:     maxConnections,
	}, nil
}

type UserSync struct {
	mysqlDB                            *sql.DB
	proxysqlDB                         *sql.DB
	logger                             *zap.SugaredLogger
	defaultHostGroupId, maxConnections int
}

func (u *UserSync) GetUser(_ context.Context, hostIp string) ([]*User, error) {

	stmt, err := u.mysqlDB.Prepare(getUserSql)
	if err != nil {
		return nil, fmt.Errorf("prepare stmt %s fail, err: %v", getUserSql, err)
	}
	defer func(stmt *sql.Stmt) {
		err := stmt.Close()
		if err != nil {
			fmt.Printf("close stmt fail, err: %v", err)
		}
	}(stmt)

	rows, err := stmt.Query(hostIp)
	if err != nil {
		return nil, fmt.Errorf("execute %s fail, err: %v", getUserSql, err)
	}

	userList := make([]*User, 0, 10)

	for rows.Next() {
		userTemp := newUser()
		err := rows.Scan(&userTemp.username, &userTemp.password)
		if err != nil {
			return nil, fmt.Errorf("query user fail, err: %v", err)
		}
		u.logger.Infof("found user %s", userTemp.username)
		userList = append(userList, userTemp)
	}

	return userList, nil
}

func (u *UserSync) LoadUser(_ context.Context, userList []*User) error {

	for _, user := range userList {
		sqlStr := fmt.Sprintf(insertUserSql, user.username, user.password, u.defaultHostGroupId, u.maxConnections)
		_, err := u.proxysqlDB.Exec(sqlStr)
		if err != nil {
			return fmt.Errorf("execute %s fail, err: %v", insertUserSql, err)
		}
	}
	u.logger.Info("insert mysql_users success")

	_, err := u.proxysqlDB.Exec(loadUserSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", loadUserSql, err)
	}
	u.logger.Info("load mysql user to runtime success")

	_, err = u.proxysqlDB.Exec(saveUserSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", loadUserSql, err)
	}
	u.logger.Info("save mysql user to disk success")

	return nil
}

func (u *UserSync) CleanUser(_ context.Context) error {
	_, err := u.proxysqlDB.Exec(cleanUserSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", cleanUserSql, err)
	}
	u.logger.Info("clean mysql_users success")

	return nil
}
