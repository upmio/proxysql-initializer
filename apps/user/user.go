package user

import (
	"context"
	"database/sql"
	"fmt"
)

func newUser() *User {
	return &User{
		maxConn: maxConnections,
	}
}

type User struct {
	username string
	password string
	maxConn  int
}

func NewUserSync(mysqlDB, proxysqlDB *sql.DB) (*UserSync, error) {
	if mysqlDB == nil {
		return nil, fmt.Errorf("mysqlDB is nil")
	}

	if proxysqlDB == nil {
		return nil, fmt.Errorf("proxysqlDB is nil")
	}

	return &UserSync{
		mysqlDB:    mysqlDB,
		proxysqlDB: proxysqlDB,
	}, nil
}

type UserSync struct {
	mysqlDB    *sql.DB
	proxysqlDB *sql.DB
}

func (u *UserSync) GetUser(ctx context.Context, hostIp string) ([]*User, error) {

	stmt, err := u.mysqlDB.Prepare(getUserSql)
	if err != nil {
		return nil, fmt.Errorf("prepare stmt %s fail, err: %v", getUserSql, err)
	}

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
		userList = append(userList, userTemp)
	}

	return userList, nil
}

func (u *UserSync) LoadUser(ctx context.Context, userList []*User) error {
	if len(userList) == 0 {
		return fmt.Errorf("input user list is empty")
	}

	for _, user := range userList {
		sqlStr := fmt.Sprintf(insertUserSql, user.username, user.password, defaultHostGroup, maxConnections)
		_, err := u.proxysqlDB.Exec(sqlStr)
		if err != nil {
			return fmt.Errorf("execute %s fail, err: %v", insertUserSql, err)
		}
	}

	_, err := u.proxysqlDB.Exec(loadUserSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", loadUserSql, err)
	}

	_, err = u.proxysqlDB.Exec(saveUserSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", loadUserSql, err)
	}

	return nil
}

func (u *UserSync) CleanUser(ctx context.Context) error {
	_, err := u.proxysqlDB.Exec(cleanUserSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", cleanUserSql, err)
	}

	return nil
}
