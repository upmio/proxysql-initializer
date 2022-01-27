package user

const (
	getUserSql = `SELECT user,authentication_string FROM user WHERE host = ? ;`

	insertUserSql = `INSERT INTO mysql_users(username,password,default_hostgroup,max_connections) VALUES ('%s','%s',%d,%d);`

	cleanUserSql = `DELETE FROM mysql_users;`

	loadUserSql = `LOAD MYSQL USERS TO RUNTIME`

	saveUserSql = `SAVE MYSQL USERS TO DISK`
)
