package sync

const (
	serverSqlStr = `REPLACE INTO mysql_servers(hostgroup_id,hostname,port) VALUES (%d,'%s',%d);`

	hostgroupSqlStr = `REPLACE INTO mysql_replication_hostgroups(writer_hostgroup,reader_hostgroup,comment) VALUES (%d,%d,%s);`

	loadsSqlStr = `LOAD MYSQL SERVERS TO RUNTIME`

	saveSqlStr = `SAVE MYSQL SERVERS TO DISK`
)
