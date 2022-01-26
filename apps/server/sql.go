package server

const (
	insertHostGroupSql = `INSERT INTO mysql_replication_hostgroups(writer_hostgroup,reader_hostgroup,comment) VALUES (%d,%d,'%s');`

	cleanHostGroupSql = `DELETE FROM mysql_replication_hostgroups;`

	insertServerSql = `INSERT INTO mysql_servers(hostgroup_id,hostname,port) VALUES (%d,'%s',%d);`

	cleanServerSql = `DELETE FROM mysql_servers;`

	loadServerSql = `LOAD MYSQL SERVERS TO RUNTIME;`

	saveServerSql = `SAVE MYSQL SERVERS TO DISK;`
)
