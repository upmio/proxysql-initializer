package sync

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	svcGroupEnvKey       = "SERVICE_GROUP_NAME"
	namespaceEnvKey      = "NAMESPACE"
	svcGroupNameLabelKey = "dbscale.service.group"
	svcGroupTypeLabelKey = "dbscale.service.image.name"
	writerHostGroup      = 10
	readerHostGroup      = 20
)

func NewServerSync(username, password, host, svcGroupType string, port int) (*ServerSync, error) {
	// create incluster config object
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/main?charset=utf8", username, password, host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %v mysql fail, error: %v", dsn, err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping %v mysql fail, error: %v", dsn, err)
	}

	svcGroupEnvValue := os.Getenv(svcGroupEnvKey)

	namespaceEnvValue := os.Getenv(namespaceEnvKey)

	if svcGroupEnvValue == "" {
		return nil, fmt.Errorf("get %s environment variables failed", svcGroupEnvKey)
	}
	if namespaceEnvValue == "" {
		return nil, fmt.Errorf("get %s environment variables failed", namespaceEnvKey)
	}

	return &ServerSync{
		client:       clientset,
		svcGroupName: svcGroupEnvValue,
		namespace:    namespaceEnvValue,
		svcGroupType: svcGroupType,
	}, nil
}

type ServerSync struct {
	client       *kubernetes.Clientset
	namespace    string
	svcGroupName string
	svcGroupType string
	db           *sql.DB
}

func newMysql(ip string, port int) *mysql {
	return &mysql{
		ip:   ip,
		port: port,
	}
}

type mysql struct {
	ip   string
	port int
}

func (s *ServerSync) GetMysqlServerFromKubeByLabel(ctx context.Context) ([]*mysql, error) {
	var ret = make([]*mysql, 0, 10)
	podList, err := s.client.CoreV1().Pods(s.namespace).List(ctx, metaV1.ListOptions{
		LabelSelector: labels.Set{
			svcGroupNameLabelKey: s.svcGroupName,
			svcGroupTypeLabelKey: s.svcGroupType,
		}.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("get pods list fail, err: %v", err)
	}

	for _, pod := range podList.Items {
		for _, container := range pod.Spec.Containers {
			if container.Name == "mysql" {
				mysqlObj := newMysql(pod.Status.PodIP, int(container.Ports[0].ContainerPort))
				ret = append(ret, mysqlObj)
			}
		}
	}
	fmt.Printf("get %s service mysql pod ip %v", s.svcGroupName, ret)
	return ret, nil
}

func (s *ServerSync) LoadMysqlServerToRuntime(ctx context.Context, mysqlList []*mysql) error {
	if len(mysqlList) == 0 {
		return fmt.Errorf("input server list is empty")
	}
	sqlStr := fmt.Sprintf(hostgroupSqlStr, writerHostGroup, readerHostGroup, s.svcGroupName)
	_, err := s.db.Exec(sqlStr)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", hostgroupSqlStr, err)
	}

	for _, mysqlServer := range mysqlList {
		sqlStr := fmt.Sprintf(serverSqlStr, writerHostGroup, mysqlServer.ip, mysqlServer.port)
		_, err := s.db.Exec(sqlStr)
		if err != nil {
			return fmt.Errorf("execute %s fail, err: %v", serverSqlStr, err)
		}
	}

	_, err = s.db.Exec(loadsSqlStr)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", loadsSqlStr, err)
	}

	_, err = s.db.Exec(saveSqlStr)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", saveSqlStr, err)
	}

	return nil
}
