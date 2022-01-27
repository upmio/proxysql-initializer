package server

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func NewServerSync(db *sql.DB, logger *zap.SugaredLogger, namespace, svcGroupName string) (*ServerSync, error) {

	if db == nil {
		return nil, fmt.Errorf("pass db connect is nil")
	}

	// create incluster config object
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("create incluster config fail, error: %v", err)
	}

	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create clientset fail, error: %v", err)
	}

	return &ServerSync{
		client:            clientset,
		namespace:         namespace,
		svcGroupNameLabel: svcGroupNameLabel,
		svcGroupName:      svcGroupName,
		svcGroupTypeLabel: svcGroupTypeLabel,
		proxysqlDB:        db,
		logger:            logger,
	}, nil
}

type ServerSync struct {
	client            *kubernetes.Clientset
	namespace         string
	svcGroupNameLabel string
	svcGroupName      string
	svcGroupTypeLabel string
	proxysqlDB        *sql.DB
	logger            *zap.SugaredLogger
}

func newServer(ip string, hostgroup, port int) *Server {
	return &Server{
		ip:        ip,
		port:      port,
		hostgroup: hostgroup,
	}
}

type Server struct {
	hostgroup int
	ip        string
	port      int
}

func (s *ServerSync) GetServer(ctx context.Context, svcType string) ([]*Server, error) {
	var serverList = make([]*Server, 0, 5)
	podList, err := s.client.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{
			s.svcGroupNameLabel: s.svcGroupName,
			s.svcGroupTypeLabel: svcType,
		}.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("get server list fail, err: %v", err)
	}

	s.logger.Infof("found %d server endpoint", len(podList.Items))

	for _, pod := range podList.Items {
		for _, container := range pod.Spec.Containers {
			if container.Name == svcType {
				serverObj := newServer(pod.Status.PodIP, writerHostGroup, int(container.Ports[0].ContainerPort))
				serverList = append(serverList, serverObj)
				s.logger.Infof("found server %s", pod.Status.PodIP)
			}
		}
	}

	return serverList, nil
}

func (s *ServerSync) LoadServer(ctx context.Context, serverList []*Server) error {
	if len(serverList) == 0 {
		return fmt.Errorf("input servers list is empty")
	}

	sqlStr := fmt.Sprintf(insertHostGroupSql, writerHostGroup, readerHostGroup, s.svcGroupName)
	_, err := s.proxysqlDB.Exec(sqlStr)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", insertHostGroupSql, err)
	}

	s.logger.Info("insert mysql_replication_hostgroups success")

	for _, mysqlServer := range serverList {
		sqlStr := fmt.Sprintf(insertServerSql, writerHostGroup, mysqlServer.ip, mysqlServer.port)
		_, err := s.proxysqlDB.Exec(sqlStr)
		if err != nil {
			return fmt.Errorf("execute %s fail, err: %v", insertServerSql, err)
		}
	}

	s.logger.Info("insert mysql_servers success")

	_, err = s.proxysqlDB.Exec(loadServerSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", loadServerSql, err)
	}
	s.logger.Info("load mysql server to runtime success")

	_, err = s.proxysqlDB.Exec(saveServerSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", saveServerSql, err)
	}
	s.logger.Info("save mysql server to disk success")

	return nil
}

func (s *ServerSync) CleanServer(ctx context.Context) error {

	_, err := s.proxysqlDB.Exec(cleanHostGroupSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", cleanHostGroupSql, err)
	}
	s.logger.Info("clean mysql_replication_hostgroups success")

	_, err = s.proxysqlDB.Exec(cleanServerSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", cleanServerSql, err)
	}

	s.logger.Info("clean mysql_servers success")

	return nil
}
