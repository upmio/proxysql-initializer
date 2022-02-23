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

func NewServerSync(db *sql.DB, logger *zap.SugaredLogger, namespace, svcGroupName string, rwHostGroupId, roHostGroupId int) (*ServerSync, error) {

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
		client:        clientset,
		namespace:     namespace,
		svcGroupName:  svcGroupName,
		proxysqlDB:    db,
		logger:        logger,
		rwHostGroupId: rwHostGroupId,
		roHostGroupId: roHostGroupId,
	}, nil
}

type ServerSync struct {
	client                       *kubernetes.Clientset
	namespace                    string
	svcGroupName                 string
	proxysqlDB                   *sql.DB
	logger                       *zap.SugaredLogger
	rwHostGroupId, roHostGroupId int
}

func newServer(ip string, port int) *Server {
	return &Server{
		ip:   ip,
		port: port,
	}
}

type Server struct {
	ip   string
	port int
}

func (s *ServerSync) GetServerFromK8s(ctx context.Context) ([]*Server, error) {
	var serverList = make([]*Server, 0)
	podList, err := s.client.CoreV1().Pods(s.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{
			svcGroupNameLabel: s.svcGroupName,
			svcTypeLabel:      svcType,
		}.String(),
	})

	if err != nil {
		return nil, fmt.Errorf("get server list fail, err: %v", err)
	}

	if len(podList.Items) == 0 {
		return nil, fmt.Errorf("server list lenth is zero")
	}

	s.logger.Infof("found %d endpoint from k8s ", len(podList.Items))

	for _, pod := range podList.Items {
		readOnly, ok := pod.Labels[readOnlyLabel]
		if !ok {
			return nil, fmt.Errorf("pod %s does not have label %s", pod.Name, readOnlyLabel)
		}
		for _, container := range pod.Spec.Containers {
			if container.Name == svcType {

				switch readOnly {
				case "true":
					s.logger.Infof("slave server name: %s ip: %s", pod.Name, pod.Status.PodIP)
				case "false":
					serverObj := newServer(pod.Status.PodIP, int(container.Ports[0].ContainerPort))
					serverList = append(serverList, serverObj)
					s.logger.Infof("master server name: %s ip: %s", pod.Name, pod.Status.PodIP)
				default:
					return nil, fmt.Errorf("pod %s label %s is not true and false", pod.Name, readOnlyLabel)
				}
			}
		}
	}

	if len(serverList) > 1 {
		return nil, fmt.Errorf("get master pod count more than 1")
	}

	return serverList, nil
}

func (s *ServerSync) SyncServerToProxy(_ context.Context, serverList []*Server) error {
	_, err := s.proxysqlDB.Exec(cleanHostGroupSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", cleanHostGroupSql, err)
	}
	s.logger.Info("clean mysql_replication_hostgroups success")

	sqlStr := fmt.Sprintf(insertHostGroupSql, s.rwHostGroupId, s.roHostGroupId, s.svcGroupName)
	_, err = s.proxysqlDB.Exec(sqlStr)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", insertHostGroupSql, err)
	}

	s.logger.Info("insert mysql_replication_hostgroups success")

	_, err = s.proxysqlDB.Exec(cleanServerSql)
	if err != nil {
		return fmt.Errorf("execute %s fail, err: %v", cleanServerSql, err)
	}

	s.logger.Info("clean mysql_servers success")

	for _, server := range serverList {
		sqlStr := fmt.Sprintf(insertServerSql, s.rwHostGroupId, server.ip, server.port)
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
