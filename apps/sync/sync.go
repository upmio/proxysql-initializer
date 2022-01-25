package sync

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func NewServerSync(username, password, host string, port int) (*ServerSync, error) {
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
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &ServerSync{
		client:    clientset,
		selector:  os.Getenv("SVC_ID"),
		namespace: os.Getenv("NAMESPACE"),
		db:        db,
	}, nil
}

type ServerSync struct {
	client    *kubernetes.Clientset
	namespace string
	selector  string
	db        *sql.DB
}

func (s *ServerSync) GetServerList(ctx context.Context) ([]string, error) {
	var ret = make([]string, 0, 10)
	podList, err := s.client.CoreV1().Pods(s.namespace).List(ctx, metaV1.ListOptions{LabelSelector: s.selector})
	if err != nil {
		return nil, err
	}

	for _, pod := range podList.Items {
		ret = append(ret, pod.Status.PodIP)
	}
	return ret, nil
}

func (s *ServerSync) SyncServerList(ctx context.Context, serverList []string) error {
	if len(serverList) == 0 {
		return fmt.Errorf("input server list is empty")
	}

	for _, server := range serverList {
		fmt.Println(server)
	}
	return nil
}
