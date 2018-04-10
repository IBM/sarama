package sarama

import (
	"context"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	docker "github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

type KafkaTestServerConfig struct {
	KafkaPort string
	ZkPort    string
}

type KafkaTestServer struct {
	KafkaTestServerConfig
	ctx         context.Context
	cancel      context.CancelFunc
	containerId string

	t      *testing.T
	docker *docker.Client
}

func NewKafkaTestServer(t *testing.T) (*KafkaTestServer, error) {
	var err error
	t.Helper()

	s := &KafkaTestServer{
		t: t,
		KafkaTestServerConfig: KafkaTestServerConfig{KafkaPort: "9092", ZkPort: "2181"},
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.docker, err = docker.NewEnvClient()

	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *KafkaTestServer) Up() {
	s.t.Helper()
	s.cleanupContainer()
	response, err := s.docker.ImagePull(s.ctx, "spotify/kafka", types.ImagePullOptions{})

	if err != nil {
		s.t.Error(err)
		s.t.Fail()
	}

	_, err = ioutil.ReadAll(response)
	if err != nil {
		s.t.Error(err)
		s.t.Fail()
	}
	response.Close()

	internalKafkaPort := nat.Port(fmt.Sprintf("9092/tcp"))
	internalZkPort := nat.Port(fmt.Sprint("2181/tcp"))

	containerInfo, err := s.docker.ContainerCreate(
		s.ctx,
		&container.Config{
			Image: "spotify/kafka",
			ExposedPorts: nat.PortSet{
				internalKafkaPort: struct{}{},
				internalZkPort:    struct{}{},
			},
			Env: []string{
				"ADVERTISED_HOST=localhost",
				fmt.Sprintf("ADVERTISED_PORT=%s", s.KafkaPort),
				"NUM_PARTITIONS=2",
			},
			AttachStderr: true,
			AttachStdout: true,
		},
		&container.HostConfig{
			AutoRemove: true,
			PortBindings: nat.PortMap{
				internalKafkaPort: []nat.PortBinding{
					{HostIP: "0.0.0.0", HostPort: s.KafkaPort},
				},
				internalZkPort: []nat.PortBinding{
					{HostIP: "0.0.0.0", HostPort: s.ZkPort},
				},
			},
		},
		nil,
		s.containerName(),
	)

	if err != nil {
		s.t.Error(err)
		s.t.FailNow()
		return
	}

	s.containerId = containerInfo.ID

	err = s.docker.ContainerStart(s.ctx, s.containerId, types.ContainerStartOptions{})

	if err != nil {
		s.t.Error(err)
		s.t.FailNow()
		return
	}

	s.waitForStartup(0)

	s.t.Logf("started: %s", s.containerName())
}

func (s *KafkaTestServer) Down() {
	s.t.Helper()
	s.cleanupContainer()
}

func (s *KafkaTestServer) Address() string {
	return fmt.Sprintf("localhost:%s", s.KafkaPort)
}

func (s *KafkaTestServer) waitForStartup(count int) error {
	_, err := NewClient([]string{s.Address()}, NewConfig())

	if err == nil {
		return nil
	}

	if count > 20 {
		return err
	}

	s.t.Log("Waiting for kafka")

	time.Sleep(time.Millisecond * 500)

	return s.waitForStartup(count + 1)
}

func (s *KafkaTestServer) cleanupContainer() {
	ctx := context.Background()
	filterBy, _ := filters.ParseFlag(fmt.Sprintf("name=^/%s$", s.containerName()), filters.NewArgs())

	containerList, err := s.docker.ContainerList(ctx, types.ContainerListOptions{
		All:     true,
		Filters: filterBy,
	})

	if err != nil {
		s.t.Error(err)
		s.t.FailNow()
	}

	for _, orphanedContainer := range containerList {
		s.docker.ContainerStop(ctx, orphanedContainer.ID, nil)
		s.docker.ContainerWait(ctx, orphanedContainer.ID)
		s.docker.ContainerRemove(ctx, orphanedContainer.ID, types.ContainerRemoveOptions{
			Force: true,
		})

		s.t.Logf("removed container: %s", orphanedContainer.ID)
	}
}

func (s *KafkaTestServer) containerName() string {
	return "testtopicconsumer"
}
