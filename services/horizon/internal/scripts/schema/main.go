// this script produces a dump of the horizon database schema
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	dockerClient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go/services/horizon/internal/db2/schema"
)

func pullImage(cli *dockerClient.Client, image string) error {
	reader, err := cli.ImagePull(context.Background(), image, types.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer reader.Close()

	// wait for the pull to finish
	if _, err := io.Copy(os.Stderr, reader); err != nil {
		return err
	}
	return nil
}

func startContainer(cli *dockerClient.Client, image string, env []string) (string, error) {

	config := &container.Config{Image: image, Env: env}

	hostConfig := &container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
	}

	containerInfo, err := cli.ContainerCreate(context.Background(), config, hostConfig, nil, "")
	if err != nil {
		if dockerClient.IsErrNotFound(err) {
			if err := pullImage(cli, image); err != nil {
				return "", err
			}
			containerInfo, err = cli.ContainerCreate(context.Background(), config, hostConfig, nil, "")
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}

	if err = cli.ContainerStart(context.Background(), containerInfo.ID, types.ContainerStartOptions{}); err != nil {
		return "", err
	}

	return containerInfo.ID, err
}

func dbURLFromContainer(cli *dockerClient.Client, containerID, dbName string) (string, error) {
	inspectResult, err := cli.ContainerInspect(context.Background(), containerID)
	if err != nil {
		return "", err
	}

	if entry, ok := inspectResult.NetworkSettings.Ports["5432/tcp"]; !ok || len(entry) != 1 {
		return "", errors.New("container missing 5432 port")
	}

	hostAndPort := inspectResult.NetworkSettings.Ports["5432/tcp"][0]
	return fmt.Sprintf(
		"postgres://postgres@localhost:%s/%s?sslmode=disable",
		hostAndPort.HostPort,
		dbName,
	), nil
}

const pingTimeoutSeconds = 30

func main() {
	cli, err := dockerClient.NewEnvClient()
	if err != nil {
		logrus.WithError(err).Error("could not create docker client")
		return
	}

	containerID, err := startContainer(cli, "postgres:9.6.15-alpine", []string{"POSTGRES_DB=horizon"})
	if err != nil {
		logrus.WithError(err).Error("could not create postgres container")
		return
	}
	dbURL, err := dbURLFromContainer(cli, containerID, "horizon")
	if err != nil {
		logrus.WithError(err).Error("could not extract postgres url from container")
		return
	}
	defer cli.ContainerRemove(
		context.Background(),
		containerID,
		types.ContainerRemoveOptions{Force: true},
	)

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		logrus.WithError(err).Error("could not open postgres connection")
		return
	}

	for secs := 0; secs <= pingTimeoutSeconds; secs++ {
		if err = db.Ping(); err == nil {
			break
		}
		if secs >= pingTimeoutSeconds {
			logrus.WithError(err).Error("could not ping postgres container")
			return
		}
		time.Sleep(time.Millisecond * 500)
	}

	_, err = schema.Migrate(db, schema.MigrateUp, 0)
	if err != nil {
		logrus.WithError(err).Error("could not apply migrations")
		return
	}

	execResponse, err := cli.ContainerExecCreate(context.Background(), containerID, types.ExecConfig{
		Cmd: []string{
			"pg_dump",
			"postgres://postgres@localhost/horizon?sslmode=disable",
			"-n",
			"public",
			"--no-owner",
			"--no-acl",
			"--inserts",
			"--no-security-labels",
		},
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		logrus.WithError(err).Error("could not apply migrations")
		return
	}

	resp, err := cli.ContainerExecAttach(context.Background(), execResponse.ID, types.ExecStartCheck{
		Tty:    false,
		Detach: false,
	})
	if err != nil {
		logrus.WithError(err).Error("could not run pg dump")
		return
	}
	defer resp.Close()

	_, err = stdcopy.StdCopy(os.Stdout, os.Stderr, resp.Reader)
	if err != nil {
		logrus.WithError(err).Error("could not obtain output from container")
		return
	}
}
