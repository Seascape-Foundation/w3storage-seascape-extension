package main

import (
	"context"
	"github.com/ahmetson/service-lib/configuration"
	"github.com/ahmetson/service-lib/identity"
	"github.com/ahmetson/service-lib/log"
	"github.com/ahmetson/service-lib/remote"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ahmetson/w3storage-extension/handler"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

// We won't test the requests.
// The requests are tested in the controllers
// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing context
type TestControllerSuite struct {
	suite.Suite
	dbName    string
	container *mysql.MySQLContainer
	client    *remote.ClientSocket
	ctx       context.Context
}

func (suite *TestControllerSuite) SetupTest() {
	suite.dbName = "test"
	_, filename, _, _ := runtime.Caller(0)
	storageAbiSql := "20230308171023_storage_abi.sql"
	storageAbiPath := filepath.Join(filepath.Dir(filename), "..", "_db", "migrations", storageAbiSql)

	ctx := context.TODO()
	container, err := mysql.RunContainer(ctx,
		mysql.WithDatabase(suite.dbName),
		mysql.WithUsername("root"),
		mysql.WithPassword("tiger"),
		mysql.WithScripts(storageAbiPath),
	)

	suite.Require().NoError(err)
	suite.container = container
	suite.ctx = ctx

	logger, err := log.New("controller-suite", false)
	suite.Require().NoError(err)
	appConfig, err := configuration.NewAppConfig(logger)
	suite.Require().NoError(err)

	// Overwrite the host
	host, err := container.Host(ctx)
	suite.Require().NoError(err)
	appConfig.SetDefault("SDS_DATABASE_HOST", host)

	// Overwrite the port
	ports, err := container.Ports(ctx)
	suite.Require().NoError(err)
	exposedPort := ""
	for _, port := range ports {
		if len(ports) > 0 {
			exposedPort = port[0].HostPort
			break
		}
	}
	suite.Require().NotEmpty(exposedPort)
	DatabaseConfigurations.Parameters["SDS_DATABASE_PORT"] = exposedPort
	DatabaseConfigurations.Parameters["SDS_DATABASE_NAME"] = suite.dbName

	//go Run(app_config, logger)
	// wait for initiation of the controller
	time.Sleep(time.Second * 1)

	databaseService, err := identity.Inprocess("DATABASE")
	suite.Require().NoError(err)
	client, err := remote.InprocRequestSocket(databaseService.Url(), logger, appConfig)
	suite.Require().NoError(err)

	suite.client = client

	suite.T().Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			suite.T().Fatalf("failed to terminate container: %s", err)
		}
		if err := client.Close(); err != nil {
			suite.T().Fatalf("failed to close client socket: %s", err)
		}
	})
}

func (suite *TestControllerSuite) TestInsert() {
	suite.T().Log("test INSERT command")
	// query
	arguments := []interface{}{"test_id", `[{}]`}
	request := handler.DatabaseQueryRequest{
		Fields:    []string{"abi_id", "body"},
		Tables:    []string{"storage_abi"},
		Arguments: arguments,
	}
	var reply handler.InsertReply
	err := handler.INSERT.Request(suite.client, request, &reply)
	suite.Require().NoError(err)

	// query
	arguments = []interface{}{"test_id"}
	request = handler.DatabaseQueryRequest{
		Fields:    []string{"abi_id"},
		Tables:    []string{"storage_abi"},
		Where:     "abi_id = ?",
		Arguments: arguments,
	}
	var readReply handler.SelectRowReply
	err = handler.SelectRow.Request(suite.client, request, &readReply)
	suite.Require().NoError(err)
	suite.Require().EqualValues("test_id", readReply.Outputs["abi_id"])

	suite.T().Log("test SELECT ALL command")
	// query
	request = handler.DatabaseQueryRequest{
		Fields: []string{"abi_id", "body"},
		Tables: []string{"storage_abi"},
	}
	var replyAll handler.SelectAllReply
	err = handler.SelectAll.Request(suite.client, request, &replyAll)
	suite.Require().NoError(err)
	suite.T().Log(replyAll)
	suite.Require().Len(replyAll.Rows, 1)
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestController(t *testing.T) {
	suite.Run(t, new(TestControllerSuite))
}
