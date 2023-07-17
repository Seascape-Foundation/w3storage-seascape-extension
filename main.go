package main

import (
	"github.com/ahmetson/service-lib/configuration"
	"github.com/ahmetson/service-lib/extension"
	databaseExtension "github.com/ahmetson/service-lib/extension/database"
	"github.com/ahmetson/service-lib/log"
	"github.com/web3-storage/go-w3s-client"
)

func main() {
	logger, err := log.New("main", true)
	if err != nil {
		logger.Fatal("log.New(`main`)", "error", err)
	}

	logger.Info("Load app configuration")
	appConfig, err := configuration.NewAppConfig(logger)
	if err != nil {
		logger.Fatal("configuration.NewAppConfig", "error", err)
	}
	logger.Info("App configuration loaded successfully")

	if len(appConfig.Services) == 0 {
		logger.Fatal("missing service configuration in service.yml")
	}

	apiToken := appConfig.GetString("W3_STORAGE_API_TOKEN")
	if len(apiToken) == 0 {
		logger.Fatal("missing web3.storage credentials 'W3_STORAGE_API_TOKEN' in env")
	}
	// client is declared in the handler.go
	w3client, err = w3s.NewClient(w3s.WithToken(apiToken))
	if err != nil {
		logger.Fatal("failed to create web4.storage client", "error", err)
	}

	////////////////////////////////////////////////////////////////////////
	//
	// Establish a connection to the oss
	//
	////////////////////////////////////////////////////////////////////////
	// create a database connection
	// if security is enabled, then get the database credentials from vault
	// Set the database connection
	//appConfig.SetDefaults(DatabaseConfigurations)
	//databaseParameters, err := GetParameters(appConfig)
	//if err != nil {
	//	logger.Fatal("GetParameters", "error", err)
	//}

	logger.Info("Run database controller")

	/////////////////////////////////////////////////////////////////////////
	//
	// Create the extension
	//
	/////////////////////////////////////////////////////////////////////////

	service, err := extension.New(appConfig.Services[0], logger)
	if err != nil {
		logger.Fatal("failed to initialize extension", "error", err)
	}

	dbController := service.GetFirstController()
	dbController.RegisterCommand(databaseExtension.Exist, onExist)
	dbController.RegisterCommand(databaseExtension.SelectRow, onSelectRow)
	dbController.RegisterCommand(databaseExtension.SelectAll, onSelectAll)
	dbController.RegisterCommand(databaseExtension.Delete, onDelete)
	dbController.RegisterCommand(databaseExtension.Insert, onInsert)
	dbController.RegisterCommand(databaseExtension.Update, onUpdate)

	service.Run()
}
