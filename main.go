package main

import (
	"github.com/ahmetson/service-lib/communication/command"
	"github.com/ahmetson/service-lib/configuration"
	"github.com/ahmetson/service-lib/extension"
	databaseExtension "github.com/ahmetson/service-lib/extension/database"
	"github.com/ahmetson/service-lib/log"
	"github.com/web3-storage/go-w3s-client"
)

func main() {
	logger, err := log.New("main", true)
	if err != nil {
		log.Fatal("log.New(`main`)", "error", err)
	}

	appConfig, err := configuration.New(logger)
	if err != nil {
		logger.Fatal("configuration.NewAppConfig", "error", err)
	}

	// W3Extension specific requirements check
	apiToken := appConfig.GetString("W3_STORAGE_API_TOKEN")
	if len(apiToken) == 0 {
		logger.Fatal("missing web3.storage credentials 'W3_STORAGE_API_TOKEN' in env")
	}

	////////////////////////////////////////////////////////////////////////
	//
	// Establish a connection
	//
	w3client, err = w3s.NewClient(w3s.WithToken(apiToken))
	if err != nil {
		logger.Fatal("failed to create web4.storage client", "error", err)
	}

	/////////////////////////////////////////////////////////////////////////
	//
	// Create the extension
	//
	service, err := extension.New(appConfig, logger)
	if err != nil {
		logger.Fatal("extension.New", "error", err)
	}

	service.AddController(configuration.ReplierType)
	// web proxy is set for the testing purpose
	const webProxyUrl = "github.com/ahmetson/web-proxy"
	//const organizationProxyUrl = "github.com/ahmetson/organization-proxy"
	// organization proxy is set to connect from other services
	service.RequireProxy(webProxyUrl)
	//service.RequireProxy(organizationProxyUrl)

	service.Pipe(webProxyUrl, service.GetControllerName())
	//service.Pipe(organizationProxyUrl, service.GetControllerName())

	///////////////////////////////////////////////////////////////////////
	//
	// Set the routes
	//
	dbController := service.GetController()
	dbController.AddRoute(command.NewRoute(databaseExtension.Exist, onExist))
	dbController.AddRoute(command.NewRoute(databaseExtension.SelectRow, onSelectRow))
	dbController.AddRoute(command.NewRoute(databaseExtension.SelectAll, onSelectAll))
	dbController.AddRoute(command.NewRoute(databaseExtension.Delete, onDelete))
	dbController.AddRoute(command.NewRoute(databaseExtension.Insert, onInsert))
	dbController.AddRoute(command.NewRoute(databaseExtension.Update, onUpdate))

	///////////////////////////////////////////////////////////////////////
	//
	// Done, let's run it
	//
	err = service.Prepare()
	if err != nil {
		logger.Fatal("service.Prepare", "error", err)
	}

	service.Run()
}
