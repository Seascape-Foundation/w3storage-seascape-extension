// The database package handles all the database operations.
// Note that for now it uses Mysql as a hardcoded data
//
// The database is creating a new service with the inproc reply controller.
// For any database operation interact with the service.
package main

import (
	"context"
	"fmt"
	"github.com/ahmetson/common-lib/data_type/key_value"
	"github.com/ahmetson/service-lib/communication/command"
	"github.com/ahmetson/service-lib/communication/message"
	databaseExtension "github.com/ahmetson/service-lib/extension/database"
	"github.com/ahmetson/service-lib/log"
	"github.com/ahmetson/service-lib/remote"
	"github.com/ipfs/go-cid"
	"github.com/web3-storage/go-w3s-client"
	"io/fs"
	"os"
)

var w3client w3s.Client

type Storage struct {
	FileName string
	Cid      cid.Cid
}

// getStorageParameterAt returns the FileName and Cid from the query request.
// It could fail if the index of the parameter is out of range or the CID is invalid
func getStorageParameterAt(queryParameters databaseExtension.QueryRequest, index int) (Storage, error) {
	storage := Storage{}

	if len(queryParameters.Fields) < index ||
		len(queryParameters.Tables) < index {
		return storage, fmt.Errorf("the index is out of range")
	}

	storage.FileName = queryParameters.Fields[index]
	cidString := queryParameters.Tables[index]
	cidDecoded, err := cid.Decode(cidString)
	if err != nil {
		return storage, fmt.Errorf("the '%s' table is invalid cid: %w", cidString, err)
	} else {
		storage.Cid = cidDecoded
	}

	return storage, nil
}

// read function reads the data from the remote web3 storage.
// It won't verify the w3client to be initialized. Therefore, call this only after setting w3client.
func read(storage Storage) (key_value.KeyValue, error) {
	kv := key_value.Empty()

	res, err := w3client.Get(context.Background(), storage.Cid)
	if err != nil {
		return kv, fmt.Errorf("failed to get %s cid from ipfs: %w", storage.Cid.String(), err)
	}

	//
	_, fileSystem, _ := res.Files()

	// Open a file in a directory
	openedFile, err := fileSystem.Open("/" + storage.FileName)
	if err != nil {
		return kv, fmt.Errorf("failed to open %s file in %s cid: %w", storage.FileName, storage.Cid.String(), err)
	}

	fileStat, _ := openedFile.Stat()
	if fileStat.Size() == 0 {
		kv.Set(storage.FileName, "")
		return kv, nil
	}

	var fileContent = make([]byte, fileStat.Size())
	_, err = openedFile.Read(fileContent)
	if err != nil {
		return kv, fmt.Errorf("failed to read content of file %s in %s cid: %w", storage.FileName, storage.Cid.String(), err)
	}

	kv.Set(storage.FileName, string(fileContent))

	return kv, nil
}

// write function writes the data to the remote web3 storage.
// It won't verify the w3client to be initialized. Therefore, call this only after setting w3client.
//
// Upon success, it will return the generated CID.
//
// this function depends on the folder
func write(fileName string, content string) (string, error) {
	file, err := os.Create(fileName)
	if err != nil {
		return "", fmt.Errorf("failed to create a temporary file '%s': %w", fileName, err)
	}
	_, err = file.Write([]byte(content))
	if err != nil {
		_ = file.Close()
		return "", fmt.Errorf("failed to write the content into temporary %s: %w", fileName, err)
	}
	// Test the putting a new file

	fileCid, err := w3client.Put(context.Background(), fs.File(file))
	if err != nil {
		_ = file.Close()
		return "", fmt.Errorf("failed to put the file %s into w3storage: %w", fileName, err)
	}

	_ = file.Close()

	return fileCid.String(), nil
}

// cidMatchesFileNames validates the incoming parameters
// the fieldLen arguments is given to make sure that request has exact given amount of cids and file names.
//
// if there could be arbitrary amount of cids and filenames, then pass fieldLen as 0.
func cidMatchesFileNames(queryParameters databaseExtension.QueryRequest, fieldLen int) error {
	if fieldLen > 0 {
		if len(queryParameters.Fields) != fieldLen {
			return fmt.Errorf("required %d field length, but query request has %d fields", fieldLen, len(queryParameters.Fields))
		} else if len(queryParameters.Tables) != fieldLen {
			return fmt.Errorf("required %d tables, but query request has %d tables", fieldLen, len(queryParameters.Tables))
		} else {
			return nil
		}
	} else {
		fieldLen = len(queryParameters.Fields)
		if fieldLen == 0 {
			return fmt.Errorf("required at least 1 field, but query is empty")
		} else if len(queryParameters.Tables) != fieldLen {
			return fmt.Errorf("required at least %d tables (same as fields), but query request has %d tables", fieldLen, len(queryParameters.Tables))
		} else {
			return nil
		}
	}
}

// onSelectAll selects all rows from the database
//
// intended to be used once during the app launch for caching.
//
// Minimize the database queries by using this
var onSelectAll = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	if w3client == nil {
		return message.Fail("w3client is null")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters databaseExtension.QueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	if err := cidMatchesFileNames(queryParameters, 0); err != nil {
		return message.Fail("cidMatchesFileNames: " + err.Error())
	}

	length := len(queryParameters.Fields)
	rows := make([]key_value.KeyValue, length)

	for i := 0; i < length; i++ {
		storage, err := getStorageParameterAt(queryParameters, i)
		if err != nil {
			return message.Fail("getStorageParameter: " + err.Error())
		}

		kv, err := read(storage)
		if err != nil {
			return message.Fail("failed to read data of file " + storage.FileName + " in " + storage.Cid.String() + " cid. error: " + err.Error())
		}

		rows[i] = kv
	}

	reply := databaseExtension.SelectAllReply{
		Rows: rows,
	}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

// checks whether there are any rows that matches to the query
var onExist = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	if w3client == nil {
		return message.Fail("w3client is null")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters databaseExtension.QueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	if err := cidMatchesFileNames(queryParameters, 1); err != nil {
		return message.Fail("cidMatchesFileNames: " + err.Error())
	}

	storage, err := getStorageParameterAt(queryParameters, 0)
	if err != nil {
		return message.Fail("getStorageParameter: " + err.Error())
	}

	kv, err := read(storage)
	if err != nil {
		return message.Fail("failed to read data of file " + storage.FileName + " in " + storage.Cid.String() + " cid. error: " + err.Error())
	}

	content, err := kv.GetString(storage.FileName)
	if err != nil {
		return message.Fail("failed to get file content from storage: " + err.Error())
	}

	reply := databaseExtension.ExistReply{}
	reply.Exist = false
	if len(content) > 0 {
		reply.Exist = true
	}

	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

// Read the row only once
// func on_read_one_row(db *sql.DB, query string, parameters []interface{}, outputs []interface{}) ([]interface{}, error) {
var onSelectRow = func(request message.Request, _ log.Logger, clients remote.Clients) message.Reply {
	if w3client == nil {
		return message.Fail("w3client is null")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters databaseExtension.QueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	if err := cidMatchesFileNames(queryParameters, 1); err != nil {
		return message.Fail("cidMatchesFileNames: " + err.Error())
	}

	storage, err := getStorageParameterAt(queryParameters, 0)
	if err != nil {
		return message.Fail("getStorageParameter: " + err.Error())
	}

	kv, err := read(storage)
	if err != nil {
		return message.Fail("failed to read data of file " + storage.FileName + " in " + storage.Cid.String() + " cid. error: " + err.Error())
	}

	reply := databaseExtension.SelectRowReply{
		Outputs: kv,
	}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

// Execute the deletion
var onDelete = func(request message.Request, logger log.Logger, _ remote.Clients) message.Reply {
	// heavily relying on onExist for the validation.
	// in case of the file change, then make sure that onDelete has two parameters
	existReply := onExist(request, logger, nil)
	if !existReply.IsOK() {
		return message.Fail("onExist failed: " + existReply.Message)
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters databaseExtension.QueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	storage, err := getStorageParameterAt(queryParameters, 0)
	if err != nil {
		return message.Fail("getStorageParameter: " + err.Error())
	}

	content := ""

	fileCid, err := write(storage.FileName, content)
	if err != nil {
		return message.Fail("failed to read data of file " + storage.FileName + " in " + storage.Cid.String() + " cid. error: " + err.Error())
	}

	reply := databaseExtension.DeleteReply{
		Id: fileCid,
	}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

// Execute the insert
var onInsert = func(request message.Request, _ log.Logger, _ remote.Clients) message.Reply {
	if w3client == nil {
		return message.Fail("w3client is null")
	}

	//parameters []interface{}, outputs []interface{}
	var queryParameters databaseExtension.QueryRequest
	err := request.Parameters.Interface(&queryParameters)
	if err != nil {
		return message.Fail("parameter validation:" + err.Error())
	}

	if len(queryParameters.Fields) != 1 {
		return message.Fail("missing the file name in fields or too many file names were given")
	}
	if len(queryParameters.Arguments) != 1 {
		return message.Fail("missing the file content in the arguments or too many contents were given")
	}

	content, ok := queryParameters.Arguments[0].(string)
	if !ok {
		return message.Fail("the argument should be a string but it's not")
	}
	fileName := queryParameters.Fields[0]

	fileCid, err := write(fileName, content)
	if err != nil {
		return message.Fail("failed to write on web3storage: " + err.Error())
	}

	reply := databaseExtension.InsertReply{
		Id: fileCid,
	}
	replyMessage, err := command.Reply(&reply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}

var onUpdate = func(request message.Request, logger log.Logger, _ remote.Clients) message.Reply {
	reply := onInsert(request, logger, nil)
	if !reply.IsOK() {
		return reply
	}

	id, err := reply.Parameters.GetString("id")
	if err != nil {
		return message.Fail("onInsert didn't return id: " + err.Error())
	}
	updateReply := databaseExtension.UpdateReply{
		Id: id,
	}

	replyMessage, err := command.Reply(&updateReply)
	if err != nil {
		return message.Fail("command.Reply: " + err.Error())
	}

	return replyMessage
}
