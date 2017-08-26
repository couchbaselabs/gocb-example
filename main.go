package main

import (
	"fmt"
	"gopkg.in/couchbase/gocb.v1"
	"log"
	"time"
)


type Example struct {
	ClusterConnection *gocb.Cluster
	SourceBucket      *gocb.Bucket
	TargetBucket      *gocb.Bucket
}

func NewExample() *Example {
	return &Example{}
}

func (e *Example) Connect(connSpecStr string) (err error) {

	e.ClusterConnection, err = gocb.Connect(connSpecStr)
	if err != nil {
		return err
	}

	e.SourceBucket, err = e.ClusterConnection.OpenBucket("travel-sample", "password")
	if err != nil {
		return err
	}

	e.TargetBucket, err = e.ClusterConnection.OpenBucket("travel-sample-copy", "password")
	if err != nil {
		return err
	}

	return nil
}


// A function that takes a doc id and returns an error
type PostInsertCallback func(docId string) error

func (e *Example) CopyBucket(postInsertCallback PostInsertCallback) (err error) {

	result := map[string]interface{}{}
	_, err = e.SourceBucket.Get("airline_10", &result)
	if err != nil {
		return err
	}

	log.Printf("Result: %+v", result)

	err = e.SourceBucket.Manager("", "").CreatePrimaryIndex("", true, false)
	if err != nil {
		return err
	}

	// Get the doc ID and the doc body in a single query
	query := gocb.NewN1qlQuery("SELECT META(`travel-sample`).id,* FROM `travel-sample`")
	rows, err := e.SourceBucket.ExecuteN1qlQuery(query, nil)
	if err != nil {
		return err
	}

	row := map[string]interface{}{}
	for rows.Next(&row) {

		// Get row ID
		rowIdRaw, ok := row["id"]
		if !ok {
			return fmt.Errorf("Row does not have id field")
		}
		rowIdStr, ok := rowIdRaw.(string)
		if !ok {
			return fmt.Errorf("Row id field not of expected type")
		}

		// Get row document
		docRaw, ok := row["travel-sample"]
		if !ok {
			return fmt.Errorf("Row does not have doc field")
		}

		_, err := e.TargetBucket.Insert(rowIdStr, docRaw, 0)
		if err != nil {
			return err
		}

		// Invoke the post-insert callback
		if err := postInsertCallback(rowIdStr); err != nil {
			return err
		}

	}

	return nil

}

//func (e *Example) AddXattrs() error {
//
//	k := "airline_10123"
//
//	cas, err := e.TargetBucket.Get(k, nil)
//	if err != nil {
//		return err
//	}
//
//	mutateFlag := gocb.SubdocDocFlagNone
//
//	xattrKey := "Metadata"
//	xattrVal := map[string]interface{}{
//		"DateCopied": time.Now(),
//	}
//	builder := e.TargetBucket.MutateInEx(k, mutateFlag, gocb.Cas(cas), uint32(0)).
//		UpsertEx(xattrKey, xattrVal, gocb.SubdocFlagXattr) // Update the xattr
//
//	docFragment, err := builder.Execute()
//	if err != nil {
//		return err
//	}
//	log.Printf("docFragment: %+v", docFragment)
//
//	return nil
//
//}

func (e *Example) GetXattrs(docId, xattrKey string) (xattrVal interface{}, err error) {

	res, err := e.TargetBucket.LookupIn(docId).
		GetEx(xattrKey, gocb.SubdocFlagXattr).
		Execute()
	if err != nil {
		return nil, err
	}

	res.Content(xattrKey, &xattrVal)

	return xattrVal, nil

}

func (e *Example) GetSubdocField(docId, subdocKey string) (retValue interface{}, err error) {


	frag, err := e.TargetBucket.LookupIn(docId).Get(subdocKey).Execute()
	if err != nil {
		return nil, err
	}
	frag.Content(subdocKey, &retValue)


	return retValue, nil


}

func main() {

	// Create an example application and connect to a couchbase cluster
	e := NewExample()
	e.Connect("couchbase://localhost")

	// Create a post-insert callback function that will be invoked on
	// every document that is copied from the source bucket and inserted into the target bucket.
	xattrKey := "Metadata"
	postInsertCallback := func(docId string) error {

		cas, err := e.TargetBucket.Get(docId, nil)
		if err != nil {
			return err
		}

		mutateFlag := gocb.SubdocDocFlagNone

		xattrVal := map[string]interface{}{
			"DateCopied": time.Now(),
		}
		builder := e.TargetBucket.MutateInEx(docId, mutateFlag, gocb.Cas(cas), uint32(0)).
			UpsertEx(xattrKey, xattrVal, gocb.SubdocFlagXattr) // Update the xattr

		_, err = builder.Execute()
		if err != nil {
			return err
		}

		return nil
	}

	log.Printf("postInsertCallback: %v", postInsertCallback)

	//// Copy the bucket and pass the post-insert callback function
	//if err := e.CopyBucket(postInsertCallback); err != nil {
	//	panic(fmt.Errorf("Error: %v", err))
	//}

	//if err := e.AddXattrs(); err != nil {
	//	panic(fmt.Errorf("Error: %v", err))
	//}


	xattrVal, err := e.GetXattrs("airline_10123", xattrKey)
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}
	log.Printf("xattrVal: %+v", xattrVal)


	retValue, err := e.GetSubdocField("airline_10123", "type")
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}

	log.Printf("retVal: %+v", retValue)


	// Iterate over all docs and update the type field to app:<existing_type>


}
