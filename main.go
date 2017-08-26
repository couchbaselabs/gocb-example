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

	err = e.SourceBucket.Manager("", "").CreatePrimaryIndex("", true, false)
	if err != nil {
		return err
	}

	return nil
}


// A function that takes a doc id and returns an error
type DocProcessor func(docId string) error

func (e *Example) CopyBucket(postInsertCallback DocProcessor) (err error) {

	log.Printf("Copying source bucket -> target bucket")

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

func (e *Example) SetSubdocField(docId, subdocKey string, subdocVal interface{}) (err error) {

	_, err = e.TargetBucket.MutateInEx(docId, gocb.SubdocDocFlagNone, 0, 0).
		UpsertEx(subdocKey, subdocVal, gocb.SubdocFlagNone).
		Execute()

	if err != nil {
		return err
	}

	return nil

}

func (e *Example) ForEachDocInTargetBucket(postInsertCallback DocProcessor) (err error) {
	return e.ForEachDocInBucket(postInsertCallback, e.TargetBucket)
}

func (e *Example) ForEachDocInBucket(docProcessor DocProcessor, bucket *gocb.Bucket) (err error) {

	log.Printf("Performing operation over bucket: %v", bucket.Name())

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

		// Invoke the doc processor callback
		if err := docProcessor(rowIdStr); err != nil {
			return err
		}

	}

	return nil
}


func main() {

	// Create an example application and connect to a couchbase cluster
	e := NewExample()
	e.Connect("couchbase://localhost")

	// Create a post-insert callback function that will be invoked on
	// every document that is copied from the source bucket and inserted into the target bucket.
	// It adds the "DateCopied" XATTR to the doc.
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

	// Copy the bucket and pass the post-insert callback function
	if err := e.CopyBucket(postInsertCallback); err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}


	xattrVal, err := e.GetXattrs("airline_10123", xattrKey)
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}
	log.Printf("xattrVal: %+v", xattrVal)


	retValue, err := e.GetSubdocField("airline_10123", "type")
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}

	log.Printf("airline_10123 type: %+v", retValue)


	// Iterate over all docs and update the type field to app:<existing_type>

	appendNamespaceToTypeField := func(docId string) error {

		currentValueOfTypeField, err := e.GetSubdocField(docId, "type")
		if err != nil {
			return err
		}

		newValueOfTypeField := fmt.Sprintf("foo-component:%v", currentValueOfTypeField)

		err = e.SetSubdocField(docId, "type", newValueOfTypeField)
		if err != nil {
			return err
		}

		return nil
	}

	if err := e.ForEachDocInTargetBucket(appendNamespaceToTypeField); err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}

	retValue, err = e.GetSubdocField("airline_10123", "type")
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}

	log.Printf("airline_10123 type (after): %+v", retValue)


}
