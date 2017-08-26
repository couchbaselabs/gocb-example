package main

import (
	"fmt"
	"gopkg.in/couchbase/gocb.v1"
	"log"
	"time"
)

const (

	// XATTRS will be stored under this key
	xattrKey = "Metadata"

	// A sample doc ID for inspection purposes
	sampleDocId = "airline_10123"

)

// A custom function type that takes a doc id and returns an error
type DocIdProcessor func(docId string) error

// A struct to keep references to the cluster connection and open buckets
type ExampleApp struct {
	ClusterConnection *gocb.Cluster
	SourceBucket      *gocb.Bucket
	TargetBucket      *gocb.Bucket
}

// Create a new ExampleApp
func NewExample() *ExampleApp {
	return &ExampleApp{}
}

// Connect to the cluster and buckets, create primary indexes
func (e *ExampleApp) Connect(connSpecStr string) (err error) {

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

	err = e.TargetBucket.Manager("", "").CreatePrimaryIndex("", true, false)
	if err != nil {
		return err
	}

	return nil
}



func (e *ExampleApp) CopyBucketAddXATTRS() (err error) {

	// Create a post-insert callback function that will be invoked on
	// every document that is copied from the source bucket and inserted into the target bucket.
	// It adds the "DateCopied" XATTR to the doc.
	postInsertCallback := func(docId string) error {

		cas, err := e.TargetBucket.Get(docId, nil)
		if err != nil {
			return err
		}

		mutateFlag := gocb.SubdocDocFlagNone

		xattrVal := map[string]interface{}{
			"DateCopied": time.Now(),
			"UpstreamSource": e.SourceBucket.Name(),
		}
		builder := e.TargetBucket.MutateInEx(docId, mutateFlag, gocb.Cas(cas), uint32(0)).
			UpsertEx(xattrKey, xattrVal, gocb.SubdocFlagXattr) // Update the xattr

		_, err = builder.Execute()
		if err != nil {
			return err
		}

		return nil
	}

	// Copy the bucket and pass the post-insert callback function
	if err := e.CopyBucketWithCallback(postInsertCallback); err != nil {
		return err
	}

	return nil

}

func (e *ExampleApp) CopyBucketWithCallback(postInsertCallback DocIdProcessor) (err error) {

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

func (e *ExampleApp) GetXattrs(docId, xattrKey string) (xattrVal interface{}, err error) {

	res, err := e.TargetBucket.LookupIn(docId).
		GetEx(xattrKey, gocb.SubdocFlagXattr).
		Execute()
	if err != nil {
		return nil, err
	}

	res.Content(xattrKey, &xattrVal)

	return xattrVal, nil

}

func (e *ExampleApp) GetSubdocField(docId, subdocKey string) (retValue interface{}, err error) {


	frag, err := e.TargetBucket.LookupIn(docId).Get(subdocKey).Execute()
	if err != nil {
		return nil, err
	}
	frag.Content(subdocKey, &retValue)

	return retValue, nil

}

func (e *ExampleApp) SetSubdocField(docId, subdocKey string, subdocVal interface{}) (err error) {

	_, err = e.TargetBucket.MutateInEx(docId, gocb.SubdocDocFlagNone, 0, 0).
		UpsertEx(subdocKey, subdocVal, gocb.SubdocFlagNone).
		Execute()

	if err != nil {
		return err
	}

	return nil

}

// Loop over each doc in the target bucket and callback the doc id processor with the doc id
func (e *ExampleApp) ForEachDocIdTargetBucket(postInsertCallback DocIdProcessor) (err error) {
	return e.ForEachDocIdBucket(postInsertCallback, e.TargetBucket)
}

// Loop over each doc in the bucket and callback the doc id processor with the doc id
func (e *ExampleApp) ForEachDocIdBucket(docIdProcessor DocIdProcessor, bucket *gocb.Bucket) (err error) {

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
		if err := docIdProcessor(rowIdStr); err != nil {
			return err
		}

	}

	return nil
}


func (e *ExampleApp) AddNameSpaceToTypeFieldViaSubdoc(namespacePrefix string) (err error) {

	// Iterate over all docs and update the type field to app:<existing_type>
	appendNamespaceToTypeField := func(docId string) error {

		currentValueOfTypeField, err := e.GetSubdocField(docId, "type")
		if err != nil {
			return err
		}

		newValueOfTypeField := fmt.Sprintf("%v:%v", namespacePrefix, currentValueOfTypeField)

		err = e.SetSubdocField(docId, "type", newValueOfTypeField)
		if err != nil {
			return err
		}

		return nil
	}

	if err := e.ForEachDocIdTargetBucket(appendNamespaceToTypeField); err != nil {
		return err
	}

	return nil
}

func main() {

	// Create an example application and connect to a couchbase cluster
	e := NewExample()
	e.Connect("couchbase://localhost")

	// ----------------------------- Copy Source Bucket -> Target Bucket -----------------------------------------------

	// Copy the source bucket to the target bucket, adding XATTRS during the process
	if err := e.CopyBucketAddXATTRS(); err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}

	// Verify: Grab a sample doc (arbitrarily chosen) and display the XATTR value
	xattrVal, err := e.GetXattrs(sampleDocId, xattrKey)
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}
	log.Printf("XATTR val for doc %v: %+v", sampleDocId, xattrVal)

	// -------------------------- Add Namespace to type fields via subdoc API ------------------------------------------

	// Before adding namespace to all type fields, grab the sample doc and display the current type
	retValue, err := e.GetSubdocField(sampleDocId, "type")
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}
	log.Printf("%v type (before): %+v", sampleDocId, retValue)

	// Add a namespace to all type fields via subdoc API so that if the type was previously "airline" it will be
	// changed to "foo-component:airline"
	if err := e.AddNameSpaceToTypeFieldViaSubdoc("foo-component"); err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}

	// Verify that the sample doc has the new type
	retValue, err = e.GetSubdocField(sampleDocId, "type")
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}
	log.Printf("%v type (after): %+v", sampleDocId, retValue)


}
