package main

import (
	"fmt"
	"log"
	"time"

	"gopkg.in/couchbase/gocb.v1"
)

const (

	// XATTRS will be stored under this key
	xattrKey = "Metadata"

	// A sample doc ID for inspection purposes
	sampleDocId = "airline_10123"

	designDoc = "all_docs"

	viewName = designDoc
)

// A custom function type that takes a slice of doc ids and a slice of doc bodies and returns an error
type DocProcessor func(docId []string, doc []interface{}) error

type BucketSpec struct {
	Name          string
	Password      string
	AdminPassword string // Used to create bucket manager for adding views
}

// A struct to keep references to the cluster connection and open buckets
type ExampleApp struct {

	// Use N1QL?  If false, use views
	UseN1ql bool

	ClusterConnection *gocb.Cluster
	SourceBucketSpec  BucketSpec
	TargetBucketSpec  BucketSpec
	SourceBucket      *gocb.Bucket
	TargetBucket      *gocb.Bucket
}

// Create a new ExampleApp
func NewExample(sourceBucketSpec, targetBucketSpec BucketSpec) *ExampleApp {
	return &ExampleApp{
		UseN1ql:          false,
		SourceBucketSpec: sourceBucketSpec,
		TargetBucketSpec: targetBucketSpec,
	}
}

// Connect to the cluster and buckets, create primary indexes
func (e *ExampleApp) Connect(connSpecStr string) (err error) {

	// Connect to cluster
	e.ClusterConnection, err = gocb.Connect(connSpecStr)
	if err != nil {
		return err
	}

	// Connect to Source Bucket
	e.SourceBucket, err = e.ClusterConnection.OpenBucket(
		e.SourceBucketSpec.Name,
		e.SourceBucketSpec.Password,
	)
	if err != nil {
		return err
	}

	// Connect to Target Bucket
	e.TargetBucket, err = e.ClusterConnection.OpenBucket(
		e.TargetBucketSpec.Name,
		e.TargetBucketSpec.Password,
	)
	if err != nil {
		return err
	}

	switch e.UseN1ql {
	case true:
		// Create primary index on source bucket
		err = e.SourceBucket.Manager("", "").CreatePrimaryIndex("", true, false)
		if err != nil {
			return err
		}

		// Create primary index on target bucket
		err = e.TargetBucket.Manager("", "").CreatePrimaryIndex("", true, false)
		if err != nil {
			return err
		}

	case false: // use views

		// Create design doc
		gocbDesignDoc := &gocb.DesignDocument{
			Name:  designDoc,
			Views: map[string]gocb.View{},
		}

		// Create javascript map function that emits doc id and doc body
		// NOTE: this is not efficient to emit the entire doc in the view query.
		// The more efficient and recommended way is to just emit the id, and do a separate lookup for the doc body.
		mapFunction := `function(doc, meta) {
               emit(meta.id, doc)
        }`
		// Create View
		gocbView := gocb.View{
			Map: mapFunction,
		}

		// Add view to design doc
		gocbDesignDoc.Views[viewName] = gocbView

		// Add design doc + view to source bucket
		sourceBucketManager := e.SourceBucket.Manager("Administrator", e.SourceBucketSpec.AdminPassword)
		if err := sourceBucketManager.UpsertDesignDocument(gocbDesignDoc); err != nil {
			return err
		}

		// Add design doc + view to target bucket
		targetBucketManager := e.TargetBucket.Manager("Administrator", e.TargetBucketSpec.AdminPassword)
		if err := targetBucketManager.UpsertDesignDocument(gocbDesignDoc); err != nil {
			return err
		}

	}

	return nil
}

// Copies source bucket to target bucket, inserting XATTRS in target docs
func (e *ExampleApp) CopyBucketAddXATTRS() (err error) {

	// Create a post-insert callback function that will be invoked on
	// every document that is copied from the source bucket and inserted into the target bucket.
	// It adds the "DateCopied" XATTR to the doc.
	postInsertCallback := func(docIds []string, docs []interface{}) error {

		for _, docId := range docIds {

			// Get existing doc in order to get CAS
			cas, err := e.TargetBucket.Get(docId, nil)
			if err != nil {
				return err
			}

			// The XATTR value contains metadata about the document: the bucket it was originally copied from
			// as well as the date it was copied.
			xattrVal := map[string]interface{}{
				"DateCopied":     time.Now(),
				"UpstreamSource": e.SourceBucket.Name(),
			}

			// Create CAS-safe XATTR mutation
			builder := e.TargetBucket.MutateInEx(docId, gocb.SubdocDocFlagNone, gocb.Cas(cas), uint32(0)).
				UpsertEx(xattrKey, xattrVal, gocb.SubdocFlagXattr)

			// Execute mutation
			_, err = builder.Execute()
			if err != nil {
				return err
			}

		}


		return nil
	}

	// Copy the bucket and pass the post-insert callback function
	if err := e.CopyBucketWithCallback(postInsertCallback); err != nil {
		return err
	}

	return nil

}

func (e *ExampleApp) CopyBucket() (err error) {
	if err := e.CopyBucketWithCallback(nil); err != nil {
		return err
	}

	return nil
}

func TableScanN1qlQuery(bucketName string) string {
	// Get the doc ID and the doc body in a single query -- eg:
	// "SELECT META(`travel-sample`).id,* FROM `travel-sample`"
	//         ^^^^^^^^^^^^ doc id      ^ doc body
	return fmt.Sprintf(
		"SELECT META(`%s`).id,* FROM `%s`",
		bucketName,
		bucketName,
	)
}

func (e *ExampleApp) CopyBucketWithCallback(postInsertCallback DocProcessor) (err error) {

	// A docprocesser callback that *wraps* the postInsertCallback to do the following:
	// - Insert the doc into the target bucket
	// - Invoke the postInsertCallback
	copyEachDoc := func(docIds []string, docs []interface{}) error {

		switch len(docIds) {
		case 1:

			// Insert the doc into the target bucket
			_, err := e.TargetBucket.Insert(docIds[0], docs[0], 0)
			if err != nil {
				return fmt.Errorf("Error inserting doc id: %v.  Err: %v", docIds[0], err)
			}

			if postInsertCallback != nil {
				return postInsertCallback(docIds, docs)
			}

			return nil

		default:

			// copy docs via bulk ops
			var items []gocb.BulkOp

			for i, docId := range docIds {
				item := &gocb.InsertOp{
					Key:   docId,
					Value: docs[i],
				}
				items = append(items, item)
			}

			// Do the underlying bulk operation
			log.Printf("Inserting %v items", len(items))
			if err := e.TargetBucket.Do(items); err != nil {
				return err
			}
			log.Printf("Inserted %v items", len(items))

			// Make sure all bulk ops succeeded
			for _, item := range items {
				insertItem := item.(*gocb.InsertOp)
				if insertItem.Err != nil {
					return insertItem.Err
				}
			}

			if postInsertCallback != nil {
				return postInsertCallback(docIds, docs)
			}

			return nil

		}

	}

	return e.ForEachDocIdSourceBucket(copyEachDoc)

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
func (e *ExampleApp) ForEachDocIdTargetBucket(postInsertCallback DocProcessor) (err error) {
	if e.UseN1ql {
		return e.ForEachDocIdBucketN1ql(postInsertCallback, e.TargetBucket)
	} else {
		return e.ForEachDocIdBucketViews(postInsertCallback, e.TargetBucket)
	}
}

func (e *ExampleApp) ForEachDocIdSourceBucket(postInsertCallback DocProcessor) (err error) {
	if e.UseN1ql {
		return e.ForEachDocIdBucketN1ql(postInsertCallback, e.SourceBucket)
	} else {
		return e.ForEachDocIdBucketViews(postInsertCallback, e.SourceBucket)
	}
}

// Loop over each doc in the bucket and callback the doc id processor with the doc id
func (e *ExampleApp) ForEachDocIdBucketN1ql(docProcessor DocProcessor, bucket *gocb.Bucket) (err error) {

	log.Printf("Performing operation over bucket: %v", bucket.Name())

	// Get the doc ID and the doc body in a single query
	query := gocb.NewN1qlQuery(TableScanN1qlQuery(bucket.Name()))
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
		docRaw, ok := row[bucket.Name()]
		if !ok {
			return fmt.Errorf("Row does not have doc field: %+v.  Row: %+v", bucket.Name(), row)
		}

		// Invoke the doc processor callback
		if err := docProcessor([]string{rowIdStr}, []interface{}{docRaw}); err != nil {
			return err
		}

	}

	return nil
}

// Loop over each doc in the bucket and callback the doc id processor with the doc id
// TODO: make sure this works if the view is in the process of being indexed
func (e *ExampleApp) ForEachDocIdBucketViews(docProcessor DocProcessor, bucket *gocb.Bucket) (err error) {

	log.Printf("Performing operation over bucket via view query: %v", bucket.Name())

	viewQuery := gocb.NewViewQuery(designDoc, viewName)

	//	// TODO: if this page size is larger, it will return "panic: Error: queue overflowed" when doing bulk inserts.  Should handle that case.
	pageSize := uint(1000)
	skip := uint(0)

	for {

		viewQuery.Limit(pageSize)
		viewQuery.Skip(skip)

		log.Printf("Calling ExecuteViewQuery: %v", viewQuery)
		viewResults, err := bucket.ExecuteViewQuery(viewQuery)
		if err != nil {
			return fmt.Errorf("Error executing viewQuery: %v.  Err: %v", viewQuery, err)
		}

		numResultsProcessed := 0
		row := map[string]interface{}{}
		docIds := []string{}
		docs := []interface{}{}

		for {

			if gotRow := viewResults.Next(&row); gotRow == false {
				log.Printf("No more rows in view result.")
				if numResultsProcessed == 0 {
					// No point in going to the next page, since this page had 0 results
					return nil
				}
				// We've processed all results in this page, break out of inner for loop to process another page of results
				break
			}

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
			docRaw, ok := row["value"]
			if !ok {
				return fmt.Errorf("Row does not have doc field: %+v.  Row: %+v", bucket.Name(), row)
			}

			docIds = append(docIds, rowIdStr)
			docs = append(docs, docRaw)

			skip += 1
			numResultsProcessed += 1

		}

		// Invoke the doc processor callback
		if err := docProcessor(docIds, docs); err != nil {
			return err
		}

	}

	log.Printf("finished looping over viewResults")

	return nil
}

func (e *ExampleApp) AddNameSpaceToTypeFieldViaSubdoc(namespacePrefix string) (err error) {

	// Iterate over all docs and update the type field to app:<existing_type>
	// TODO: handle errors like "panic: Error: temporary failure occurred, try again later"
	appendNamespaceToTypeField := func(docIds []string, docs []interface{}) error {

		for _, docId := range docIds {

			currentValueOfTypeField, err := e.GetSubdocField(docId, "type")
			if err != nil {
				return fmt.Errorf("Error getting subdoc field: %v.  Doc: %v", err, docId)
			}

			newValueOfTypeField := fmt.Sprintf("%v:%v", namespacePrefix, currentValueOfTypeField)

			err = e.SetSubdocField(docId, "type", newValueOfTypeField)
			if err != nil {
				return fmt.Errorf("Error setting subdoc field: %v.  Doc: %v", err, docId)
			}

		}

		return nil
	}

	if err := e.ForEachDocIdTargetBucket(appendNamespaceToTypeField); err != nil {
		return err
	}

	return nil
}

func main() {

	sourceBucketSpec := BucketSpec{
		Name:          "travel-sample",
		Password:      "password",
		AdminPassword: "password",
	}
	targetBucketSpec := BucketSpec{
		Name:          "travel-sample-copy",
		Password:      "password",
		AdminPassword: "password",
	}
	e := NewExample(sourceBucketSpec, targetBucketSpec)
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
