package main


import (
	"fmt"
	"gopkg.in/couchbase/gocb.v1"
	"log"
)

type Example struct {
	ClusterConnection *gocb.Cluster
	SourceBucket *gocb.Bucket
	TargetBucket *gocb.Bucket
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

func (e *Example) CopyBucket() (err error) {

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


	// row := TravelSampleRow{}
	row := map[string]interface{}{}
	for rows.Next(&row) {

		// fmt.Printf("Row: %+v\n", row)

		// Get row ID
		rowIdRaw, ok := row["id"]
		if !ok {
			return fmt.Errorf("Row does not have id field")
		}
		rowIdStr, ok := rowIdRaw.(string)
		if !ok {
			return fmt.Errorf("Row id field not of expected type")
		}
		log.Printf("rowID: %v\n", rowIdStr)

		// Get row document
		docRaw, ok := row["travel-sample"]
		if !ok {
			return fmt.Errorf("Row does not have doc field")
		}
		log.Printf("docRaw: %+v", docRaw)

		_, err := e.TargetBucket.Insert(rowIdStr, docRaw, 0)
		if err != nil {
			return err
		}


	}

	return nil

}

func (e *Example) AddXattrs() error {


	return nil

}


func main() {

	e := NewExample()
	e.Connect("couchbase://localhost")

	if err := e.CopyBucket(); err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}

	if err := e.AddXattrs(); err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}




}