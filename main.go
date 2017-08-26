package main


import (
	"fmt"
	"gopkg.in/couchbase/gocb.v1"
	"log"
)

type Example struct {
	ClusterConnection *gocb.Cluster

}

func NewExample() {
	return &Example{}
}

func (e *Example) Connect(connSpecStr string) error {

}

func CopyBucket() error {

	cluster, err := gocb.Connect("couchbase://localhost")
	if err != nil {
		return err
	}

	sourceBucket, err := cluster.OpenBucket("travel-sample", "password")
	if err != nil {
		return err
	}

	targetBucket, err := cluster.OpenBucket("travel-sample-copy", "password")
	if err != nil {
		return err
	}

	result := map[string]interface{}{}
	_, err = sourceBucket.Get("airline_10", &result)
	if err != nil {
		return err
	}

	log.Printf("Result: %+v", result)

	err = sourceBucket.Manager("", "").CreatePrimaryIndex("", true, false)
	if err != nil {
		return err
	}

	// Get the doc ID and the doc body in a single query
	query := gocb.NewN1qlQuery("SELECT META(`travel-sample`).id,* FROM `travel-sample`")
	rows, err := sourceBucket.ExecuteN1qlQuery(query, nil)
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

		_, err := targetBucket.Insert(rowIdStr, docRaw, 0)
		if err != nil {
			return err
		}


	}

	return nil

}




func main() {

	err := CopyBucket()
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}

	err := AddXattrs()
	if err != nil {
		panic(fmt.Errorf("Error: %v", err))
	}




}