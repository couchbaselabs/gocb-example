
Some code to demonstrate the following GoCB usage:

- Copies the data from a source bucket to a target bucket
    - Iterate docs via N1QL query
    - Iterate docs via View query
- Anonymizes the document contents via [json-anonymizer](https://github.com/tleyden/json-anonymizer)
- Add an XATTR (Extended Attribute) to each doc
- Manipulate fields via Subdoc API

## Setup

```
go get github.com/couchbaselabs/gocb-example
```


## References

* https://developer.couchbase.com/documentation/server/current/sdk/go/start-using-sdk.html
* https://github.com/couchbaselabs/devguide-examples/blob/master/go/subdocument.go
* https://github.com/couchbase/gocb/blob/master/bucket_subdoc_test.go