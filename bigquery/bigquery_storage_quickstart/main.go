// Copyright 2018 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// This quickstart application demonstrates usage of the BigQuery Storage read API.
// The sample demonstrates features such as column projection (limiting the output
// to a subset of a table's columns), column filtering (using simple predicates to
// filter records on the server side), establishing the snapshot time
// (reading data from the table at a specific point in time), and decoding Avro
// row blocks using the third party goavro library.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	gax "github.com/googleapis/gax-go"
	"github.com/linkedin/goavro"
	"google.golang.org/grpc"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1beta1"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1beta1"
)

// The BigQuery Storage API may send message blocks up to 10MB in size, which is larger than the default message size.
var rpcOpts = gax.WithGRPCOptions(
	grpc.MaxCallRecvMsgSize(1024 * 1024 * 11),
)

// Command-line flags.
var (
	projectID = flag.String("project_id", "",
		"Cloud Project for attributing usage costs, in projects/<projectid> format")
	snapShotMs = flag.Int64("snapshot_ms", 0,
		"Snapshot time to use for reads, represented in epoch milliseconds format.  Default behavior reads current data.")
)

// main loop controls overall program flow.
func main() {
	flag.Parse()
	ctx := context.Background()
	bqStorageClient, err := bqStorage.NewBigQueryStorageClient(ctx)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer bqStorageClient.Close()

	// Determine the parent project we'll use for the read session.  The session may exist
	// in a different project than the table being read.
	parentProject := os.Getenv("GOLANG_SAMPLES_PROJECT_ID")
	if *projectID != "" {
		parentProject = *projectID
	}
	if parentProject == "" {
		log.Fatalf("No parent project for the read session specified.  Use the --project_id flag to specify, or set the GOLANG_SAMPLES_PROJECT_ID environment variable.")
	}

	// This example uses baby name data from the public datasets.
	readTable := &bqStoragepb.TableReference{
		ProjectId: "bigquery-public-data",
		DatasetId: "usa_names",
		TableId:   "usa_1910_current",
	}

	// We limit the output columns to a subset of those allowed in the table, and set
	// a simple filter to only report names from the state of Washington (WA).
	tableReadOptions := &bqStoragepb.TableReadOptions{
		SelectedFields: []string{"name", "number", "state"},
		RowRestriction: "state = \"WA\"",
	}

	readSessionRequest := &bqStoragepb.CreateReadSessionRequest{
		Parent:         fmt.Sprintf("projects/%s", parentProject),
		TableReference: readTable,
		ReadOptions:    tableReadOptions,
	}

	// Set a snapshot time if it's been specified.
	if *snapShotMs > 0 {
		ts, err := ptypes.TimestampProto(time.Unix(0, *snapShotMs*1000))
		if err != nil {
			log.Fatalf("Invalid snapshot millis (%d): %v", *snapShotMs, err)
		}
		readSessionRequest.TableModifiers = &bqStoragepb.TableModifiers{
			SnapshotTime: ts,
		}
	}

	// Create the session from the request.
	session, err := bqStorageClient.CreateReadSession(ctx, readSessionRequest, rpcOpts)
	if err != nil {
		log.Fatalf("couldn't create session: %v", err)
	}

	// We'll use only a single stream for reading data from the table.  Because of dynamic sharding,
	// this will yield all the rows in the table.  However, if you wanted to fan out multiple readers
	// you could do so by having a reader process each individual stream.
	readStream := session.GetStreams()[0]

	ch := make(chan *bqStoragepb.AvroRows)

	// Use a waitgroup to coordinate the reading and decoding goroutines.
	var wg sync.WaitGroup

	// Start the reading in one goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := processStream(ctx, bqStorageClient, readStream, ch)
		if err != nil {
			log.Printf("Stream Read Failure: %v", err)
		}
		close(ch)
	}()

	// Start Avro processing and decoding in another goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := processAvro(ctx, session.GetAvroSchema().GetSchema(), ch)
		if err != nil {
			log.Fatalf("Error processing avro: %v", err)
		}
	}()

	// Wait until both the reading and decoding goroutines complete.
	wg.Wait()

}

// print the decoded row datum.
func printDatum(d interface{}) {
	m, ok := d.(map[string]interface{})
	if !ok {
		log.Printf("failed type assertion: %v", d)
	}
	// Go's map implementation returns keys in a non-predictable ordering, so we sort the keys before accessing.
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s: %-20v ", key, valueFromTypeMap(m[key]))
	}
	fmt.Println()
}

// valueFromTypeMap returns the first value/key in the type map.  This function is only
// suitable for simple schemas, complex typing such as arrays and records necessitate
// a more robust implementation.  See the goavro library and the Avro specification for
// more information.
func valueFromTypeMap(field interface{}) interface{} {
	m, ok := field.(map[string]interface{})
	if ok {
		for _, v := range m {
			// Return the first key encountered.  A more robust implementation would
			// likely select based on some prioritization among logical types.
			return v
		}
	}
	return nil
}

// processStream reads rows from a single storage Stream, and sends the Avro data blocks to a channel.
// This function will retry on transient stream failures and bookmark progress to avoid re-reading data
// that's already been successfully transmitted.
func processStream(ctx context.Context, client *bqStorage.BigQueryStorageClient, st *bqStoragepb.Stream, ch chan<- *bqStoragepb.AvroRows) error {
	var offset int64
	streamRetry := 3

DoneLoop:
	for {
		// Send the initiating request to start streaming row blocks.
		rowStream, err := client.ReadRows(ctx, &bqStoragepb.ReadRowsRequest{
			ReadPosition: &bqStoragepb.StreamPosition{
				Stream: st,
				Offset: offset,
			}}, rpcOpts)
		if err != nil {
			return fmt.Errorf("Couldn't invoke ReadRows: %v", err)
		}

		// Process the streamed responses.
		for {
			r, err := rowStream.Recv()
			if err == io.EOF {
				break DoneLoop
			}
			if err != nil {
				streamRetry--
				if streamRetry <= 0 {
					return fmt.Errorf("Exhausted retries.  Failure: %v", err)
				}
			}

			rc := r.GetAvroRows().GetRowCount()
			if rc > 0 {
				// bookmark our progress in case of retries and send the rowblock on the channel.
				offset = offset + rc
				ch <- r.GetAvroRows()
			}
		}
	}
	return nil
}

// processAvro receives row blocks from a channel, and uses the provided Avro schema to decode the
// blocks into individual row messages for printing.  Will continue to run until the channel is
// closed or the provided context is cancelled.
func processAvro(ctx context.Context, schema string, ch <-chan *bqStoragepb.AvroRows) error {

	// Establish a decoder that can process blocks of messages using the reference schema.
	// All blocks share the same schema, so the decoder can be long-lived.
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatalf("couldn't create codec: %v", err)
	}

DecodeLoop:
	for {
		select {
		case <-ctx.Done():
			// context was cancelled.  Stop.
			break DecodeLoop
		case rows, ok := <-ch:
			if !ok {
				// Channel closed, no further avro messages.  Stop.
				break DecodeLoop
			}
			undecoded := rows.GetSerializedBinaryRows()
			for len(undecoded) > 0 {

				datum, remainingBytes, err := codec.NativeFromBinary(undecoded)

				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("Decoding error with %d bytes remaining: %v", len(undecoded), err)
				}

				// print the decoded row datum.
				printDatum(datum)
				undecoded = remainingBytes
			}
		}
	}
	return nil
}
