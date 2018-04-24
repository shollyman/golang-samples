// Copyright 2016 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package snippets contains snippets for the Google BigQuery Go package.
package snippets

import (
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

func createDataset(client *bigquery.Client, datasetID string) error {
	ctx := context.Background()
	// [START bigquery_create_dataset]
	if err := client.Dataset(datasetID).Create(ctx, &bigquery.DatasetMetadata{}); err != nil {
		return err
	}
	// [END bigquery_create_dataset]
	return nil
}

func updateDatasetDescription(client *bigquery.Client, datasetID string) error {
	ctx := context.Background()
	// [START bigquery_update_dataset_description]
	ds := client.Dataset(datasetID)
	original, err := ds.Metadata(ctx)
	if err != nil {
		return err
	}
	changes := bigquery.DatasetMetadataToUpdate{
		Description: "Updated Description.",
	}
	if _, err = ds.Update(ctx, changes, original.ETag); err != nil {
		return err
	}
	// [END bigquery_update_dataset_description]
	return nil
}

func updateDatasetDefaultExpiration(client *bigquery.Client, datasetID string) error {
	ctx := context.Background()
	// [START bigquery_update_dataset_expiration]
	ds := client.Dataset(datasetID)
	original, err := ds.Metadata(ctx)
	if err != nil {
		return err
	}
	changes := bigquery.DatasetMetadataToUpdate{
		DefaultTableExpiration: 24 * time.Hour,
	}
	if _, err := client.Dataset(datasetID).Update(ctx, changes, original.ETag); err != nil {
		return err
	}
	// [END bigquery_update_dataset_expiration]
	return nil
}

func updateDatasetAccessControl(client *bigquery.Client, datasetID string) error {
	ctx := context.Background()
	// [START bigquery_update_dataset_access]
	ds := client.Dataset(datasetID)
	original, err := ds.Metadata(ctx)
	if err != nil {
		return err
	}
	// Append a new access control entry to the existing access list
	changes := bigquery.DatasetMetadataToUpdate{
		Access: append(original.Access, &bigquery.AccessEntry{
			Role:       bigquery.ReaderRole,
			EntityType: bigquery.UserEmailEntity,
			Entity:     "sample.bigquery.dev@gmail.com"},
		),
	}

	// Leverage the ETag for the update to assert there's been no modifications to the
	// dataset since the metadata was originally read.
	if _, err := ds.Update(ctx, changes, original.ETag); err != nil {
		return err
	}
	// [END bigquery_update_dataset_access]
	return nil
}

func deleteEmptyDataset(client *bigquery.Client, datasetID string) error {
	ctx := context.Background()
	// [START bigquery_delete_dataset]
	if err := client.Dataset(datasetID).Delete(ctx); err != nil {
		return fmt.Errorf("Failed to delete dataset: %v", err)
	}
	// [END bigquery_delete_dataset]
	return nil
}

func listDatasets(client *bigquery.Client) error {
	ctx := context.Background()
	// [START bigquery_list_datasets]
	it := client.Datasets(ctx)
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		fmt.Println(dataset.DatasetID)
	}
	// [END bigquery_list_datasets]
	return nil
}

// [START bigquery_create_table]

// Item represents a row item.
type Item struct {
	Name  string
	Count int
}

// Save implements the ValueSaver interface.
func (i *Item) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"Name":  i.Name,
		"Count": i.Count,
	}, "", nil
}

// [END bigquery_create_table]

func createTable(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	// [START bigquery_create_table]
	schema, err := bigquery.InferSchema(Item{})
	if err != nil {
		return err
	}
	table := client.Dataset(datasetID).Table(tableID)
	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		return err
	}
	// [END bigquery_create_table]
	return nil
}

func listTables(client *bigquery.Client, w io.Writer, datasetID string) error {
	ctx := context.Background()
	// [START bigquery_list_tables]
	ts := client.Dataset(datasetID).Tables(ctx)
	for {
		t, err := ts.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "Table: %q\n", t.TableID)
	}
	// [END bigquery_list_tables]
	return nil
}

func insertRows(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	// [START bigquery_insert_stream]
	u := client.Dataset(datasetID).Table(tableID).Uploader()
	items := []*Item{
		// Item implements the ValueSaver interface.
		{Name: "n1", Count: 7},
		{Name: "n2", Count: 2},
		{Name: "n3", Count: 1},
	}
	if err := u.Put(ctx, items); err != nil {
		return err
	}
	// [END bigquery_insert_stream]
	return nil
}

func listRows(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	// [START bigquery_list_rows]
	q := client.Query(fmt.Sprintf(`
		SELECT name, count
		FROM %s.%s
		WHERE count >= 5
	`, datasetID, tableID))
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(row)
	}
	// [END bigquery_list_rows]
	return nil
}

func asyncQuery(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	// [START bigquery_async_query]
	q := client.Query(fmt.Sprintf(`
		SELECT name, count
		FROM %s.%s
	`, datasetID, tableID))
	job, err := q.Run(ctx)
	if err != nil {
		return err
	}

	// Wait until async querying is done.
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}

	it, err := job.Read(ctx)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(row)
	}
	// [END bigquery_async_query]
	return nil
}

func browseTable(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	// [START bigquery_browse_table]
	table := client.Dataset(datasetID).Table(tableID)
	it := table.Read(ctx)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(row)
	}
	// [END bigquery_browse_table]
	return nil
}

func copyTable(client *bigquery.Client, datasetID, srcID, dstID string) error {
	ctx := context.Background()
	// [START bigquery_copy_table]
	dataset := client.Dataset(datasetID)
	copier := dataset.Table(dstID).CopierFrom(dataset.Table(srcID))
	copier.WriteDisposition = bigquery.WriteTruncate
	job, err := copier.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	// [END bigquery_copy_table]
	return nil
}

func deleteTable(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	// [START bigquery_delete_table]
	table := client.Dataset(datasetID).Table(tableID)
	if err := table.Delete(ctx); err != nil {
		return err
	}
	// [END bigquery_delete_table]
	return nil
}

func importFromGCS(client *bigquery.Client, datasetID, tableID, gcsURI string) error {
	ctx := context.Background()
	// [START bigquery_import_from_gcs]
	// For example, "gs://data-bucket/path/to/data.csv"
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.AllowJaggedRows = true
	// TODO: set other options on the GCSReference.

	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateNever
	// TODO: set other options on the Loader.

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	// [END bigquery_import_from_gcs]
	return nil
}

func importFromFile(client *bigquery.Client, datasetID, tableID, filename string) error {
	ctx := context.Background()
	// [START bigquery_import_from_file]
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	source := bigquery.NewReaderSource(f)
	source.AllowJaggedRows = true
	// TODO: set other options on the GCSReference.

	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(source)
	loader.CreateDisposition = bigquery.CreateNever
	// TODO: set other options on the Loader.

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	// [END bigquery_import_from_file]
	return nil
}

func exportToGCS(client *bigquery.Client, datasetID, tableID, gcsURI string) error {
	ctx := context.Background()
	// [START bigquery_export_gcs]
	// For example, "gs://data-bucket/path/to/data.csv"
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","

	extractor := client.Dataset(datasetID).Table(tableID).ExtractorTo(gcsRef)
	extractor.DisableHeader = true
	job, err := extractor.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	// [END bigquery_export_gcs]
	return nil
}

func exportSampleTableAsCSV(client *bigquery.Client, gcsURI string) error {
	ctx := context.Background()
	// [START bigquery_extract_table]
	srcProject := "bigquery-public-data"
	srcDataset := "samples"
	srcTable := "shakespeare"

	// For example, gcsUri = "gs://mybucket/shakespeare.csv"
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","

	extractor := client.DatasetInProject(srcProject, srcDataset).Table(srcTable).ExtractorTo(gcsRef)
	extractor.DisableHeader = true
	// run the job in the US location
	extractor.Location = "US"

	job, err := extractor.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	// [END bigquery_extract_table]
	return nil
}

func exportSampleTableAsJSON(client *bigquery.Client, gcsURI string) error {
	ctx := context.Background()
	// [START bigquery_extract_table_json]
	srcProject := "bigquery-public-data"
	srcDataset := "samples"
	srcTable := "shakespeare"

	// For example, gcsUri = "gs://mybucket/shakespeare.json"
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.DestinationFormat = bigquery.JSON

	extractor := client.DatasetInProject(srcProject, srcDataset).Table(srcTable).ExtractorTo(gcsRef)
	// run the job in the US location
	extractor.Location = "US"

	job, err := extractor.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	// [END bigquery_extract_table_json]
	return nil
}

func importJSONExplicitSchema(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	// [START bigquery_load_table_gcs_json]
	gcsRef := bigquery.NewGCSReference("gs://cloud-samples-data/bigquery/us-states/us-states.json")
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.Schema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "post_abbr", Type: bigquery.StringFieldType},
	}
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("Job completed with error: %v", status.Err())
	}
	// [END bigquery_load_table_gcs_json]
	return nil
}

func importJSONAutodetectSchema(client *bigquery.Client, datasetID, tableID string) error {
	ctx := context.Background()
	// [START bigquery_load_table_gcs_json_autodetect]
	gcsRef := bigquery.NewGCSReference("gs://cloud-samples-data/bigquery/us-states/us-states.json")
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.AutoDetect = true
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("Job completed with error: %v", status.Err())
	}
	// [END bigquery_load_table_gcs_json_autodetect]
	return nil
}
