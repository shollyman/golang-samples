// Copyright 2017 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Sample firestore_quickstart demonstrates how to connect to Firestore, and add and list documents.
package main

import (
	"fmt"
	"log"

	"golang.org/x/net/context"

	"google.golang.org/api/iterator"

	"gocloud-exp/firestore"
)

func main() {
	ctx := context.Background()

	// [START fs_initialize]
	// Set your Google Cloud Platform project ID.
	const projectID = "YOUR_PROJECT_ID"

	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	// [END fs_initialize]

	// [START fs_add_data_1]
	_, err = client.Collection("employees").Doc("alovelace").Set(ctx, map[string]interface{}{
		"first": "Ada",
		"last":  "Lovelace",
		"born":  1815,
	})
	if err != nil {
		log.Fatalf("Failed adding alovelace: %v", err)
	}
	// [END fs_add_data_1]

	// [START fs_add_data_2]
	_, err = client.Collection("employees").Doc("aturing").Set(ctx, map[string]interface{}{
		"first":  "Alan",
		"middle": "Mathison",
		"last":   "Turing",
		"born":   1912,
	})
	if err != nil {
		log.Fatalf("Failed adding aturing: %v", err)
	}
	// [END fs_add_data_2]

	iter := client.Collection("employees").Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Failed to iterate: %v", err)
		}
		fmt.Println(doc.Data())
	}
}
