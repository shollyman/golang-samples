// Copyright 2017 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"

	"golang.org/x/net/context"

	"gocloud-exp/firestore"

	"google.golang.org/api/iterator"
)

func prepareRetrieve(ctx context.Context, client *firestore.Client) error {
	// [START fs_retrieve_create_examples]
	cities := []struct {
		id string
		c  City
	}{
		{id: "Tokyo", c: City{Name: "Tokyo", Country: "Japan", Capital: true, Population: 13617445}},
		{id: "NYC", c: City{Name: "New York City", Country: "USA", Capital: false, Population: 8550405}},
		{id: "Beijing", c: City{Name: "Beijing", Country: "China", Capital: true, Population: 21700000}},
		{id: "London", c: City{Name: "London", Country: "UK", Capital: true, Population: 8673713}},
	}
	for _, c := range cities {
		_, err := client.Collection("cities").Doc(c.id).Set(ctx, c.c)
		if err != nil {
			return err
		}
	}
	// [END fs_retrieve_create_examples]
	return nil
}

func docAsMap(ctx context.Context, client *firestore.Client) (map[string]interface{}, error) {
	// [START fs_get_doc_as_map]
	dsnap, err := client.Collection("cities").Doc("Beijing").Get(ctx)
	if err != nil {
		return nil, err
	}
	m := dsnap.Data()
	fmt.Printf("Document data: %#v\n", m)
	// [END fs_get_doc_as_map]
	return m, nil
}

func docAsEntity(ctx context.Context, client *firestore.Client) (*City, error) {
	// [START fs_get_doc_as_entity]
	dsnap, err := client.Collection("cities").Doc("Beijing").Get(ctx)
	if err != nil {
		return nil, err
	}
	var c City
	dsnap.DataTo(&c)
	fmt.Printf("Document data: %#v\n", c)
	// [END fs_get_doc_as_entity]
	return &c, nil
}

func multipleDocs(ctx context.Context, client *firestore.Client) error {
	// [START fs_get_multiple_docs]
	fmt.Println("All capital cities:")
	iter := client.Collection("cities").Where("capital", "==", true).Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(doc.Data())
	}
	// [END fs_get_multiple_docs]
	return nil
}

func allDocs(ctx context.Context, client *firestore.Client) error {
	// [START fs_get_all_docs]
	fmt.Println("All cities:")
	iter := client.Collection("cities").Documents(ctx)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(doc.Data())
	}
	// [END fs_get_all_docs]
	return nil
}
