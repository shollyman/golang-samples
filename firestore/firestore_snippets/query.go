// Copyright 2017 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"golang.org/x/net/context"

	"gocloud-exp/firestore"
)

func prepareQuery(ctx context.Context, client *firestore.Client) error {
	// [START fs_query_create_examples]
	cities := []struct {
		id string
		c  City
	}{
		{id: "Sao Paulo", c: City{Name: "Sao Paulo", Country: "Brazil", Capital: true, Population: 12038175}},
		{id: "Sydney", c: City{Name: "Sydney", Country: "Australia", Capital: true, Population: 4921000}},
		{id: "Paris", c: City{Name: "Paris", Country: "France", Capital: true, Population: 8673713}},
		{id: "NYC", c: City{Name: "New York City", Country: "USA", Capital: false, Population: 8550405}},
	}
	for _, c := range cities {
		if _, err := client.Collection("cities").Doc(c.id).Set(ctx, c.c); err != nil {
			return err
		}
	}
	// [END fs_query_create_examples]
	return nil
}

func createQuery(client *firestore.Client) {
	// [START fs_create_query]
	query := client.Collection("cities").Where("capital", "==", true)
	// [END fs_create_query]
	_ = query
}

func createSimpleQueries(client *firestore.Client) {
	cities := client.Collection("cities")
	// [START fs_simple_queries]
	countryQuery := cities.Where("country", "==", "Brazil")
	popQuery := cities.Where("population", "<", 8550405)
	cityQuery := cities.Where("name", ">=", "Paris")
	// [END fs_simple_queries]

	_ = countryQuery
	_ = popQuery
	_ = cityQuery
}

func createChainedQuery(client *firestore.Client) {
	cities := client.Collection("cities")
	// [START fs_chained_query]
	// Create chained where clauses to query the cities
	// in "Australia" named "Sydney".
	query := cities.Where("country", "==", "Australia").Where("name", "==", "Sydney")
	// [END fs_chained_query]

	_ = query
}

func createInvalidChainedQuery(client *firestore.Client) {
	// Note: this is an instance of a currently unsupported chained
	// query: equality with inequality. All cities in USA with population
	// over 5 million.
	// This query requires support for creation of composite indices
	// which is not currently available.
	cities := client.Collection("cities")
	// [START fs_invalid_chained_query]
	query := cities.Where("country", "==", "USA").Where("population", ">", 5000000)
	// [END fs_invalid_chained_query]

	_ = query
}

func createRangeQuery(client *firestore.Client) {
	cities := client.Collection("cities")
	// [START fs_range_query]
	query := cities.Where("country", ">=", "Brazil").Where("country", "<", "USA")
	// [END fs_range_query]

	_ = query
}

func createInvalidRangeQuery(client *firestore.Client) {
	// Note: This is an invalid range query: range operators
	// are limited to a single field.
	cities := client.Collection("cities")
	// [START fs_invalid_range_query]
	query := cities.Where("country", ">=", "Brazil").Where("population", ">", 1000000)
	// [END fs_invalid_range_query]

	_ = query
}

func createOrderByNameLimitQuery(client *firestore.Client) {
	cities := client.Collection("cities")
	// [START fs_order_by_name_limit_query]
	query := cities.OrderBy("name", firestore.Asc).Limit(3)
	// [END fs_order_by_name_limit_query]

	_ = query
}

func createOrderByNameDescLimitQuery(client *firestore.Client) {
	cities := client.Collection("cities")
	// [START fs_order_by_name_desc_limit_query]
	query := cities.OrderBy("name", firestore.Desc).Limit(3)
	// [END fs_order_by_name_desc_limit_query]

	_ = query
}

func createRangeWithOrderByQuery(client *firestore.Client) {
	cities := client.Collection("cities")
	// [START fs_range_order_by_query]
	query := cities.Where("population", ">", 2500000).OrderBy("population", firestore.Asc)
	// [END fs_range_order_by_query]

	_ = query
}

func createInvalidRangeWithOrderByQuery(client *firestore.Client) {
	cities := client.Collection("cities")
	// [START fs_invalid_range_order_by_query]
	// Note: This is an invalid query. It violates the constraint that range
	// and order by are required to be on the same field.
	query := cities.Where("population", ">", 2500000).OrderBy("country", firestore.Asc)
	// [END fs_invalid_range_order_by_query]

	_ = query
}
