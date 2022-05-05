// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [START bigquery_remote_function]

// Package bigqueryremotefunction demonstrates exposing the Cloud Translate
// API to BigQuery as remote UDFs.
package bigqueryremotefunction

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	translate "cloud.google.com/go/translate/apiv3"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	translatepb "google.golang.org/genproto/googleapis/cloud/translate/v3"
)

var translationClient *translate.TranslationClient
var projectID string

func init() {

	// Setup references tothe project ID and the translation client for re-use.
	ctx := context.Background()
	// TRANSLATION_PROJECT is populated by setting env variables when deploying the function(s).
	if id, ok := os.LookupEnv("TRANSLATION_PROJECT"); ok {
		projectID = id
	}
	if client, err := translate.NewTranslationClient(ctx); err == nil {
		translationClient = client
	}
	RegisterUDF("DetectLanguage", detectLanguage)
	//RegisterUDF("TranslateText", translateText)
}

// RegisterUDF is a utility function to abstract request management from UDF logic.
func RegisterUDF(name string, f func(context.Context, *UDFRequest) *UDFResponse) {
	functions.HTTP(name, func(resp http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var udfReq *UDFRequest
		if err := json.NewDecoder(r.Body).Decode(udfReq); err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			return
		}
		udfResp := f(ctx, udfReq)
		// Normalize the UDF response.  If the error is set, clear the response.
		if udfResp.ErrorMessage != "" {
			udfResp.Replies = nil
		}
		b, err := json.Marshal(udfResp)
		if err != nil {
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}
		// Send success HTTP and encoded response.
		resp.WriteHeader(http.StatusOK)
		resp.Write(b)
	})
}

type CallData []interface{}
type Value interface{}

// UDFRequest models the expected request format from BigQuery.
// More information:
// https://cloud.google.com/bigquery/docs/reference/standard-sql/remote-functions#input_format
type UDFRequest struct {
	RequestID          string            `json:"requestId"`
	Caller             string            `json:"caller"`
	SessionUser        string            `json:"sessionUser"`
	UserDefinedContext map[string]string `json:"userDefinedContext"`
	Calls              []CallData        `json:"calls"`
}

// UDFResponse models the expected response format that BigQuery expects from a remote UDF.
type UDFResponse struct {
	ErrorMessage string
	Replies      []interface{}
}

// detectLanguage is a UDF that performs language detection on a given string.
//
// Request:
// * A single STRING field per row.
// Response:
// * A STRING and FLOAT, representing most likely langauge and confidence.
func detectLanguage(ctx context.Context, req *UDFRequest) *UDFResponse {

	resp := &UDFResponse{}

	if len(req.Calls) == 0 {
		resp.ErrorMessage = "no calls in request"
		return resp
	}
	for k, row := range req.Calls {
		str, ok := row[0].(string)
		if !ok {
			resp.ErrorMessage = fmt.Sprintf("failed to decode string col in row %d of batch: %v", k, row[0])
			return resp
		}
		detectReq := &translatepb.DetectLanguageRequest{
			Parent:   fmt.Sprintf("projects/%s/locations/global", projectID),
			MimeType: "text/plain",
			Source: &translatepb.DetectLanguageRequest_Content{
				Content: str,
			},
		}
		detected, err := translationClient.DetectLanguage(ctx, detectReq)
		if err != nil {
			resp.ErrorMessage = fmt.Sprintf("failed to detect on input %q: %v", str, err)
			return resp
		}
		reply := []Value{"UNKNOWN", 0.0}
		if len(detected.GetLanguages()) > 0 {
			reply = []Value{detected.GetLanguages()[0].GetLanguageCode(), detected.GetLanguages()[0].GetConfidence()}
		}
		resp.Replies = append(resp.Replies, reply)
	}
	return resp
}
