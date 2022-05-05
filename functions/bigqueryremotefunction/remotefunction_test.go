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

package bigqueryremotefunction

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestDetectUDF(t *testing.T) {

	testcases := []struct {
		description  string
		req          *UDFRequest
		wantResponse *UDFResponse
	}{
		{
			description: "empty",
			req: &UDFRequest{
				Caller: "foo",
			},
			wantResponse: &UDFResponse{
				ErrorMessage: "no calls in request",
			},
		},
		{
			description: "single lang",
			req: &UDFRequest{
				Calls: []CallData{
					{"hola"},
				},
			},
			wantResponse: &UDFResponse{},
		},
	}

	ctx := context.Background()
	for _, tc := range testcases {

		got := detectLanguage(ctx, tc.req)
		if diff := cmp.Diff(got, tc.wantResponse); diff != "" {
			t.Errorf("%s: -got, +want: %s", tc.description, diff)
		}
	}
}
