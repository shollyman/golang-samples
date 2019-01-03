// Copyright 2018 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package main

import (
	"testing"

	"github.com/GoogleCloudPlatform/golang-samples/internal/testutil"
)

func TestApp(t *testing.T) {
	m := testutil.BuildMain(t)
	defer m.Cleanup()

	if !m.Built() {
		t.Errorf("failed to build app")
	}

	// TODO(shollyman): Run the app and check for a nonzero exit code.
	// Perhaps another testutil RunUntilExit(timeout time.Duration) that returns nonzero exit codes?
	// Expose stderr as test output?
}
