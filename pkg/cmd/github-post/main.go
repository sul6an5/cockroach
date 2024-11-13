// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Command github-post parses the JSON-formatted output from a Go test session,
// as generated by either 'go test -json' or './pkg.test | go tool test2json -t',
// and posts issues for any failed tests to GitHub. If there are no failed
// tests, it assumes that there was a build error and posts the entire log to
// GitHub.
package main

import (
	"flag"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/bazci/githubpost"
)

func main() {
	formatterName := flag.String("formatter", "", "formatter to use to construct GitHub issues")
	flag.Parse()

	githubpost.PostFromJSON(*formatterName, os.Stdin)
}
