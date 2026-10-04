// Command ci is the Dagger-backed CI/CD pipeline for retry-endpoint. The pipeline
// itself is github.com/lightwebinc/ci/gopipe; this file is only its
// configuration. Usage: go run ./ci <subcommand> [flags] (see the Makefile).
package main

import "github.com/lightwebinc/ci/gopipe"

func main() { gopipe.Main(gopipe.Config{Repo: "retry-endpoint"}) }
