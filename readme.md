# dataset

[![Qri](https://img.shields.io/badge/made%20by-qri-magenta.svg?style=flat-square)](https://qri.io)
[![GoDoc](https://godoc.org/github.com/qri-io/dataset?status.svg)](http://godoc.org/github.com/qri-io/dataset)
[![License](https://img.shields.io/github/license/qri-io/dataset.svg?style=flat-square)](./LICENSE)
[![Codecov](https://img.shields.io/codecov/c/github/qri-io/dataset.svg?style=flat-square)](https://codecov.io/gh/qri-io/dataset)
[![CI](https://img.shields.io/circleci/project/github/qri-io/dataset.svg?style=flat-square)](https://circleci.com/gh/qri-io/dataset)
[![Go Report Card](https://goreportcard.com/badge/github.com/qri-io/dataset)](https://goreportcard.com/report/github.com/qri-io/dataset)

Dataset contains the qri ("query") dataset document definition. This package contains the base definition, as well as a number of
subpackages that build from this base to add functionality as necessary Datasets take inspiration from HTML documents, deliniating semantic purpose to predefined tags of the document, but instead of orienting around presentational markup, dataset documents emphasize interoperability and composition. The principle encoding format for a dataset document is JSON.

### Subpackage Overview

* **compression**: defines supported types of compression for interpreting a dataset
* **detect**: dataset structure & schema inference
* **dsfs**: "datasets on a content-addressed file system" tools to work with datasets stored with the [cafs](https://github.com/qri-io/qri) interface: `github.com/qri-io/cafs`
* **dsgraph**: expressing relationships between and within datasets as graphs
* **dsio**: `io` primitives for working with dataset bodies as readers, writers, buffers, oriented around row-like "entries".
* **dstest**: utility functions for working with tests that need datasets
* **dsutil**: utility functions that avoid dataset bloat
* **generate**: io primitives for generating data
* **use_generate**: small package that uses generate to create test data
* **validate**: dataset validation & checking functions
* **vals**: data type mappings & definitions

## Getting Involved

We would love involvement from more people! If you notice any errors or would
like to submit changes, please see our
[Contributing Guidelines](./.github/CONTRIBUTING.md).