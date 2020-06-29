<a name="v0.2.0"></a>
# [v0.2.0](https://github.com/qri-io/dataset/compare/v0.1.4...v0.2.0) (2020-06-29)

A minor release that introduces a number of small fixes, an overhauled `gen` package based on new tabular type detection.


### Bug Fixes

* **detect:** Don't treat strings starting with 't','f','n' as the wrong type ([1eb7656](https://github.com/qri-io/dataset/commit/1eb7656))
* **detect:** Iterate type counts in a deterministic manner ([6427bdd](https://github.com/qri-io/dataset/commit/6427bdd))
* **dsfs.getDepth:** fix algorithm & add tests ([f67cfd6](https://github.com/qri-io/dataset/commit/f67cfd6))
* **dsio:** json decoder emits int64 instead of int ([8a8404c](https://github.com/qri-io/dataset/commit/8a8404c))
* **dsio:** remove stub schema function for CSV & XLSX formats ([94a15a5](https://github.com/qri-io/dataset/commit/94a15a5))
* **dsutil:** use a context cancel instead of not loading viz ([53231a0](https://github.com/qri-io/dataset/commit/53231a0))
* **entryreader:** json over batch size propperly unmarshals now ([#227](https://github.com/qri-io/dataset/issues/227)) ([71e64eb](https://github.com/qri-io/dataset/commit/71e64eb))
* **NewJSONPrettyWriter:** now writer correctly writes object values when indenting ([2d2e247](https://github.com/qri-io/dataset/commit/2d2e247))


### Features

* **detect:** detect tabular schemas from go types ([cdaceda](https://github.com/qri-io/dataset/commit/cdaceda))
* **dsgen:** add dsgen command for generating datasets, overhaul gen pkg ([bf363af](https://github.com/qri-io/dataset/commit/bf363af))
* **readme:** Readme component for datasets ([c2db273](https://github.com/qri-io/dataset/commit/c2db273))
* **structure:** add RequiresTabularSchema method ([9f24359](https://github.com/qri-io/dataset/commit/9f24359))
* **tabular:** package tabular defines tools for tabular datasets ([9fec0a3](https://github.com/qri-io/dataset/commit/9fec0a3))
* **transform:** add InlineScript method, matching readme ([8929f14](https://github.com/qri-io/dataset/commit/8929f14))


### Performance Improvements

* **csv:** increase read buffer size for csv reader ([#225](https://github.com/qri-io/dataset/issues/225)) ([a8aa566](https://github.com/qri-io/dataset/commit/a8aa566))



# [0.1.4](https://github.com/qri-io/dataset/compare/v0.1.2...v0.1.4) (2019-09-04)

This patch release include small fixes for dsio.JSON format reader, strict valdation error returns in dsfs.SaveDataset, and a method for Dropping _derived_ values from a Dataset & Components


### Bug Fixes

* **dsio:** don't fail on escaped forward slashes ([8a6c7b0](https://github.com/qri-io/dataset/commit/8a6c7b0))
* **strict:** If strict dataset fails to validate, show errors ([ea472ce](https://github.com/qri-io/dataset/commit/ea472ce))
* **strict:** Write errors to stderr not stdout ([06e5a4f](https://github.com/qri-io/dataset/commit/06e5a4f))
* **structure:** Omit errCount if empty ([1af25ff](https://github.com/qri-io/dataset/commit/1af25ff))


### Features

* **DropDerived:** add methods for dropping derived values ([#200](https://github.com/qri-io/dataset/issues/200)) ([f9ddda7](https://github.com/qri-io/dataset/commit/f9ddda7))
* **json:** JSONOptions has a map. JSONWriter can pretty-print. ([51c9c4c](https://github.com/qri-io/dataset/commit/51c9c4c))



<a name="v0.1.2"></a>
# [v0.1.2](https://github.com/qri-io/dataset/compare/v0.1.1...v0.1.2) (2019-06-10)

Quick patch release that adds a utility function to dsviz templates: `isType`.

### Features

* **dsviz:** add isType method ([d3e7c24](https://github.com/qri-io/dataset/commit/d3e7c24))



<a name="v0.1.1"></a>
# [v0.1.1](https://github.com/qri-io/dataset/compare/v0.1.0...v0.1.1) (2019-06-03)

Due to a circular module dependency, we've move `github.com/qri-io/dsdiff` into `github.com/qri-io/dataset/dsdiff`.

### Features

* **dsdiff:** merge dsdiff into dataset ([b5a6945](https://github.com/qri-io/dataset/commit/b5a6945))



<a name="v0.1.0"></a>
#  (2019-06-03)

This is the first proper release of `dataset`. In preparation for go 1.13, in which go.mod files and go modules are the primary way to handle go dependencies, we are going to do an official release of all our modules. This will be version v0.1.0 of `dataset`.

The change log is huge here because we haven't been properly cutting releases until now. From here forward, that changes! Yay! Progress!

### Bug Fixes

* **benchmark:** Relative paths for benchmark test data, fix broken paths ([4e8488c](https://github.com/qri-io/dataset/commit/4e8488c))
* **Body:** more body renaming ([4dd0c4f](https://github.com/qri-io/dataset/commit/4dd0c4f))
* **CodingStructure:** schema should code to full JSON ([6580109](https://github.com/qri-io/dataset/commit/6580109))
* **csv:** inferred csv structures use lazyQuotes by default, set VariadicFields ([5bacbb9](https://github.com/qri-io/dataset/commit/5bacbb9)), closes [#140](https://github.com/qri-io/dataset/issues/140)
* **csv:** properly handle solo carriage returns ([a200d54](https://github.com/qri-io/dataset/commit/a200d54))
* **dataset:** Dataset BodyFile uses BodyPath. Fixes http drag-n-drop. ([393ba3d](https://github.com/qri-io/dataset/commit/393ba3d))
* **dataset:** Dataset has transient field NumVersions ([87ec0bf](https://github.com/qri-io/dataset/commit/87ec0bf))
* **dataset:** Test for DropTransientValues ([cbac108](https://github.com/qri-io/dataset/commit/cbac108))
* **DatasetPod:** update datasetPod json struct tags ([5e9aacb](https://github.com/qri-io/dataset/commit/5e9aacb))
* **detect.JSONSchema:** fix bufio overflow when detecting on large files ([2f9289f](https://github.com/qri-io/dataset/commit/2f9289f))
* **dsfs:** rename data.json to body.json on Write ([c5de390](https://github.com/qri-io/dataset/commit/c5de390))
* **dsfs.CreateDataset:** remove 'no meaninful changes' check ([eceb759](https://github.com/qri-io/dataset/commit/eceb759))
* **dsio.CBORReader:** fix length not being read from top level ([#95](https://github.com/qri-io/dataset/issues/95)) ([610100e](https://github.com/qri-io/dataset/commit/610100e))
* **dsio.CBORWriter:** made encoding canonical, added test to prove it ([4ef2dce](https://github.com/qri-io/dataset/commit/4ef2dce))
* **dsio.CSVReader,dsfs.CreateDataset:** fix mutex pass-by-ref, csv read corner case ([6740283](https://github.com/qri-io/dataset/commit/6740283))
* **dsio.ErrEOF:** should be replaced with io.EOF ([8fd8475](https://github.com/qri-io/dataset/commit/8fd8475))
* **dsio.JSONReader:** fix buffer overflow edge case reading large JSON entries ([ffac445](https://github.com/qri-io/dataset/commit/ffac445))
* **dsio.JSONReader:** handle edge case of object/array reading empty buffer ([5ac7e5c](https://github.com/qri-io/dataset/commit/5ac7e5c))
* **dsio.JSONReader:** handle whenever a token lands on a buffer boundary ([ed7f249](https://github.com/qri-io/dataset/commit/ed7f249))
* **dsio.JSONWriter:** make JSONWriter write Entry.Value, not Entry ([14e2ee5](https://github.com/qri-io/dataset/commit/14e2ee5))
* **dstest:** Cache the reading of TestCases, speeding up unit tests ([3852d9a](https://github.com/qri-io/dataset/commit/3852d9a))
* **dstest:** fix dstest loading .star files ([155433c](https://github.com/qri-io/dataset/commit/155433c))
* **dsutil.FromRequest:** need to infer structure on passed-in body files ([40c5625](https://github.com/qri-io/dataset/commit/40c5625))
* **dsviz:** use the teeReader when creating an entry reader ([4f9c8ae](https://github.com/qri-io/dataset/commit/4f9c8ae))
* **export:** Exporting a zip includes viz and transform and reference ([990732e](https://github.com/qri-io/dataset/commit/990732e))
* **export:** Test exporting with transform and viz. Run `go fmt` ([c9d6561](https://github.com/qri-io/dataset/commit/c9d6561))
* **loadDatasetRefs:** add correct path to dataset ([8fdb07d](https://github.com/qri-io/dataset/commit/8fdb07d))
* **meta:** IsEmpty function does not check to see if `Meta.License` is nil ([2aca344](https://github.com/qri-io/dataset/commit/2aca344))
* **save:** Fix deadlock when saving invalid file ([a1d2103](https://github.com/qri-io/dataset/commit/a1d2103))
* **Structure:** added missing FormatConfig check to CompareStructures ([dfa9148](https://github.com/qri-io/dataset/commit/dfa9148))
* **structure json:** omit empty dataset structure in json ([535d912](https://github.com/qri-io/dataset/commit/535d912))
* **subset:** previews need to contain info to check commit signatures ([94ae5db](https://github.com/qri-io/dataset/commit/94ae5db))
* **test:** Fix test that was broken on Linux by avoiding double-buffer ([95e6e0e](https://github.com/qri-io/dataset/commit/95e6e0e))
* **windows:** Fix ipfs path problem on Windows, by using "/" always. ([bb8ccbe](https://github.com/qri-io/dataset/commit/bb8ccbe))
* **WriteDataset:** cover posibility of no `Viz.renderFile` but still having a `Viz.scriptFile` ([f3ffa3d](https://github.com/qri-io/dataset/commit/f3ffa3d))
* **xlsx:** Fix spelling of source filename ([92edb94](https://github.com/qri-io/dataset/commit/92edb94))
* **xlsx:** update brokn excelize API dep ([21a5f7a](https://github.com/qri-io/dataset/commit/21a5f7a))
* **zip:** WriteZipArchive accepts format option for dataset file ([b4133d7](https://github.com/qri-io/dataset/commit/b4133d7))
* have json reader output vals.ObjectValue when it should ([92291de](https://github.com/qri-io/dataset/commit/92291de))
* **dsfs:** added set of unexported load methods ([3892350](https://github.com/qri-io/dataset/commit/3892350))
* **dsio,dsfs:** fix bugs in entry counts, json reader string error ([9fd75cc](https://github.com/qri-io/dataset/commit/9fd75cc))
* **vals.Coding:** cleanup json coding errors ([d251cb1](https://github.com/qri-io/dataset/commit/d251cb1))
* corrected expected hash in `TestCreateDataset` ([38a5e7b](https://github.com/qri-io/dataset/commit/38a5e7b))
* fix lint error ([c2e1a74](https://github.com/qri-io/dataset/commit/c2e1a74))
* fix upstream error diffing dataset meta ([0d0eef9](https://github.com/qri-io/dataset/commit/0d0eef9))
* removed redundant visconfig marshal object functions from rebase ([7603e53](https://github.com/qri-io/dataset/commit/7603e53))
* resolving conflicts caused by rebase from master ([9914184](https://github.com/qri-io/dataset/commit/9914184))
* updated calls to datasetDiffer functions to comply with refactor ([f09b675](https://github.com/qri-io/dataset/commit/f09b675))
* updated dataset tests to be compatible with differ and fixed circleci config ([b6aeae1](https://github.com/qri-io/dataset/commit/b6aeae1))
* updated duplicate test function name ([f9785a6](https://github.com/qri-io/dataset/commit/f9785a6))
* **Assign:** fixes to assign method pointer errors ([4cffca3](https://github.com/qri-io/dataset/commit/4cffca3))
* **commit.IsEmpty:** commit.IsEmpty() now does the right thing. added tests ([580f2fe](https://github.com/qri-io/dataset/commit/580f2fe))
* **CommitMsg:** moved CompareCommitMsg from tests suite into package ([ad2e117](https://github.com/qri-io/dataset/commit/ad2e117))
* **csvValidation:**  updated csvReader config and regex character count ([3b4745c](https://github.com/qri-io/dataset/commit/3b4745c))
* **csvValidation:** updated error messages to match and be less cryptic ([1b47446](https://github.com/qri-io/dataset/commit/1b47446))
* **Dataset:** fix datasets not un/marshaling commit properly ([ffbafbf](https://github.com/qri-io/dataset/commit/ffbafbf))
* **Dataset:** theme is an array of strings for now, other were breaking parsing ([99de9b2](https://github.com/qri-io/dataset/commit/99de9b2))
* **dsfs.LoadRows:** make cdxj row reading work ([4905f77](https://github.com/qri-io/dataset/commit/4905f77))
* **dsfs/dataset, visconfig:** edit to fix tests after rebase ([8dda96d](https://github.com/qri-io/dataset/commit/8dda96d))
* **prepareDataset:** prepareDataset can load previous dataset if no data is provided ([1b4efc1](https://github.com/qri-io/dataset/commit/1b4efc1))
* **visconfig:** create all functions needed to dereference, load and write VisConfig to/from the store ([6379da5](https://github.com/qri-io/dataset/commit/6379da5))
* updated `generateCommitMsg` to include the dataset's format ([fecfcdb](https://github.com/qri-io/dataset/commit/fecfcdb))
* updated commit message generation to work with updated differ ([2e36d02](https://github.com/qri-io/dataset/commit/2e36d02))
* updated visconfig 'kind' to 'qri' and updated hashes in datasets_test ([b6a7a18](https://github.com/qri-io/dataset/commit/b6a7a18))
* **circleci:** fix missing circleci dep ([0212126](https://github.com/qri-io/dataset/commit/0212126))
* **compare, VisConfig, KindVisConfig:** fix typos ([7e744d3](https://github.com/qri-io/dataset/commit/7e744d3))
* **dsfs.CreateDataset:** actually write data on call to CreateDataset ([0b9db83](https://github.com/qri-io/dataset/commit/0b9db83))
* **dsfs.SaveDataset:** make SaveDataset write transform ([3fe9363](https://github.com/qri-io/dataset/commit/3fe9363))
* **dsgraph:** add AbstractDataset node type ([45db941](https://github.com/qri-io/dataset/commit/45db941))
* **dsio:** added error return for NewRowReader,Writer,Buffer ([3426e9f](https://github.com/qri-io/dataset/commit/3426e9f))
* **dsio.CDXJReader:** restore cdxj reader for upstream tests ([f36129b](https://github.com/qri-io/dataset/commit/f36129b))
* **dsio.JsonWriter:** return valid json when no rows written ([63b0833](https://github.com/qri-io/dataset/commit/63b0833))
* **dsio.JsonWriter:** writing json datatype with a json writer ([12f40e2](https://github.com/qri-io/dataset/commit/12f40e2))
* **dsio.LoadDataset:** fix outdated reference to dataset.CommitMsg ([baf1d47](https://github.com/qri-io/dataset/commit/baf1d47))
* **transform.Abstract:** fix not setting Data prop ([1af1efc](https://github.com/qri-io/dataset/commit/1af1efc))
* **VisConfig:** even if kind is not empty, the VisConfig should be considered empty ([e37240d](https://github.com/qri-io/dataset/commit/e37240d))


### Code Refactoring

* **viz:** overhauled template processing ([d6adf2c](https://github.com/qri-io/dataset/commit/d6adf2c))
* change path representations to string values ([277f4fa](https://github.com/qri-io/dataset/commit/277f4fa)), closes [qri-io/cafs#22](https://github.com/qri-io/cafs/issues/22)
* **Transform:** remove AbstractTransform, update Transform ([e5c07ba](https://github.com/qri-io/dataset/commit/e5c07ba))


### Features

* **benchmarks:** benchmark tests for datatype package ([d02569d](https://github.com/qri-io/dataset/commit/d02569d))
* **cbor:** Support indefinite structures. More tests for cbor reading. ([30dddb2](https://github.com/qri-io/dataset/commit/30dddb2))
* **CBOR:** experimental support for concise binary object representation (CBOR) ([e60cc07](https://github.com/qri-io/dataset/commit/e60cc07))
* **Commit:** added git like treatment of long or multi-line commit title adding overflow to the message ([bd0f690](https://github.com/qri-io/dataset/commit/bd0f690))
* **commit.Title:** updated PrepareDataset to include an auto-commit message from datasetDiffer ([fe416be](https://github.com/qri-io/dataset/commit/fe416be))
* **commit.Title:** updated PrepareDataset to include an auto-commit message from datasetDiffer ([cd004e3](https://github.com/qri-io/dataset/commit/cd004e3))
* **CommitMsg:** Store comit message in dataset definition ([a92da68](https://github.com/qri-io/dataset/commit/a92da68)), closes [#12](https://github.com/qri-io/dataset/issues/12)
* **CommitMsg.Assign:** added Assign method to CommitMsg ([8890b9f](https://github.com/qri-io/dataset/commit/8890b9f))
* **compare, kind:** add CompareVisCompare, KindVisConfig, and tests ([0c50697](https://github.com/qri-io/dataset/commit/0c50697))
* **createDataset:** added function `confirmChangesOccurred` to prevent empty updates ([328119f](https://github.com/qri-io/dataset/commit/328119f))
* **createDataset:** added function `confirmChangesOccurred` to prevent empty updates ([8d14230](https://github.com/qri-io/dataset/commit/8d14230))
* **CSVOptions:** added LazyQuotes, Separatore, VariadicFields options ([a3da89f](https://github.com/qri-io/dataset/commit/a3da89f))
* **dataset:** add VisConfig as field in dataset and accompanying tests ([3816720](https://github.com/qri-io/dataset/commit/3816720))
* **dataset.Body:** rename Data fields to Body ([cdec388](https://github.com/qri-io/dataset/commit/cdec388))
* **dataset.Dataset:** add AccrualPeriodicity field ([929caf0](https://github.com/qri-io/dataset/commit/929caf0))
* **dataset.Kind:** add Kind identifiers to all dataset models ([dd75eb9](https://github.com/qri-io/dataset/commit/dd75eb9))
* **dataset.Structure:** add ErrCount field to structure ([fbcee73](https://github.com/qri-io/dataset/commit/fbcee73)), closes [#47](https://github.com/qri-io/dataset/issues/47)
* **DatasetPod:** add convenience fields to DSP ([bbb4373](https://github.com/qri-io/dataset/commit/bbb4373))
* **datatype:** add IsFloat, IsJSON, IsDate ([ec3dbaa](https://github.com/qri-io/dataset/commit/ec3dbaa))
* **datatype.Json:** Add Json datatype to dataset/datatype package ([8ed621f](https://github.com/qri-io/dataset/commit/8ed621f))
* **datatypes:** add more tests to ParseInteger and ParseFloat ([b2efdbf](https://github.com/qri-io/dataset/commit/b2efdbf))
* **depth:** add structure depth property ([e422f75](https://github.com/qri-io/dataset/commit/e422f75))
* **detect:** added basic JSON schema detection ([bc42a8d](https://github.com/qri-io/dataset/commit/bc42a8d))
* **detect:** added basic JSON schema detection ([b05ea86](https://github.com/qri-io/dataset/commit/b05ea86))
* **detect.CBOR:** added decect for CBOR data format ([f7df690](https://github.com/qri-io/dataset/commit/f7df690))
* **detect.FromReader, dsio.TrackedReader:** return number of bytes read ([c497d47](https://github.com/qri-io/dataset/commit/c497d47))
* **diso.CSV:** use dataset structure to decode csv strings to rich types ([cf28804](https://github.com/qri-io/dataset/commit/cf28804))
* **dsfs.CreateDataset:** add checksum, length, and row count when creating datasets ([85f0a57](https://github.com/qri-io/dataset/commit/85f0a57))
* **dsfs.CreateDataset:** add force flag to force an update ([60ad670](https://github.com/qri-io/dataset/commit/60ad670))
* **dsfs.CreateDataset:** added private key check ([3f0fdad](https://github.com/qri-io/dataset/commit/3f0fdad))
* **dsfs.CreateDataset:** initial implementaiton of dsfs.CreateDataset ([4785a9e](https://github.com/qri-io/dataset/commit/4785a9e))
* **dsfs.JSONFile:** export jsonFile method ([6d9dd39](https://github.com/qri-io/dataset/commit/6d9dd39))
* **dsfs.LoadDatasetRef:** loaded datasets retain an unexported path property ([4e9b63f](https://github.com/qri-io/dataset/commit/4e9b63f))
* **dsfs.LoadViz/TransformScript:** convenince funcs for loading script files ([49666e1](https://github.com/qri-io/dataset/commit/49666e1))
* **dsfs.PackageFilepath:** added method for determining canonical filepaths ([5f3f962](https://github.com/qri-io/dataset/commit/5f3f962))
* **dsgraph:** graph links between hashes in a qri repo ([c69e72d](https://github.com/qri-io/dataset/commit/c69e72d))
* **dsio:** Generate example entries, fuzz testing, documentation ([40e8d35](https://github.com/qri-io/dataset/commit/40e8d35))
* **dsio:** new file streams, with Copy and PagedReader ([f923ef4](https://github.com/qri-io/dataset/commit/f923ef4))
* **dsio.Cdxj:** added cdxj Reader / Writers to dsio package ([fa272b0](https://github.com/qri-io/dataset/commit/fa272b0)), closes [qri-io/qri#87](https://github.com/qri-io/qri/issues/87)
* **dsio.JSONReader:** rewrite JSONReader for better errors and performance ([819375c](https://github.com/qri-io/dataset/commit/819375c))
* **dsio.JSONReader, dsio.JSONWriter:** update JSON readers & writers ([6aef27e](https://github.com/qri-io/dataset/commit/6aef27e))
* **dsio.StructuredRowBuffer:** added StructuredRowBuffer ([9845493](https://github.com/qri-io/dataset/commit/9845493))
* **dsio.StructuredRowBuffer:** StructuredRowBuffer with OrderBy and Unique ([7397baf](https://github.com/qri-io/dataset/commit/7397baf))
* **dstest:** updated for go 1.10, added dstest package ([b1b743e](https://github.com/qri-io/dataset/commit/b1b743e))
* **dsutil:** add methods for decoding datasetPod from zips and http requests ([d9af5b0](https://github.com/qri-io/dataset/commit/d9af5b0))
* **dsutil.DsYAML:** added convenience func to decode from yaml ([2411604](https://github.com/qri-io/dataset/commit/2411604))
* **dsutil/zip:** if there is a `renderedFile` add it to the zip archive ([a862dd7](https://github.com/qri-io/dataset/commit/a862dd7))
* **dsviz:** add bodyEntries and allBodyEntries template funcs ([ff05412](https://github.com/qri-io/dataset/commit/ff05412))
* **dsviz:** add initial viz package ([fdc32a7](https://github.com/qri-io/dataset/commit/fdc32a7))
* **dsviz.PredefinedHTML:** add global predefined HTML templates ([54e229a](https://github.com/qri-io/dataset/commit/54e229a))
* **fill_struct:** Meta implements SetKeyVal used by FillStruct ([2a0fe86](https://github.com/qri-io/dataset/commit/2a0fe86))
* **fill_struct:** Rename SetKeyVal to SetArbitrary ([e0e647b](https://github.com/qri-io/dataset/commit/e0e647b))
* **JsonReader:** added initial implementation of a json reader ([98753b5](https://github.com/qri-io/dataset/commit/98753b5))
* **Kind:** added Kind identifier to dataset definition ([810d6d5](https://github.com/qri-io/dataset/commit/810d6d5))
* **Meta:** added MarshalJSONOBject to force obj marshaling ([661199f](https://github.com/qri-io/dataset/commit/661199f))
* **Meta.Set:** add Set method to meta ([9100c1a](https://github.com/qri-io/dataset/commit/9100c1a))
* **package:** add `PackageFileRendered` that points to `index.html` ([62ef201](https://github.com/qri-io/dataset/commit/62ef201))
* **package, dataset:** add VisConfig to package and dataset and accompanying tests ([ad9532d](https://github.com/qri-io/dataset/commit/ad9532d))
* **pod.Assign:** give Assign pattern to DatasetPod variants ([1bd5fe0](https://github.com/qri-io/dataset/commit/1bd5fe0))
* **Query:** added new Assign and Save methods for Query/AbstractQuery ([40fa97b](https://github.com/qri-io/dataset/commit/40fa97b))
* **Query:** renamed Query to AbstractQuery, added in concrete Query ([a43a8e5](https://github.com/qri-io/dataset/commit/a43a8e5))
* **RefType:** added RefType method for sniffing hash references from raw input ([9dc49ce](https://github.com/qri-io/dataset/commit/9dc49ce))
* **Save:** if a commit message is provided without a title, message propagates up to title ([75cba92](https://github.com/qri-io/dataset/commit/75cba92))
* **SetPath:** the SetPath function allows outside packages to change the path ([69bb25f](https://github.com/qri-io/dataset/commit/69bb25f))
* **SignableBytes:** sign dataset hash & timestamp instead of title ([e88e347](https://github.com/qri-io/dataset/commit/e88e347))
* **subset:** dataset subset previews ([13198f6](https://github.com/qri-io/dataset/commit/13198f6))
* **timestamp:** dsfs.Timestamp now an exported function, to be overriden when testing datasets ([a33edad](https://github.com/qri-io/dataset/commit/a33edad))
* **transform:** fix bug in IsEmpty ([4008bbe](https://github.com/qri-io/dataset/commit/4008bbe))
* **TransformPod.Secrets:** add Secrets field to TransformPod ([4a0ca92](https://github.com/qri-io/dataset/commit/4a0ca92))
* **validat.Data:** added data validate method ([ef091f5](https://github.com/qri-io/dataset/commit/ef091f5))
* **validate.Dataset:** better validate funcs ([a673084](https://github.com/qri-io/dataset/commit/a673084))
* **validate.ValidName:** added func to check dataset names ([6398e72](https://github.com/qri-io/dataset/commit/6398e72))
* **visConfig:** add MarshalJSONObject func that always returns a json Object, even if visConfig is empty or a reference ([b45274a](https://github.com/qri-io/dataset/commit/b45274a))
* **visConfig:** add VisConfig struc ([37c6c51](https://github.com/qri-io/dataset/commit/37c6c51))
* **visConfig, Structure:** add tests to vis_config.go and structure.go ([7e8b543](https://github.com/qri-io/dataset/commit/7e8b543))
* **viz:** refactor vizconfig into initial support for render templates ([b831288](https://github.com/qri-io/dataset/commit/b831288))
* **Viz:** add field to Viz to store path to the rendered output ([1d1461c](https://github.com/qri-io/dataset/commit/1d1461c))
* **xlsx:** initial support for xlsx data format, identity dsio ([bbcd537](https://github.com/qri-io/dataset/commit/bbcd537))
* qri datasets now support high-dimensional data & jsonschemas ([070a221](https://github.com/qri-io/dataset/commit/070a221))
* rename datatypes to vals, add Value primitives ([17a0b08](https://github.com/qri-io/dataset/commit/17a0b08))
* replace dataset.Schema with jsonschema.RootSchema ([ba678fe](https://github.com/qri-io/dataset/commit/ba678fe))


### Performance Improvements

* **dsfs.CreateDataset:** parallelize ds prep with io.MultiWriter ([2b027f7](https://github.com/qri-io/dataset/commit/2b027f7))


### rafactor

* merge exported structs into Pod structs, remove Pod suffix ([21f2d36](https://github.com/qri-io/dataset/commit/21f2d36))


### BREAKING CHANGES

* **viz:** viz template processing syntax has changed
* All dataset components exhibit the Pod pattern, pod suffix is dropped.
* all "path" manipulation methods now accept and return strings instead of datastore.Key
* **dataset.Body:** dataset.DataPath is renamed to dataset.BodyPath.
* **Transform:** this breaks hashes, again..
* **Kind:** adding kind to the dataset def breaks hashes
* **dsio.StructuredRowBuffer:** * dsio.Buffer is now dsio.StructuredBuffer (for clarity)
* dsio.CSVWriter now writes a header line if the structure.FormatConfig
  indicates HeaderRow = true
* **datatype.Json:** Array and Object data types are removed,
JsonArray data format is removed.
* **Query:** the Defintion of dataset has changed. This will break hashes
from all previously run queries. So sad, but it's early days, and this is bound
to happen a bunch.



