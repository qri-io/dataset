## Performance

2018-04-17

    go test github.com/qri-io/dataset/dsio -bench=.

    BenchmarkCBORWriterArrays-2    	    3000	    478424 ns/op
    BenchmarkCBORWriterObjects-2   	    2000	    584435 ns/op
    BenchmarkCBORReader-2          	     300	   5081171 ns/op
    BenchmarkCSVWriterArrays-2     	    1000	   1369984 ns/op
    BenchmarkCSVWriterObjects-2    	    1000	   1406440 ns/op
    BenchmarkCSVReader-2           	    1000	   1463376 ns/op
    BenchmarkJSONWriterArrays-2    	    1000	   1377027 ns/op
    BenchmarkJSONWriterObjects-2   	    1000	   1558887 ns/op
    BenchmarkJSONReader-2          	     500	   2607946 ns/op

2018-03-29

    go test github.com/qri-io/dataset/dsio -bench=.

    BenchmarkCBORWriterArrays-2    	    3000	    423851 ns/op
    BenchmarkCBORWriterObjects-2   	    2000	    572609 ns/op
    BenchmarkCBORReader-2          	     300	   5024830 ns/op
    BenchmarkCSVWriterArrays-2     	    1000	   1448891 ns/op
    BenchmarkCSVWriterObjects-2    	    1000	   1457973 ns/op
    BenchmarkCSVReader-2           	    1000	   1454932 ns/op
    BenchmarkJSONWriterArrays-2    	    1000	   1423156 ns/op
    BenchmarkJSONWriterObjects-2   	    1000	   1620801 ns/op
    BenchmarkJSONReader-2          	     300	   5286851 ns/op

## Fuzz testing

From: [https://medium.com/@dgryski/go-fuzz-github-com-arolek-ase-3c74d5a3150c](http://https://medium.com/@dgryski/go-fuzz-github-com-arolek-ase-3c74d5a3150c)

How to fuzz test:

    go install github.com/qri-io/dataset/use_generate
    cd $GOPATH
    mkdir out
    bin/use_generate
    cp $GOPATH/out/* workdir/corpus/.

    go get github.com/dvyukov/go-fuzz/go-fuzz
    go get github.com/dvyukov/go-fuzz/go-fuzz-build
    go install github.com/dvyukov/go-fuzz/go-fuzz
    go install github.com/dvyukov/go-fuzz/go-fuzz-build

    go-fuzz-build github.com/qri-io/dataset/dsio
    go-fuzz -bin=dsio-fuzz.zip -workdir=workdir
