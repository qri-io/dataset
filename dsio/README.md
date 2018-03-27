## Performance

2018-27-03

    go test github.com/qri-io/dataset/dsio -bench=.

    BenchmarkCBORWriterArrays-2    	    3000	    459859 ns/op
    BenchmarkCBORWriterObjects-2   	    3000	    576226 ns/op
    BenchmarkCSVWriterArrays-2     	    1000	   1557871 ns/op
    BenchmarkCSVWriterObjects-2    	    1000	   1489634 ns/op
    BenchmarkJSONWriterArrays-2    	    1000	   1412656 ns/op
    BenchmarkJSONWriterObjects-2   	    1000	   1665526 ns/op
