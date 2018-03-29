## Performance

2018-29-03

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
