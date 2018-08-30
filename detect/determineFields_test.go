package detect

var egCorruptCsvData = []byte(`
		"""fhkajslfnakjlcdnajcl ashklj asdhcjklads ch,,,\dagfd
	`)

var egNaicsCsvData = []byte(`
STATE,FIRM,PAYR_N,PAYRFL_N,STATEDSCR,NAICSDSCR,entrsizedscr
00,--,74883.53,5621697325,United States,Total,01:  Total
00,--,35806.37,241347624,United States,Total,02:  0-4`)

var egNoHeaderData1 = []byte(`
example,false,other,stuff
ex,true,text,col
		`)

var egNoHeaderData2 = []byte(`
this,example,has,a,number,column,1
this,example,has,a,number,column,2
this,example,has,a,number,column,3`)

var egNoHeaderData3 = []byte(`
one, 1, three
one, 2, three`)

var egNonDeterministicHeader = []byte(`
not,possible,to,tell,if,this,csv,data,has,a,header
not,possible,to,tell,if,this,csv,data,has,a,header
not,possible,to,tell,if,this,csv,data,has,a,header
not,possible,to,tell,if,this,csv,data,has,a,header
`)
