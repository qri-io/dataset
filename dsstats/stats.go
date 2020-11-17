// Package dsstats calculates statistical metadata for a given dataset
package dsstats

import (
	"fmt"
	"sort"

	"github.com/axiomhq/hyperloglog"
	topk "github.com/dgryski/go-topk"
	logger "github.com/ipfs/go-log"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/dataset/dsstats/histosketch"
)

var (
	// StopFreqCountThreshold is the number of unique values past which we will
	// stop keeping frequencies. This is a simplistic line of defense against
	// unweildly memory consumption. 200 because more is not really user friendly
	// output
	StopFreqCountThreshold = 200
	// HistogramCentroidCount is the max number of centroids/bins for our histogram
	// calculations more bins give better precision at the tradeoff of more CPU
	// and memory usage. 32 is a pretty decent count for fairly large datasets.
	HistogramCentroidCount uint = 32

	// package logger
	log = logger.Logger("dsstats")
)

// Calculate determines a stats component by reading each entry in the Body of a
// given dataset. Requires an open BodyFile and well-formed Structure component
func Calculate(ds *dataset.Dataset) (st *dataset.Stats, err error) {
	body := ds.BodyFile()
	if body == nil {
		return nil, fmt.Errorf("stats: dataset has no body file")
	}
	if ds.Structure == nil {
		return nil, fmt.Errorf("stats: dataset is missing structure")
	}

	r, err := dsio.NewEntryReader(ds.Structure, ds.BodyFile())
	if err != nil {
		return nil, err
	}

	return CalculateFromEntryReader(r)
}

// CalculateFromEntryReader consumes an entry reader to generate a Stats
// component
func CalculateFromEntryReader(r dsio.EntryReader) (st *dataset.Stats, err error) {
	acc := NewAccumulator(r.Structure())
	defer acc.Close()

	err = dsio.EachEntry(r, func(i int, ent dsio.Entry, e error) error {
		if e != nil {
			return e
		}
		acc.WriteEntry(ent)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if err := acc.Close(); err != nil {
		return nil, err
	}

	return &dataset.Stats{
		Qri:   dataset.KindStats.String(),
		Stats: ToMap(acc),
	}, nil
}

// Statser produces a slice of Stat objects
type Statser interface {
	Stats() []Stat
}

// ToMap converts stats to a Plain Old Data object
func ToMap(s Statser) []map[string]interface{} {
	stats := s.Stats()
	if stats == nil {
		return nil
	}

	sm := make([]map[string]interface{}, len(stats))
	for i, stat := range stats {
		sm[i] = stat.Map()
		sm[i]["type"] = stat.Type()
	}
	return sm
}

// Stat describes common features of all statistical types
type Stat interface {
	// Type returns a string identifier for the kind of statistic being reported
	Type() string
	// Map reports statistical details as a map, map must not return nil
	Map() map[string]interface{}
}

// Accumulator wraps a dsio.EntryReader, on each call to read stats
// will update it's internal statistics
// Consumers can only assume the return value of Accumulator.Stats is final
// after a call to Close
type Accumulator struct {
	st    *dataset.Structure
	stats accumulator
}

var (
	// compile time assertions that Accumulator is an EntryWriter & Statser
	_ dsio.EntryWriter = (*Accumulator)(nil)
	_ Statser          = (*Accumulator)(nil)
)

// NewAccumulator wraps an entry reader to create a stat accumulator
func NewAccumulator(st *dataset.Structure) *Accumulator {
	return &Accumulator{st: st}
}

// Stats gets the statistics created by the accumulator
func (r *Accumulator) Stats() []Stat {
	if r.stats == nil {
		return nil
	}
	if stats, ok := r.stats.(Statser); ok {
		return stats.Stats()
	}
	return []Stat{r.stats}
}

// Structure gives the structure being read
func (r *Accumulator) Structure() *dataset.Structure {
	return r.st
}

// WriteEntry adds one row of structured data to accumulated stats
func (r *Accumulator) WriteEntry(ent dsio.Entry) error {
	if r.stats == nil {
		r.stats = newAccumulator(ent.Value)
	}
	r.stats.Write(ent)
	return nil
}

// Close finalizes the Reader
func (r *Accumulator) Close() error {
	r.stats.Close()
	return nil
}

// accumulator is the common internal inferface for creating a stat
// this package defines at least one accumulator for all values qri works with
// accumulators are one-way state machines that update with each Write
type accumulator interface {
	Stat
	Write(ent dsio.Entry)
	Close()
}

func newAccumulator(val interface{}) accumulator {
	switch val.(type) {
	default:
		return &nullAcc{}
	case float64, float32:
		return newNumericAcc("number")
	case int, int32, int64:
		return newNumericAcc("integer")
	case string:
		return newStringAcc()
	case bool:
		return &boolAcc{}
	case map[string]interface{}:
		return &objectAcc{children: map[string]accumulator{}}
	case []interface{}:
		return &arrayAcc{}
	}
}

type objectAcc struct {
	children map[string]accumulator
}

var (
	_ accumulator = (*objectAcc)(nil)
	_ Statser     = (*objectAcc)(nil)
)

// Stats gets child stats of the accumulator as a Stat slice
func (acc *objectAcc) Stats() (stats []Stat) {
	stats = make([]Stat, len(acc.children))
	keys := make([]string, len(acc.children))
	i := 0
	for key := range acc.children {
		keys[i] = key
		i++
	}
	sort.StringSlice(keys).Sort()
	for j, key := range keys {
		stats[j] = keyedStat{Stat: acc.children[key], key: key}
	}
	return stats
}

// Type indicates this stat accumulator kind
func (acc *objectAcc) Type() string { return "object" }

// Write adds an entry to the stat accumulator
func (acc *objectAcc) Write(e dsio.Entry) {
	if mapEntry, ok := e.Value.(map[string]interface{}); ok {
		for key, val := range mapEntry {
			if _, ok := acc.children[key]; !ok {
				acc.children[key] = newAccumulator(val)
			}
			acc.children[key].Write(dsio.Entry{Key: key, Value: val})
		}
	}
}

// Map formats stat values as a map
func (acc *objectAcc) Map() map[string]interface{} {
	vals := map[string]interface{}{}
	for key, val := range acc.children {
		vals[key] = val.Map()
	}
	return vals
}

// Close finalizes the accumulator
func (acc *objectAcc) Close() {
	for _, val := range acc.children {
		val.Close()
	}
}

type arrayAcc struct {
	children []accumulator
}

var (
	_ accumulator = (*arrayAcc)(nil)
	_ Statser     = (*arrayAcc)(nil)
)

// Stats gets child stats of the array accumulator
func (acc *arrayAcc) Stats() (stats []Stat) {
	stats = make([]Stat, len(acc.children))
	for i, ch := range acc.children {
		stats[i] = ch
	}
	return stats
}

// Type indicates this stat accumulator kind
func (acc *arrayAcc) Type() string { return "array" }

// Write adds an entry to the stat accumulator
func (acc *arrayAcc) Write(e dsio.Entry) {
	if arrayEntry, ok := e.Value.([]interface{}); ok {
		for i, val := range arrayEntry {
			if len(acc.children) == i {
				acc.children = append(acc.children, newAccumulator(val))
			}
			acc.children[i].Write(dsio.Entry{Index: i, Value: val})
		}
	}
}

// Map formats stat values as a map
func (acc *arrayAcc) Map() map[string]interface{} {
	vals := make([]map[string]interface{}, len(acc.children))
	for i, val := range acc.children {
		vals[i] = val.Map()
	}
	// TODO (b5) -  this is silly
	return map[string]interface{}{"values": vals}
}

// Close finalizes the accumulator
func (acc *arrayAcc) Close() {
	for _, val := range acc.children {
		val.Close()
	}
}

const (
	maxUint  = ^uint(0)
	maxFloat = float64(maxUint >> 1)
	maxInt   = int(maxUint >> 1)
	minInt   = -maxInt - 1
)

type numericAcc struct {
	typ             string
	count           int
	min             float64
	max             float64
	mean            float64
	median          float64
	dividers        []float64
	histogram       []float64
	histogramSketch *histosketch.Sketch
}

var _ accumulator = (*numericAcc)(nil)

func newNumericAcc(typ string) *numericAcc {
	return &numericAcc{
		typ:    typ,
		max:    float64(minInt),
		min:    float64(maxInt),
		median: maxFloat,
		// use histogram to accumulate values
		histogram: []float64{},
		// sketch implementation for approximate histogram calculation
		histogramSketch: histosketch.New(HistogramCentroidCount),
	}
}

// Type indicates this stat accumulator kind
func (acc *numericAcc) Type() string { return "numeric" }

// Write adds an entry to the stat accumulator
func (acc *numericAcc) Write(e dsio.Entry) {
	var v float64
	switch x := e.Value.(type) {
	case int:
		v = float64(x)
	case int32:
		v = float64(x)
	case int64:
		v = float64(x)
	case float32:
		v = float64(x)
	case float64:
		v = x
	default:
		return
	}

	acc.histogramSketch.Add(v)

	acc.mean += v
	acc.count++
	if v > acc.max {
		acc.max = v
	}
	if v < acc.min {
		acc.min = v
	}
}

// Map formats stat values as a map
func (acc *numericAcc) Map() map[string]interface{} {
	if acc.count == 0 {
		// avoid reporting default max/min figures, if count is above 0
		// at least one entry has been checked
		return map[string]interface{}{"count": 0}
	}
	m := map[string]interface{}{
		"mean":  acc.mean,
		"count": acc.count,
		"min":   acc.min,
		"max":   acc.max,
	}

	if acc.median != maxFloat {
		m["median"] = acc.median
	}

	if acc.histogram != nil {
		m["histogram"] = map[string][]float64{
			"bins":        acc.dividers,
			"frequencies": acc.histogram,
		}
	}

	return m
}

// Close finalizes the accumulator
func (acc *numericAcc) Close() {
	// finalize avg
	acc.mean = acc.mean / float64(acc.count)
	acc.dividers, acc.histogram = acc.histogramSketch.Read()
	acc.median = acc.histogramSketch.Median()
}

type stringAcc struct {
	count       int
	minLength   int
	maxLength   int
	unique      int
	frequencies map[string]int
	hll         *hyperloglog.Sketch
	topk        *topk.Stream
}

var _ accumulator = (*stringAcc)(nil)

func newStringAcc() *stringAcc {
	return &stringAcc{
		maxLength:   minInt,
		minLength:   maxInt,
		frequencies: map[string]int{},
		hll:         hyperloglog.New16(),
		topk:        topk.New(StopFreqCountThreshold),
	}
}

// Type indicates this stat accumulator kind
func (acc *stringAcc) Type() string { return "string" }

// Write adds an entry to the stat accumulator
func (acc *stringAcc) Write(e dsio.Entry) {
	if str, ok := e.Value.(string); ok {
		acc.count++

		acc.topk.Insert(str, 1)
		acc.hll.Insert([]byte(str))

		if len(str) < acc.minLength {
			acc.minLength = len(str)
		}
		if len(str) > acc.maxLength {
			acc.maxLength = len(str)
		}
	}
}

// Map formats stat values as a map
func (acc *stringAcc) Map() map[string]interface{} {
	if acc.count == 0 {
		// avoid reporting default max/min figures, if count is above 0
		// at least one entry has been checked
		return map[string]interface{}{"count": 0}
	}

	m := map[string]interface{}{
		"count":     acc.count,
		"minLength": acc.minLength,
		"maxLength": acc.maxLength,
	}

	if acc.unique != 0 {
		m["unique"] = acc.unique
	}
	if acc.frequencies != nil {
		m["frequencies"] = acc.frequencies
	}

	return m
}

// Close finalizes the accumulator
func (acc *stringAcc) Close() {
	acc.frequencies = map[string]int{}
	top := acc.topk.Keys()
	for _, k := range top {
		acc.frequencies[k.Key] = k.Count
	}
	acc.unique = int(acc.hll.Estimate())
}

type boolAcc struct {
	count      int
	trueCount  int
	falseCount int
}

var _ accumulator = (*boolAcc)(nil)

// Type indicates this stat accumulator kind
func (acc *boolAcc) Type() string { return "boolean" }

// Write adds an entry to the stat accumulator
func (acc *boolAcc) Write(e dsio.Entry) {
	if b, ok := e.Value.(bool); ok {
		acc.count++
		if b {
			acc.trueCount++
		} else {
			acc.falseCount++
		}
	}
}

// Map formats stat values as a map
func (acc *boolAcc) Map() map[string]interface{} {
	return map[string]interface{}{
		"count":      acc.count,
		"trueCount":  acc.trueCount,
		"falseCount": acc.falseCount,
	}
}

// Close finalizes the accumulator
func (acc *boolAcc) Close() {}

type nullAcc struct {
	count int
}

var _ accumulator = (*nullAcc)(nil)

// Type indicates this stat accumulator kind
func (acc *nullAcc) Type() string { return "null" }

// Write adds an entry to the stat accumulator
func (acc *nullAcc) Write(e dsio.Entry) {
	if e.Value == nil {
		acc.count++
	}
}

// Map formats stat values as a map
func (acc *nullAcc) Map() map[string]interface{} {
	return map[string]interface{}{"count": acc.count}
}

// Close finalizes the accumulator
func (acc *nullAcc) Close() {}

type keyedStat struct {
	Stat
	key string
}

// Map returns the stat, adding the "key" key indicating which key in the target
// array the stat belongs to
func (ks keyedStat) Map() map[string]interface{} {
	v := ks.Stat.Map()
	v["key"] = ks.key
	return v
}
