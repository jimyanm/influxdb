package influxql_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/influxql"
)

func BenchmarkSelect_Raw_1K(b *testing.B)   { benchmarkSelectRaw(b, 1000) }
func BenchmarkSelect_Raw_100K(b *testing.B) { benchmarkSelectRaw(b, 1000000) }

func benchmarkSelectRaw(b *testing.B, pointN int) {
	benchmarkSelect(b, MustParseSelectStatement(`SELECT fval FROM cpu`), NewRawBenchmarkIteratorCreator(pointN))
}

func benchmarkSelect(b *testing.B, stmt *influxql.SelectStatement, ic influxql.IteratorCreator) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		itrs, err := influxql.Select(stmt, ic, nil)
		if err != nil {
			b.Fatal(err)
		}
		influxql.DrainIterators(itrs)
	}
}

// NewRawBenchmarkIteratorCreator returns a new mock iterator creator with generated fields.
func NewRawBenchmarkIteratorCreator(pointN int) *IteratorCreator {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if opt.Expr != nil {
			panic("unexpected expression")
		}

		p := influxql.FloatPoint{
			Name: "cpu",
			Aux:  make([]interface{}, len(opt.Aux)),
		}

		for i := range opt.Aux {
			switch opt.Aux[i].Val {
			case "fval":
				p.Aux[i] = float64(100)
			default:
				panic("unknown iterator expr: " + opt.Expr.String())
			}
		}

		return &FloatPointGenerator{N: pointN, Fn: func(i int) *influxql.FloatPoint {
			p.Time = int64(time.Duration(i) * (10 * time.Second))
			return &p
		}}, nil
	}
	return &ic
}

func benchmarkSelectDedupe(b *testing.B, seriesN, pointsPerSeries int) {
	stmt := MustParseSelectStatement(`SELECT sval::string FROM cpu`)
	stmt.Dedupe = true

	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if opt.Expr != nil {
			panic("unexpected expression")
		}

		p := influxql.FloatPoint{
			Name: "tags",
			Aux:  []interface{}{nil},
		}

		return &FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *influxql.FloatPoint {
			p.Aux[0] = fmt.Sprintf("server%d", i%seriesN)
			return &p
		}}, nil
	}

	b.ResetTimer()
	benchmarkSelect(b, stmt, &ic)
}

func BenchmarkSelect_Dedupe_1K(b *testing.B) { benchmarkSelectDedupe(b, 1000, 100) }

func benchmarkSelectTop(b *testing.B, seriesN, pointsPerSeries int) {
	stmt := MustParseSelectStatement(`SELECT top(sval, 10) FROM cpu`)

	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			b.Fatalf("unexpected source: %s", m.Name)
		}
		if !reflect.DeepEqual(opt.Expr, MustParseExpr(`sval`)) {
			b.Fatalf("unexpected expr: %s", spew.Sdump(opt.Expr))
		}

		p := influxql.FloatPoint{
			Name: "cpu",
		}

		return &FloatPointGenerator{N: seriesN * pointsPerSeries, Fn: func(i int) *influxql.FloatPoint {
			p.Value = float64(rand.Int63())
			p.Time = int64(time.Duration(i) * (10 * time.Second))
			return &p
		}}, nil
	}

	b.ResetTimer()
	benchmarkSelect(b, stmt, &ic)
}

func BenchmarkSelect_Top_1K(b *testing.B) { benchmarkSelectTop(b, 1000, 1000) }
