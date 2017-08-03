package influxql_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/influxql"
)

// Second represents a helper for type converting durations.
const Second = int64(time.Second)

func TestSelect_Derivative_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 2.25}},
		{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -4}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 2.25}},
		{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -4}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Desc_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 0 * Second, Value: 20},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.25}},
		{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 2.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Desc_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 0 * Second, Value: 20},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z' ORDER BY desc`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.25}},
		{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 2.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Derivative_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT derivative(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -2.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Difference_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 9}},
		{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: -16}},
	}); diff != "" {
		t.Fatalf("unexpected points: %s", diff)
	}
}

func TestSelect_Difference_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 9}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 12 * Second, Value: -16}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Difference_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
	}); diff != "" {
		t.Fatalf("unexpected points: %s", diff)
	}
}

func TestSelect_Difference_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: -10}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Non_Negative_Difference_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 29},
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 16 * Second, Value: 39},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 19}},
		{&influxql.FloatPoint{Name: "cpu", Time: 16 * Second, Value: 36}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Non_Negative_Difference_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 21},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 11}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Non_Negative_Difference_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
			{Name: "cpu", Time: 8 * Second, Value: 30},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 10},
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 16 * Second, Value: 40},
			{Name: "cpu", Time: 16 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 20}},
		{&influxql.FloatPoint{Name: "cpu", Time: 16 * Second, Value: 30}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Non_Negative_Difference_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
			{Name: "cpu", Time: 8 * Second, Value: 30},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 10},
			{Name: "cpu", Time: 12 * Second, Value: 3},
			{Name: "cpu", Time: 16 * Second, Value: 40},
			{Name: "cpu", Time: 16 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT non_negative_difference(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 20}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 16 * Second, Value: 30}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Elapsed_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 11 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Elapsed_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 11 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Elapsed_String(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &StringIterator{Points: []influxql.StringPoint{
			{Name: "cpu", Time: 0 * Second, Value: "a"},
			{Name: "cpu", Time: 4 * Second, Value: "b"},
			{Name: "cpu", Time: 8 * Second, Value: "c"},
			{Name: "cpu", Time: 11 * Second, Value: "d"},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Elapsed_Boolean(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &BooleanIterator{Points: []influxql.BooleanPoint{
			{Name: "cpu", Time: 0 * Second, Value: true},
			{Name: "cpu", Time: 4 * Second, Value: false},
			{Name: "cpu", Time: 8 * Second, Value: false},
			{Name: "cpu", Time: 11 * Second, Value: true},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT elapsed(value, 1s) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 4}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 4}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 11 * Second, Value: 3}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 10 * Second, Value: 20},
			{Name: "cpu", Time: 15 * Second, Value: 10},
			{Name: "cpu", Time: 20 * Second, Value: 0},
			{Name: "cpu", Time: 30 * Second, Value: -10},
		}}, nil
	}

	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 50}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Float_GroupByTime(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 10 * Second, Value: 20},
			{Name: "cpu", Time: 15 * Second, Value: 10},
			{Name: "cpu", Time: 20 * Second, Value: 0},
			{Name: "cpu", Time: 30 * Second, Value: -10},
		}}, nil
	}

	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu WHERE time > 0s AND time < 60s GROUP BY time(20s)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 100}},
		{&influxql.FloatPoint{Name: "cpu", Time: 20 * Second, Value: -50}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Float_InterpolateGroupByTime(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 10 * Second, Value: 20},
			{Name: "cpu", Time: 15 * Second, Value: 10},
			{Name: "cpu", Time: 25 * Second, Value: 0},
			{Name: "cpu", Time: 30 * Second, Value: -10},
		}}, nil
	}

	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu WHERE time > 0s AND time < 60s GROUP BY time(20s)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 112.5}},
		{&influxql.FloatPoint{Name: "cpu", Time: 20 * Second, Value: -12.5}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 5 * Second, Value: 10},
			{Name: "cpu", Time: 10 * Second, Value: 0},
			{Name: "cpu", Time: 20 * Second, Value: -10},
		}}, nil
	}

	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 50}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 5 * Second, Value: 10},
			{Name: "cpu", Time: 5 * Second, Value: 30},
			{Name: "cpu", Time: 10 * Second, Value: 40},
		}}, nil
	}

	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT integral(value) FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 250}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_Integral_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 5 * Second, Value: 10},
			{Name: "cpu", Time: 5 * Second, Value: 30},
			{Name: "cpu", Time: 10 * Second, Value: 40},
		}}, nil
	}

	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT integral(value, 2s) FROM cpu`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0, Value: 125}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_MovingAverage_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT moving_average(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 15, Aggregated: 2}},
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 14.5, Aggregated: 2}},
		{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 11, Aggregated: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_MovingAverage_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT moving_average(value, 2) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 15, Aggregated: 2}},
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 14.5, Aggregated: 2}},
		{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 11, Aggregated: 2}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_CumulativeSum_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 30}},
		{&influxql.FloatPoint{Name: "cpu", Time: 8 * Second, Value: 49}},
		{&influxql.FloatPoint{Name: "cpu", Time: 12 * Second, Value: 52}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_CumulativeSum_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 8 * Second, Value: 19},
			{Name: "cpu", Time: 12 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 30}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 8 * Second, Value: 49}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 12 * Second, Value: 52}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_CumulativeSum_Duplicate_Float(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
		{&influxql.FloatPoint{Name: "cpu", Time: 0 * Second, Value: 39}},
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 49}},
		{&influxql.FloatPoint{Name: "cpu", Time: 4 * Second, Value: 52}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_CumulativeSum_Duplicate_Integer(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return &IntegerIterator{Points: []influxql.IntegerPoint{
			{Name: "cpu", Time: 0 * Second, Value: 20},
			{Name: "cpu", Time: 0 * Second, Value: 19},
			{Name: "cpu", Time: 4 * Second, Value: 10},
			{Name: "cpu", Time: 4 * Second, Value: 3},
		}}, nil
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT cumulative_sum(value) FROM cpu WHERE time >= '1970-01-01T00:00:00Z' AND time < '1970-01-01T00:00:16Z'`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 20}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 0 * Second, Value: 39}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 49}},
		{&influxql.IntegerPoint{Name: "cpu", Time: 4 * Second, Value: 52}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

func TestSelect_HoltWinters_GroupBy_Agg(t *testing.T) {
	var ic IteratorCreator
	ic.CreateIteratorFn = func(m *influxql.Measurement, opt influxql.IteratorOptions) (influxql.Iterator, error) {
		if m.Name != "cpu" {
			t.Fatalf("unexpected source: %s", m.Name)
		}
		return influxql.NewCallIterator(&FloatIterator{Points: []influxql.FloatPoint{
			{Name: "cpu", Time: 10 * Second, Value: 4},
			{Name: "cpu", Time: 11 * Second, Value: 6},

			{Name: "cpu", Time: 12 * Second, Value: 9},
			{Name: "cpu", Time: 13 * Second, Value: 11},

			{Name: "cpu", Time: 14 * Second, Value: 5},
			{Name: "cpu", Time: 15 * Second, Value: 7},

			{Name: "cpu", Time: 16 * Second, Value: 10},
			{Name: "cpu", Time: 17 * Second, Value: 12},

			{Name: "cpu", Time: 18 * Second, Value: 6},
			{Name: "cpu", Time: 19 * Second, Value: 8},
		}}, opt)
	}

	// Execute selection.
	itrs, err := influxql.Select(MustParseSelectStatement(`SELECT holt_winters(mean(value), 2, 2) FROM cpu WHERE time >= '1970-01-01T00:00:10Z' AND time < '1970-01-01T00:00:20Z' GROUP BY time(2s)`), &ic, nil)
	if err != nil {
		t.Fatal(err)
	} else if a, err := Iterators(itrs).ReadAll(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if diff := cmp.Diff(a, [][]influxql.Point{
		{&influxql.FloatPoint{Name: "cpu", Time: 20 * Second, Value: 11.960623419918432}},
		{&influxql.FloatPoint{Name: "cpu", Time: 22 * Second, Value: 7.953140268154609}},
	}); diff != "" {
		t.Fatalf("unexpected points:\n%s", diff)
	}
}

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
