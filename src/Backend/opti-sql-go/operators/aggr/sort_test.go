package aggr

import (
	"context"
	"fmt"
	"io"
	"opti-sql-go/Expr"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/compute"
)

func TestSortInit(t *testing.T) {
	// Simple passing test
	t.Run("sort Exec init", func(t *testing.T) {
		proj := aggProject()
		sortExec, err := NewSortExec(proj, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !sortExec.Schema().Equal(proj.Schema()) {
			t.Fatalf("expected schema %v, got %v", proj.Schema(), sortExec.schema)
		}
		sortExec.done = true
		_, err = sortExec.Next(100)
		if err != io.EOF {
			t.Fatalf("expected io.EOF error on done sortExec but got %v", err)
		}
		if sortExec.Close() != nil {
			t.Fatalf("expected nil error on close but got %v", sortExec.Close())
		}

	})
	t.Run("tok k sort exec init", func(t *testing.T) {
		proj := aggProject()
		topKVal := 5
		topK, err := NewTopKSortExec(proj, nil, uint16(topKVal))
		if err != nil {
			t.Fatal(err)
		}
		if !topK.Schema().Equal(proj.Schema()) {
			t.Fatalf("expected schema %v, got %v", proj.Schema(), topK.schema)
		}
		if topK.k != 5 {
			t.Fatalf("expected %v for top k but got %v", topKVal, topK.k)
		}
		topK.done = true
		_, err = topK.Next(100)
		if err != io.EOF {
			t.Fatalf("expected io.EOF error on done topK but got %v", err)
		}
		if topK.Close() != nil {
			t.Fatalf("expected nil error on close but got %v", topK.Close())
		}

	})
}

func TestBasicSortExpr(t *testing.T) {
	t.Run("Sort", func(t *testing.T) {
		proj := aggProject()
		nameExpr := Expr.NewColumnResolve("name")
		nameSK := NewSortKey(nameExpr, true)
		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false)
		_, err := NewSortExec(proj, CombineSortKeys(nameSK, ageSK))
		if err != nil {
			t.Fatalf("unexpected error from NewSortExec : %v\n", err)
		}
		//t.Logf("%v\n", sortExec)
	})
	t.Run("Basic Next operation", func(t *testing.T) {
		proj := aggProject()
		nameExpr := Expr.NewColumnResolve("name")
		nameSK := NewSortKey(nameExpr, true)
		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false)
		sortExec, err := NewSortExec(proj, CombineSortKeys(ageSK, nameSK))
		if err != nil {
			t.Fatalf("unexpected error from NewSortExec : %v\n", err)
		}
		sortedBatch, err := sortExec.Next(10)
		if err != nil {
			t.Fatalf("unexpected error from sortExec Next : %v\n", err)
		}
		fmt.Println(sortedBatch.PrettyPrint())

	})
}
func TestBasicTopKSortExpr(t *testing.T) {
	t.Run("TopK Sort", func(t *testing.T) {
		proj := aggProject()
		nameExpr := Expr.NewColumnResolve("name")
		nameSK := NewSortKey(nameExpr, true)
		ageExpr := Expr.NewColumnResolve("age")
		ageSK := NewSortKey(ageExpr, false)
		sortExec, err := NewTopKSortExec(proj, CombineSortKeys(nameSK, ageSK), 5)
		if err != nil {
			t.Fatalf("unexpected error from NewTopKSortExec : %v\n", err)
		}
		t.Logf("%v\n", sortExec)

	})
}

func TestOne(t *testing.T) {
	v := compute.GetExecCtx(context.Background())
	names := v.Registry.GetFunctionNames()
	for i, name := range names {
		fmt.Printf("%d: %v\n", i, name)
	}
	/*
		mem := memory.NewGoAllocator()
		floatB := array.NewFloat64Builder(mem)
		floatB.AppendValues([]float64{10.5, 20.3, 30.1, 40.7, 50.2}, []bool{true, true, true, true, true})
		pos := array.NewInt32Builder(mem)
		pos.AppendValues([]int32{1, 3, 4}, []bool{true, true, true})

		dat, err := compute.Take(context.TODO(), *compute.DefaultTakeOptions(), compute.NewDatum(floatB.NewArray()), compute.NewDatum(pos.NewArray()))
		if err != nil {
			t.Fatalf("Take failed: %v", err)
		}
		array, ok := dat.(*compute.ArrayDatum)
		if !ok {
			t.Logf("expected an array to be returned but got something else %T\n", dat)
		}
		t.Logf("data: %v\n", array.MakeArray())
	*/
}
