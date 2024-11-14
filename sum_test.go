package main

import (
	"testing"
)

// Traditional approach - concrete types
func SumInts(numbers []int) int {
	sum := 0
	for _, n := range numbers {
		sum += n
	}
	return sum
}

func SumFloat64s(numbers []float64) float64 {
	sum := 0.0
	for _, n := range numbers {
		sum += n
	}
	return sum
}

// Generic approach
func Sum[T ~int | ~float64](numbers []T) T {
	var sum T
	for _, n := range numbers {
		sum += n
	}
	return sum
}

// Example with interface{} for comparison
func SumInterfaceSlice(numbers interface{}) interface{} {
	switch nums := numbers.(type) {
	case []int:
		sum := 0
		for _, n := range nums {
			sum += n
		}
		return sum
	case []float64:
		sum := 0.0
		for _, n := range nums {
			sum += n
		}
		return sum
	default:
		return nil
	}
}

func BenchmarkSums(b *testing.B) {
	ints := make([]int, 1000)
	floats := make([]float64, 1000)

	for i := 0; i < 1000; i++ {
		ints[i] = i
		floats[i] = float64(i)
	}

	b.Run("Traditional Int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = SumInts(ints)
		}
	})

	b.Run("Generic Int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Sum(ints)
		}
	})

	b.Run("Traditional Float64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = SumFloat64s(floats)
		}
	})

	b.Run("Generic Float64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Sum(floats)
		}
	})

	b.Run("Interface Method", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = SumInterfaceSlice(ints)
		}
	})
}
