package csxutils

import "math"

var epsilon = math.Nextafter(1, 2) - 1

// ---------------------------------------------------------------------------------
// Trigonometry
// ---------------------------------------------------------------------------------

func DegToRad(degree float64) float64 {
	return degree * (math.Pi / 180)
}

func RadToDeg(radian float64) float64 {
	return radian * (180 / math.Pi)
}

// ---------------------------------------------------------------------------------
// Precise comparisons on real numbers
// ---------------------------------------------------------------------------------

// Float64Eq returns true if two float64 numbers are equal
func Float64Eq(f1, f2 float64) bool {
	return math.Abs(f1-f2) < epsilon
}
