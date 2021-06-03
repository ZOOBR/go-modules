package csxutils

import "math"

// ---------------------------------------------------------------------------------
// Trigonometry
// ---------------------------------------------------------------------------------

func DegToRad(degree float64) float64 {
	return degree * (math.Pi / 180)
}

func RadToDeg(radian float64) float64 {
	return radian * (180 / math.Pi)
}
