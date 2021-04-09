package csxutils

import "math"

const (
	GeoRadiusMax = 6378.137
	GeoRadiusMin = 6356.752
)

func CalcDistance(lat1, lon1, lat2, lon2 float64) float64 {
	latrad := ((lat2 + lat1) / 2.) * math.Pi / 180.
	radius := math.Sqrt(math.Pow(GeoRadiusMax*math.Cos(latrad), 2) + math.Pow(GeoRadiusMin*math.Sin(latrad), 2))
	deg2km := 180. / (math.Pi * radius)
	dy := (lat2 - lat1) / deg2km
	dx := (lon2 - lon1) / (deg2km / math.Cos(latrad))
	return math.Sqrt(dx*dx + dy*dy)
}

func CalcAngleDelta(firstAngle, secondAngle float64) (delta float64) {
	if firstAngle < 90 && secondAngle > 270 {
		delta = firstAngle + 360 - secondAngle
	} else if firstAngle > 270 && secondAngle < 90 {
		delta = secondAngle + 360 - firstAngle
	} else {
		delta = math.Abs(firstAngle - secondAngle)
	}
	return delta
}
