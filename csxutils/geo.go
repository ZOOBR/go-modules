package csxutils

import (
	"encoding/json"
	"math"

	"github.com/sirupsen/logrus"
)

const (
	GeoRadiusMax  = 6378.137
	GeoRadiusMin  = 6356.752
	GeoRadiusAvgM = 6371e3 // in meters
)

// GeoPoint structure WGS-64 coordinates point
type GeoPoint struct {
	Lat, Lon float64
}

type GeoArc struct {
	P1, P2 GeoPoint
}

// GeoData structure for store GeoJSON field
type GeoData struct {
	Type        string       `db:"type" json:"type"`
	Coordinates [][]GeoPoint `db:"coordinates" json:"coordinates"`
}

// GeoJSON structure for parse GeoJSON field
type GeoJSON struct {
	Type        string      `db:"type" json:"type"`
	Coordinates interface{} `db:"coordinates" json:"coordinates"`
}

// ---------------------------------------------------------------------------------
// GeoData
// ---------------------------------------------------------------------------------

// Scan coordinates to fixed structure [[[]]]
func (data *GeoData) Scan(src interface{}) error {
	val, ok := src.([]byte)
	if !ok {
		// logrus.Error("Unable scan GeoJSON")
		return nil
	}
	var m GeoJSON
	err := json.Unmarshal(val, &m)
	if err != nil {
		logrus.Error("Error unmarshal GeoJSON ", err)
		return nil
	}
	data.Type = m.Type
	if m.Type == "Polygon" {
		data.Coordinates = make([][]GeoPoint, 0)
		valArr := m.Coordinates.([]interface{})
		for _, tmpArr := range valArr {
			polygone := make([]GeoPoint, 0)
			val := tmpArr.([]interface{})
			for _, tmp := range val {
				pval := tmp.([]interface{})
				if len(pval) >= 2 {
					polygone = append(polygone, GeoPoint{Lon: pval[0].(float64), Lat: pval[1].(float64)})
				}
			}
			data.Coordinates = append(data.Coordinates, polygone)
		}
	} else if m.Type == "LineString" {
		data.Coordinates = make([][]GeoPoint, 0)
		polygone := make([]GeoPoint, 0)
		val := m.Coordinates.([]interface{})
		for _, tmp := range val {
			pval := tmp.([]interface{})
			if len(pval) >= 2 {
				polygone = append(polygone, GeoPoint{Lon: pval[0].(float64), Lat: pval[1].(float64)})
			}
		}
		data.Coordinates = append(data.Coordinates, polygone)
	} else if m.Type == "Point" {
		val := m.Coordinates.([]interface{})
		data.Coordinates = [][]GeoPoint{{{Lon: val[0].(float64), Lat: val[1].(float64)}}}
	}

	return nil
}

// ---------------------------------------------------------------------------------
// Calculating
// ---------------------------------------------------------------------------------

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

// calcUnnormalizedInitialBearing returns unnormalized initial bearing in degrees
// (sometimes referred to as forward azimuth) in degrees
//
// http://www.movable-type.co.uk/scripts/latlong.html (section 'Bearing')
func calcUnnormalizedInitialBearing(startPoint, endPoint GeoPoint) float64 {
	p1 := startPoint.Lat // phi(φ)
	l1 := startPoint.Lon // lambda (λ)
	p2 := endPoint.Lat
	l2 := endPoint.Lon
	dLambda := l2 - l1

	y := math.Sin(dLambda) * math.Cos(p2)
	x := math.Cos(p1)*math.Sin(p2) - math.Sin(p1)*math.Cos(p2)*math.Cos(dLambda)
	theta := math.Atan2(y, x) // theta (θ)

	return RadToDeg(theta)
}

// CalcBearing returns initial bearing (sometimes referred to as forward azimuth) in degrees
//
// http://www.movable-type.co.uk/scripts/latlong.html (section 'Bearing')
func CalcInitialBearing(startPoint, endPoint GeoPoint) float64 {
	thetaDeg := calcUnnormalizedInitialBearing(startPoint, endPoint) // theta (θ) in degrees
	return math.Mod(thetaDeg+360, 360)                               // normalizing
}

// CalcBearing returns final bearing in degrees
//
// http://www.movable-type.co.uk/scripts/latlong.html (section 'Bearing')
func CalcFinalBearing(startPoint, endPoint GeoPoint) float64 {
	thetaDeg := calcUnnormalizedInitialBearing(endPoint, startPoint) // theta (θ) in degrees
	return math.Mod(thetaDeg+180, 360)                               // normalizing
}

// calcCrossTrackDistance returns distance of a point from a great-circle path (arc) in meters
// and distance from start point of arc to third point (in degrees)
func calcCrossTrackDistance(arc GeoArc, point GeoPoint) (float64, float64) {
	delta13 := CalcDistance(arc.P1.Lat, arc.P1.Lon, point.Lat, point.Lon) / GeoRadiusAvgM // δ
	theta13 := CalcInitialBearing(arc.P1, point)                                          // θ
	theta12 := CalcInitialBearing(arc.P1, arc.P2)
	dXt := math.Asin(math.Sin(delta13)*math.Sin(theta13-theta12)) * GeoRadiusAvgM

	return dXt, delta13
}

// CalcCrossTrackDistance returns distance of a point from a great-circle path (arc) in meters
func CalcCrossTrackDistance(arc GeoArc, point GeoPoint) float64 {
	dXt, _ := calcCrossTrackDistance(arc, point)
	return dXt
}

// CalcAlongTrackDistance returns distance from the start point to the closest point on the arc (in meters)
func CalcAlongTrackDistance(arc GeoArc, point GeoPoint) float64 {
	dXt, delta13 := calcCrossTrackDistance(arc, point)
	dAt := math.Acos(math.Cos(delta13)/math.Cos(dXt/GeoRadiusAvgM)) * GeoRadiusAvgM

	return dAt
}

// calcHaversineDistance returns distance (in meters) between two geo points calculated by haversine formula
//
// https://en.wikipedia.org/wiki/Great-circle_distance#Computational_formulas
// http://www.movable-type.co.uk/scripts/latlong.html (section 'Distance')
func calcHaversineDistance(point1, point2 GeoPoint) float64 {
	// degrees to radians
	p1 := DegToRad(point1.Lat) // φ
	l1 := DegToRad(point1.Lon) // λ
	p2 := DegToRad(point2.Lat)
	l2 := DegToRad(point2.Lon)
	dp := p2 - p1
	dl := l2 - l1
	// Haversine formula
	a := math.Pow(math.Sin(dp/2), 2) + math.Cos(p1)*math.Cos(p2)*math.Pow(math.Sin(dl/2), 2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	d := GeoRadiusAvgM * c

	return d
}

// CalcHaversineDistance returns distance (in meters) between two geo points calculated by haversine formula
//
// https://en.wikipedia.org/wiki/Great-circle_distance#Computational_formulas
// http://www.movable-type.co.uk/scripts/latlong.html (section 'Distance')
func CalcHaversineDistance(point1, point2 GeoPoint) float64 {
	return calcHaversineDistance(point1, point2)
}

// ---------------------------------------------------------------------------------
// Checking for belong
// ---------------------------------------------------------------------------------

// insideCircleDist return true when point in radius and distance between point & center (in meters)
//
// https://en.wikipedia.org/wiki/Great-circle_distance#Computational_formulas
// http://www.movable-type.co.uk/scripts/latlong.html (`Distance`)
func insideCircleDist(point GeoPoint, center GeoPoint, radius float64) (bool, float64) {
	d := calcHaversineDistance(point, point)
	return d <= radius, d
}

// InsidePolygon return true when point[lon,lat] inside polygon
func InsidePolygon(point GeoPoint, polygon []GeoPoint) bool {
	x := point.Lon
	y := point.Lat

	inside := false
	length := len(polygon)

	j := length - 1
	for i := 0; i < length; i++ {
		xi := polygon[i].Lon
		yi := polygon[i].Lat
		xj := polygon[j].Lon
		yj := polygon[j].Lat

		intersect := ((yi > y) != (yj > y)) && (x < (xj-xi)*(y-yi)/(yj-yi)+xi)
		if intersect {
			inside = !inside
		}
		j = i
	}

	return inside
}

// InsideCircle return true when point in radius
//
// https://en.wikipedia.org/wiki/Great-circle_distance#Computational_formulas
//	http://www.movable-type.co.uk/scripts/latlong.html (`Distance`)
func InsideCircle(point GeoPoint, center GeoPoint, radius float64) bool {
	isInside, _ := insideCircleDist(point, center, radius)
	return isInside
}

// InsideCircleDist return true when point in radius and distance between point & center (in meters)
//
// https://en.wikipedia.org/wiki/Great-circle_distance#Computational_formulas
//	http://www.movable-type.co.uk/scripts/latlong.html (`Distance`)
func InsideCircleDist(point GeoPoint, center GeoPoint, radius float64) (bool, float64) {
	return insideCircleDist(point, center, radius)
}

// InsidePolyline Check inside polyline
func InsidePolyline(point GeoPoint, pnts []GeoPoint, width float64) bool {
	exists := false

	width = width * 100 * 180 / (math.Pi * GeoRadiusAvgM)
	x := point.Lon
	y := point.Lat
	// Point exists in line bounds?
	if pnts[0].Lon < pnts[1].Lon {
		exists = x >= (pnts[0].Lon-width) && x <= (pnts[1].Lon+width)
	} else {
		exists = x >= (pnts[1].Lon-width) && x <= (pnts[0].Lon+width)
	}
	if pnts[0].Lat < pnts[1].Lat {
		exists = exists && y >= (pnts[0].Lat-width) && y <= (pnts[1].Lat+width)
	} else {
		exists = exists && y >= (pnts[1].Lat-width) && y <= (pnts[0].Lat+width)
	}
	if !exists {
		return false
	}

	if pnts[1].Lon == pnts[0].Lon { // Vertical line
		exists = x >= (pnts[0].Lon-width) && x <= (pnts[0].Lon+width)

		if exists {
			if pnts[0].Lat > pnts[1].Lat {
				exists = y >= (pnts[1].Lat-width) && y <= (pnts[0].Lat+width)
			} else {
				exists = y >= (pnts[0].Lat-width) && y <= (pnts[1].Lat+width)
			}
		}
	} else if pnts[0].Lat == pnts[1].Lat { // Horizontal line
		exists = y >= (pnts[0].Lat-width) && y <= (pnts[0].Lat+width)
		if exists {
			if pnts[0].Lon > pnts[1].Lon {
				exists = x >= (pnts[1].Lon-width) && x <= (pnts[0].Lon+width)
			} else {
				exists = x >= (pnts[0].Lon-width) && x <= (pnts[1].Lon+width)
			}
		}
	} else {
		// Other line
		width = width * 2
		x0 := (pnts[0].Lon + (y-pnts[0].Lat)*(pnts[1].Lon-pnts[0].Lon)/(pnts[1].Lat-pnts[0].Lat))
		y0 := (pnts[0].Lat + (x-pnts[0].Lon)*(pnts[1].Lat-pnts[0].Lat)/(pnts[1].Lon-pnts[0].Lon))

		exists = (y <= (y0+width) && y >= (y0-width)) || (x <= (x0+width) && x >= (x0-width))
	}

	return exists
}

// CircleInsideCircle checks if one circle inside another
func CircleInsideCircle(centerIn GeoPoint, radiusIn float64, centerOut GeoPoint, radiusOut float64) bool {
	isInside, d := insideCircleDist(centerIn, centerOut, radiusOut)
	if !isInside {
		return false
	}
	return d+radiusIn <= radiusOut
}

// PolygonInsideCircle checks if polygon inside circle
func PolygonInsideCircle(polygon []GeoPoint, center GeoPoint, radius float64) bool {
	for _, p := range polygon {
		if !InsideCircle(p, center, radius) {
			return false
		}
	}
	return true
}

// PolygonInsidePolygon checks if one polygon inside another
func PolygonInsidePolygon(polygonIn []GeoPoint, polygonOut []GeoPoint) bool {
	for _, p := range polygonIn {
		if !InsidePolygon(p, polygonOut) {
			return false
		}
	}
	return true
}
