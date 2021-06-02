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

// ---------------------------------------------------------------------------------
// Checking for belong
// ---------------------------------------------------------------------------------

// insideCircleDist return true when point in radius and distance between point & center (in meters)
//
// https://en.wikipedia.org/wiki/Great-circle_distance#Computational_formulas
// http://www.movable-type.co.uk/scripts/latlong.html (`Distance`)
func insideCircleDist(point GeoPoint, center GeoPoint, radius float64) (bool, float64) {
	// degrees to radians
	x1 := DegToRad(center.Lat)
	y1 := DegToRad(center.Lon)
	x2 := DegToRad(point.Lat)
	y2 := DegToRad(point.Lon)
	dx := x2 - x1
	dy := y2 - y1
	// Haversine formula
	a := math.Pow(math.Sin(dx/2), 2) + math.Cos(x1)*math.Cos(x2)*math.Pow(math.Sin(dy/2), 2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	d := GeoRadiusAvgM * c
	return d <= radius, d
}

// InsidePolygon return true when point[lon,lat]  inside polygone
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
