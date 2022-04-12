package s2

import (
	"math"
	"strconv"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
)

const EARTH_RADIUS float64 = 6371000.0

func ToGeoHash(latitude float64, longitude float64) uint64 {
	var latLng = s2.LatLngFromDegrees(latitude, longitude)
	cell := s2.CellFromLatLng(latLng)
	return uint64(cell.ID())
}

func ExtractHashKey(hash uint64, keyLenght int) uint64 {
	hashStr := strconv.FormatUint(hash, 10)
	denominator := uint64(math.Pow(10, float64(len(hashStr)-keyLenght)))
	if denominator == 0 {
		return hash
	}

	return hash / denominator

}

func NearbyCellIds(latitude float64, longitude float64, radius float64) []s2.CellID {
	p := s2.PointFromLatLng(s2.LatLngFromDegrees(latitude, longitude))
	angle := s1.Angle((radius * 1000.0) / 6371000.0)
	cap := s2.CapFromCenterAngle(p, angle)
	region := s2.Region(cap)

	rc := &s2.RegionCoverer{MaxLevel: 20, MinLevel: 9}
	cellUnion := rc.Covering(region)
	cellIds := []s2.CellID(cellUnion)
	return cellIds
}
