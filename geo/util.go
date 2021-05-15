package geo

import "github.com/golang/geo/s2"

func ToGeoHash(latitude float64, longitude float64) uint64 {
	var latLng = s2.LatLngFromDegrees(latitude, longitude)
	cell := s2.CellFromLatLng(latLng)
	return uint64(cell.ID())
}
