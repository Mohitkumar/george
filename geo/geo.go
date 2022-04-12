package geo

import (
	"github.com/mohitkumar/george/persistence"
)

type Geo interface {
	Put(latitude float64, longitude float64, data []byte) error

	Get(latitude float64, longitude float64) ([]byte, error)

	RadiusQuery(latitude float64, longitude float64, radius float64) ([]map[string]interface{}, error)
}

var _ = (*Geo)(nil)

type geoImpl struct {
	database persistence.DB
}

func New(db persistence.DB) *geoImpl {

	g := &geoImpl{
		database: db,
	}
	return g
}

func (g *geoImpl) Put(latitude float64, longitude float64, data map[string]interface{}) (err error) {
	return g.database.Put(latitude, longitude, data)
}

func (g *geoImpl) Get(latitude float64, longitude float64) (data map[string]interface{}, err error) {
	out, err := g.database.Get(latitude, longitude)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (g *geoImpl) RadiusQuery(latitude float64, longitude float64, radius float64) ([]map[string]interface{}, error) {
	return g.database.RadiusQuery(latitude, longitude, radius)
}
