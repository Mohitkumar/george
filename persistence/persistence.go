package persistence

type DB interface {
	Put(latitude float64, longitued float64, data map[string]interface{}) error

	Get(latitude float64, longitued float64) (map[string]interface{}, error)

	RadiusQuery(latitude float64, longitude float64, radius float64) ([]map[string]interface{}, error)
}
