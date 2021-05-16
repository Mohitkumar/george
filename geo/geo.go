package geo

import (
	"log"
	"strconv"

	badger "github.com/dgraph-io/badger/v3"
)

type GeoPoint struct {
	Latitude  float64
	Longitude float64
}

type Geo interface {
	Put(point GeoPoint, data []byte) error

	Get(point GeoPoint) ([]byte, error)

	Close() error
}

type Config struct {
	Dir string
}

type store struct {
	DB *badger.DB
}

func NewStore(conf Config) *store {
	db, err := badger.Open(badger.DefaultOptions(conf.Dir))
	if err != nil {
		log.Fatal(err)
		panic("failed to open Db in specefied directory")
	}
	s := &store{
		DB: db,
	}
	return s
}

func (st *store) Put(point GeoPoint, data []byte) (err error) {
	hash := ToGeoHash(point.Latitude, point.Longitude)
	hashStr := strconv.FormatUint(hash, 10)
	err = st.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(hashStr), data)
		return err
	})
	return err
}

func (st *store) Get(point GeoPoint) (data []byte, err error) {
	hash := ToGeoHash(point.Latitude, point.Longitude)
	hashStr := strconv.FormatUint(hash, 10)
	var valCopy []byte

	err = st.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(hashStr))
		if err != nil {
			return err
		}
		valCopy = make([]byte, item.ValueSize())
		_, err = item.ValueCopy(valCopy)
		return err
	})
	if err != nil {
		return nil, err
	}
	return valCopy, nil
}

func (st *store) Close() error {
	return st.DB.Close()
}
