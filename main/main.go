package main

import "github.com/mohitkumar/george/geo"

func main() {
	config := &geo.Config{
		Dir: "/tmp/geo",
	}

	store := geo.NewStore(*config)

	point := geo.GeoPoint{Latitude: 98.97, Longitude: 112.98}
	//store.Put(point, []byte("data"))
	data, err := store.Get(point)
	if err != nil {
		println("error occured", err)
	}
	println(string(data))
}
