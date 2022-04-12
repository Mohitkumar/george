package main

import (
	"fmt"

	"github.com/mohitkumar/george/geo"
	"github.com/mohitkumar/george/persistence"
)

func main() {
	config := persistence.DynamoConfig{
		EndPoint:             "http://localhost:8000",
		AWSRegion:            "mumbai",
		TableName:            "testtable",
		GeoHashKeyColumnName: "geok",
		GeoHashColumnName:    "geohash",
		GeoHashKeyLength:     6,
		RCU:                  10,
		WCU:                  10,
	}
	db, err := persistence.NewDynamoStore(config)
	//err = db.CreateTable(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	geo := geo.New(db)
	data1 := make(map[string]interface{})
	data1["userId"] = 123
	geo.Put(12.120000, 76.680000, data1)
	data2 := make(map[string]interface{})
	data2["userId"] = 124
	geo.Put(12.140000, 76.680000, data2)

	res, err := geo.RadiusQuery(12.120000, 76.680000, 1.0)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(res)
}
