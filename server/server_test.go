package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/mohitkumar/george/api/v1"
	"github.com/mohitkumar/george/geo"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	testMap := map[string]func(
		t *testing.T,
		client api.GeoClient,
		config *Config,
	){"put success": testPut, "get success": testGet}

	for scenario, fn := range testMap {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T) (
	client api.GeoClient,
	config *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	store := geo.NewStore(geo.Config{Dir: dir})
	config = &Config{
		geo: store,
	}

	server, err := NewGrpcServer(config)
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client = api.NewGeoClient(cc)

	return client, config, func() {
		server.Stop()
		cc.Close()
		l.Close()
		store.Close()
	}

}

func testPut(t *testing.T, client api.GeoClient, config *Config) {
	ctx := context.Background()

	want := true

	response, err := client.Put(ctx, &api.PutRequest{Point: &api.GeoPoint{Latitude: 20.33, Longitude: 102.99}, Data: []byte("data")})
	require.NoError(t, err)

	require.Equal(t, want, response.GetSuccess())
}

func testGet(t *testing.T, client api.GeoClient, config *Config) {
	ctx := context.Background()

	want := "data"

	_, err := client.Put(ctx, &api.PutRequest{Point: &api.GeoPoint{Latitude: 20.33, Longitude: 102.99}, Data: []byte("data")})
	require.NoError(t, err)
	response, err := client.Get(ctx, &api.GetRequest{Point: &api.GeoPoint{Latitude: 20.33, Longitude: 102.99}})
	require.NoError(t, err)

	require.Equal(t, want, string(response.GetData()))
}
