package server

import (
	"context"

	api "github.com/mohitkumar/george/api/v1"
	"github.com/mohitkumar/george/geo"
	"google.golang.org/grpc"
)

type Config struct {
	geo geo.Geo
}

type grpcServer struct {
	api.UnimplementedGeoServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func NewGrpcServer(config *Config) (*grpc.Server, error) {
	server := grpc.NewServer()

	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}

	api.RegisterGeoServer(server, srv)
	return server, nil
}

func (s *grpcServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	point := req.GetPoint()
	data := req.GetData()
	err := s.geo.Put(geo.GeoPoint{Latitude: point.Latitude, Longitude: point.Longitude}, data)
	if err != nil {
		return nil, err
	}
	return &api.PutResponse{Success: true}, nil

}

func (s *grpcServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	point := req.GetPoint()
	data, err := s.geo.Get(geo.GeoPoint{Latitude: point.Latitude, Longitude: point.Longitude})
	if err != nil {
		return nil, err
	}
	return &api.GetResponse{Data: data}, nil
}
