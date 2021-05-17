package agent

import (
	"fmt"
	"net"
	"sync"

	"github.com/mohitkumar/george/discovery"
	"github.com/mohitkumar/george/geo"
	"github.com/mohitkumar/george/server"
	"google.golang.org/grpc"
)

type Config struct {
	DataDir       string
	BindAddr      string
	NodeName      string
	RPCPort       int
	StartJoinAddr []string
}
type Agent struct {
	Config
	geoDb        geo.Geo
	server       *grpc.Server
	membership   *discovery.Membership
	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupGeoDB,
		a.setupServer,
		a.SetupMemberhship,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Agent) setupGeoDB() error {
	a.geoDb = geo.NewStore(geo.Config{Dir: a.DataDir})
	return nil
}

func (a *Agent) setupServer() error {
	config := &server.Config{
		geo: a.geoDb,
	}
	ser, err := server.NewGrpcServer(config)
	if err != nil {
		return err
	}
	a.server = ser
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := a.server.Serve(listener); err != nil {
			a.ShutDown()
		}
	}()
	return err
}

func (a *Agent) ShutDown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.geoDb.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) SetupMemberhship() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	handler := discovery.NewLoggerHandler()

	mem, err := discovery.New(handler, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddr,
	})
	a.membership = mem
	return err
}
