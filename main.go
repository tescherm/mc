package main

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ianschenck/envflag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/tescherm/mc/core"
	"github.com/tescherm/mc/core/caches"
	"github.com/tescherm/mc/metrics"
	pb "github.com/tescherm/mc/pb"
	"google.golang.org/grpc"
)

var (
	apiPort      = envflag.Int("API_PORT", 8080, "service API listen port")
	cacheCount   = envflag.Int("NUM_CACHES", 20, "Number of caches")
	capacityFlag = envflag.String("CAPACITY", "128m", "cache size")
	metricsPort  = envflag.Int("METRICS_PORT", 9090, "service metrics listen port")
	loglevel     = envflag.String("LOG_LEVEL", "info", "log level")
	replicas     = envflag.Int("NUM_REPLICAS", 160, "number of cache node replicas")
)

var (
	logger = logrus.NewEntry(logrus.New())
)

func newServer(c *caches.Caches) *core.MemcachedService {
	s := core.New(core.Config{
		Caches: c,
		Logger: logger,
	})
	return s
}

func initLogging() {
	lvl, err := logrus.ParseLevel(*loglevel)
	if err == nil {
		logrus.SetLevel(lvl)
	} else {
		logrus.Fatalf("Could not parse log level '%s': %s", *loglevel, err.Error())
	}
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	})
	logger = logrus.WithFields(logrus.Fields{
		"service": "mc",
	})
}

func main() {
	envflag.Parse()
	initLogging()

	capacity, err := humanize.ParseBytes(*capacityFlag)
	if err != nil {
		logger.WithError(err).Fatalf("invalid CAPACITY: %s", *capacityFlag)
	}

	logger.WithFields(logrus.Fields{
		"API_PORT":     *apiPort,
		"CAPACITY":     *capacityFlag,
		"LOG_LEVEL":    *loglevel,
		"METRICS_PORT": *metricsPort,
		"NUM_CACHES":   *cacheCount,
		"NUM_REPLICAS": *replicas,
	}).Info("starting service")

	apiAddr := net.JoinHostPort("0.0.0.0", strconv.Itoa(*apiPort))
	lis, err := net.Listen("tcp", apiAddr)
	if err != nil {
		logger.WithError(err).Fatal("tcp Listen failed")
	}

	c := caches.New(caches.Config{
		Capacity:   capacity,
		CacheCount: *cacheCount,
		Replicas:   *replicas,
	})

	grpc_prometheus.EnableHandlingTimeHistogram()

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	pb.RegisterMemcachedServer(grpcServer, newServer(c))
	grpc_prometheus.Register(grpcServer)

	prometheus.MustRegister(metrics.NewCacheCollector(c))
	http.Handle("/metrics", promhttp.Handler())

	// start http (metrics) server
	metricsAddr := net.JoinHostPort("0.0.0.0", strconv.Itoa(*metricsPort))
	go func() {
		err := http.ListenAndServe(metricsAddr, nil)
		// expected error on shutdown
		if err == http.ErrServerClosed {
			logger.Infof("ListenAndServe response: %v", err)
		} else {
			logger.WithError(err).Fatal("http server listen failed")
		}
	}()

	// start grpc server
	go func() {
		err := grpcServer.Serve(lis)
		// expected error on shutdown
		if err == grpc.ErrServerStopped {
			logger.Infof("Serve response: %v", err)
		} else {
			logger.WithError(err).Fatal("grpc server listen failed")
		}
	}()

	logger.WithFields(logrus.Fields{
		"address": apiAddr,
	}).Info("started API server")

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.WithField("signal", sig).Info("caught signal")

	grpcServer.GracefulStop()

	logger.Info("server exit")
}
