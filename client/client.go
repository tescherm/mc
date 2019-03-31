package client

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/errors"
	"github.com/tescherm/mc/pb"
	"google.golang.org/grpc"
)

type Value []byte

type MemcachedClient interface {
	Get(ctx context.Context, key string) (Value, error)
	Set(ctx context.Context, key string, value Value) error
	Remove(ctx context.Context, key string) (Value, error)
	Clear(ctx context.Context) error
	Size(ctx context.Context) (uint64, error)
}

type Config struct {
	ServiceURI string
}

type client struct {
	grpc memcached.MemcachedClient
}

func New(config Config) (MemcachedClient, error) {
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(3 * time.Second),

		grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
	}

	addr := config.ServiceURI
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial %s", addr)
	}
	g := memcached.NewMemcachedClient(conn)
	return &client{grpc: g}, nil
}

func (c *client) Get(ctx context.Context, key string) (Value, error) {
	res, err := c.grpc.Get(ctx, &memcached.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "cache get (%s) failed", key)
	}
	return res.Item.Value, nil
}

func (c *client) Set(ctx context.Context, key string, value Value) error {
	_, err := c.grpc.Set(ctx, &memcached.SetRequest{
		Item: &memcached.Item{
			Key:   key,
			Value: value,
		},
	})
	if err != nil {
		return errors.Wrapf(err, "cache set (%s) failed", key)
	}
	return nil
}

func (c *client) Remove(ctx context.Context, key string) (Value, error) {
	res, err := c.grpc.Remove(ctx, &memcached.RemoveRequest{
		Key: key,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "cache remove (%s) failed", key)
	}
	return res.Item.Value, nil
}

func (c *client) Clear(ctx context.Context) error {
	_, err := c.grpc.Clear(ctx, &memcached.ClearRequest{})
	if err != nil {
		return errors.Wrapf(err, "cache clear failed")
	}
	return nil
}

func (c *client) Size(ctx context.Context) (uint64, error) {
	res, err := c.grpc.Size(ctx, &memcached.SizeRequest{})
	if err != nil {
		return 1, errors.Wrapf(err, "cache get size failed")
	}
	return res.Size, nil
}
