package client

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pkg/errors"
	"github.com/tescherm/mc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Value []byte

type Item struct {
	Key   string
	Value []byte

	casID int64
}

var ErrCASConflict = errors.New("compare-and-swap conflict")

type MemcachedClient interface {
	Get(ctx context.Context, key string) (*Item, error)
	Set(ctx context.Context, item *Item) error
	CompareAndSwap(ctx context.Context, item *Item) error
	Remove(ctx context.Context, key string) (*Item, error)
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

func (c *client) Get(ctx context.Context, key string) (*Item, error) {
	res, err := c.grpc.Get(ctx, &memcached.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "cache get (%s) failed", key)
	}

	return fromMemcachedItem(res.Item), nil
}

func (c *client) Set(ctx context.Context, item *Item) error {
	_, err := c.grpc.Set(ctx, &memcached.SetRequest{
		Item: toMemcachedItem(item),
	})
	if err != nil {
		return errors.Wrapf(err, "cache set (%s) failed", item.Key)
	}
	return nil
}

func (c *client) CompareAndSwap(ctx context.Context, item *Item) error {
	_, err := c.grpc.CompareAndSwap(ctx, &memcached.CompareAndSwapRequest{
		Item: toMemcachedItem(item),
	})
	if err != nil {
		code := status.Code(err)
		if code == codes.Aborted {
			return ErrCASConflict
		}
		return errors.Wrapf(err, "cache compare-and-swap (%s) failed", item.Key)
	}
	return nil
}

func (c *client) Remove(ctx context.Context, key string) (*Item, error) {
	res, err := c.grpc.Remove(ctx, &memcached.RemoveRequest{
		Key: key,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "cache remove (%s) failed", key)
	}
	return fromMemcachedItem(res.Item), nil
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

func toMemcachedItem(item *Item) *memcached.Item {
	if item == nil {
		return nil
	}
	return &memcached.Item{
		Key:   item.Key,
		Value: item.Value,
		CasID: item.casID,
	}
}

func fromMemcachedItem(item *memcached.Item) *Item {
	if item == nil {
		return nil
	}
	return &Item{
		Key:   item.Key,
		Value: item.Value,
		casID: item.CasID,
	}
}
