package core

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tescherm/mc/core/cache"
	"github.com/tescherm/mc/core/caches"
	"github.com/tescherm/mc/pb"
)

type MemcachedService struct {
	Caches *caches.Caches
	Logger logrus.FieldLogger
}

type Config struct {
	Caches *caches.Caches
	Logger logrus.FieldLogger
}

func New(config Config) *MemcachedService {
	logger := config.Logger.WithField("module", "service")

	return &MemcachedService{
		Caches: config.Caches,
		Logger: logger,
	}
}

func (s *MemcachedService) Get(ctx context.Context, req *memcached.GetRequest) (*memcached.GetResponse, error) {
	key := req.Key

	s.Logger.WithField("key", key).Info("Get")

	c, err := s.pick(key)
	if err != nil {
		return nil, err
	}

	value := c.Get(key)

	res := &memcached.GetResponse{
		Item: &memcached.Item{
			Key:   key,
			Value: value,
		},
	}
	return res, nil
}

func (s *MemcachedService) Set(ctx context.Context, req *memcached.SetRequest) (*memcached.SetResponse, error) {
	key := req.Item.Key
	value := req.Item.Value

	s.Logger.WithField("key", key).Info("Set")

	c, err := s.pick(key)
	if err != nil {
		return nil, err
	}

	c.Set(key, value)

	res := &memcached.SetResponse{
		Item: &memcached.Item{
			Key:   key,
			Value: value,
		},
	}
	return res, nil
}

func (s *MemcachedService) Remove(ctx context.Context, req *memcached.RemoveRequest) (*memcached.RemoveResponse, error) {
	key := req.Key

	s.Logger.WithField("key", key).Info("Remove")

	c, err := s.pick(key)
	if err != nil {
		return nil, err
	}

	value := c.Remove(key)
	res := &memcached.RemoveResponse{
		Item: &memcached.Item{
			Key:   key,
			Value: value,
		},
	}
	return res, nil
}

func (s *MemcachedService) Clear(ctx context.Context, req *memcached.ClearRequest) (*memcached.ClearResponse, error) {
	s.Logger.Info("Clear")

	s.Caches.Clear()
	return &memcached.ClearResponse{}, nil
}

func (s *MemcachedService) Size(ctx context.Context, req *memcached.SizeRequest) (*memcached.SizeResponse, error) {
	s.Logger.Info("Size")

	size := s.Caches.Size()
	res := &memcached.SizeResponse{
		Size: size,
	}
	return res, nil
}

func (s *MemcachedService) pick(key string) (cache.Cache, error) {
	c := s.Caches.CacheForKey(key)
	if c == nil {
		err := fmt.Errorf("unable to get cache for %s", key)
		return nil, errors.WithStack(err)
	}
	return c, nil
}
