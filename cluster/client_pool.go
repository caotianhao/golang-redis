package cluster

import (
	"context"
	"errors"

	"github.com/jolestar/go-commons-pool/v2"

	"go-redis/resp/client"
)

type connFactory struct {
	Peer string
}

func (f *connFactory) MakeObject(context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()
	return pool.NewPooledObject(c), nil
}

func (f *connFactory) DestroyObject(_ context.Context, obj *pool.PooledObject) error {
	c, ok := obj.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	c.Close()
	return nil
}

func (f *connFactory) ValidateObject(context.Context, *pool.PooledObject) bool {
	// do validate
	return true
}

func (f *connFactory) ActivateObject(context.Context, *pool.PooledObject) error {
	// do activate
	return nil
}

func (f *connFactory) PassivateObject(context.Context, *pool.PooledObject) error {
	// do passivate
	return nil
}
