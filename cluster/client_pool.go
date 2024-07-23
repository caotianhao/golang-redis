package cluster

import (
	"context"
	"errors"
	"github.com/jolestar/go-commons-pool/v2"
	"go-redis/resp/client"
)

type connectionFactory struct {
	Peer string
}

func (f *connectionFactory) MakeObject(context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()
	return pool.NewPooledObject(c), nil
}

func (f *connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	c.Close()
	return nil
}

func (f *connectionFactory) ValidateObject(context.Context, *pool.PooledObject) bool {
	// do validate
	return true
}

func (f *connectionFactory) ActivateObject(context.Context, *pool.PooledObject) error {
	// do activate
	return nil
}

func (f *connectionFactory) PassivateObject(context.Context, *pool.PooledObject) error {
	// do passivate
	return nil
}
