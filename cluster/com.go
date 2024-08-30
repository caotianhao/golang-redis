package cluster

import (
	"context"
	"errors"
	"strconv"

	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/client"
	"go-redis/resp/reply"
)

func (cl *MyClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cl.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}
	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

func (cl *MyClusterDatabase) returnPeerClient(peer string, cli *client.Client) error {
	connectionFactory, ok := cl.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), cli)
}

// relay relays command to peer
// select db by c.GetDBIndex()
// cannot call Prepare, Commit, execRollback of self node
func (cl *MyClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cl.self {
		// to self db
		return cl.db.Exec(c, args)
	}
	peerClient, err := cl.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cl.returnPeerClient(peer, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

// broadcast broadcasts command to all node in cluster
func (cl *MyClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	result := make(map[string]resp.Reply)
	for _, node := range cl.nodes {
		rly := cl.relay(node, c, args)
		result[node] = rly
	}
	return result
}
