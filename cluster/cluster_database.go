// Package cluster provides a server side cluster which is transparent to client.
// You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	pool "github.com/jolestar/go-commons-pool/v2"

	"go-redis/config"
	"go-redis/database"
	databaseface "go-redis/interface/database"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
)

// MyClusterDatabase represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type MyClusterDatabase struct {
	self           string
	nodes          []string
	peerPicker     *consistenthash.NodeMap
	peerConnection map[string]*pool.ObjectPool
	db             databaseface.Database
}

// MakeClusterDatabase creates and starts a node of cluster
func MakeClusterDatabase() *MyClusterDatabase {
	cluster := &MyClusterDatabase{
		self: config.Properties.Self,

		db:             database.NewStandaloneDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *MyClusterDatabase, c resp.Connection, args [][]byte) resp.Reply

// Close stops current node of cluster
func (cl *MyClusterDatabase) Close() {
	cl.db.Close()
}

var router = makeRouter()

// Exec executes command on cluster
func (cl *MyClusterDatabase) Exec(c resp.Connection, cmdLine [][]byte) (res resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			res = &reply.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" +
			cmdName + "', or not supported in cluster mode")
	}
	res = cmdFunc(cl, c, cmdLine)
	return
}

// AfterClientClose does some clean after client close connection
func (cl *MyClusterDatabase) AfterClientClose(c resp.Connection) {
	cl.db.AfterClientClose(c)
}
