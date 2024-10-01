package database

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"

	"go-redis/aof"
	"go-redis/config"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
)

// StandaloneDatabase is a set of multiple database set
type StandaloneDatabase struct {
	dbSet []*DB
	// handle aof persistence
	aofHandler *aof.MyAofHandler
}

// NewStandaloneDatabase creates a redis database,
func NewStandaloneDatabase() *StandaloneDatabase {
	mdb := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	mdb.dbSet = make([]*DB, config.Properties.Databases)
	for i := range mdb.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(mdb)
		if err != nil {
			panic(err)
		}
		mdb.aofHandler = aofHandler
		for _, db := range mdb.dbSet {
			// avoid closure
			singleDB := db
			singleDB.addAof = func(line CmdLine) {
				mdb.aofHandler.AddAof(singleDB.index, line)
			}
		}
	}
	return mdb
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (mdb *StandaloneDatabase) Exec(c resp.Connection, line [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmdName := strings.ToLower(string(line[0]))
	if cmdName == "select" {
		if len(line) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, line[1:])
	}
	// normal commands
	dbIndex := c.GetDBIndex()
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(line)
}

// Close graceful shutdown database
func (mdb *StandaloneDatabase) Close() {
}

// AfterClientClose 在客户端关闭连接后执行
func (mdb *StandaloneDatabase) AfterClientClose(_ resp.Connection) {
}

func execSelect(c resp.Connection, mdb *StandaloneDatabase, args [][]byte) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}
