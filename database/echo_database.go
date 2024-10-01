package database

import (
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
)

// EchoDatabase 结构体是一个简单的数据库实现
type EchoDatabase struct {
}

// Exec 函数用于执行命令，将接收到的参数原样返回
func (e EchoDatabase) Exec(_ resp.Connection, args [][]byte) resp.Reply {
	return reply.MakeMultiBulkReply(args) // 将接收到的参数转换为MultiBulkReply返回
}

// AfterClientClose 函数在客户端连接关闭后调用，记录日志
func (e EchoDatabase) AfterClientClose(resp.Connection) {
	logger.Info("EchoDatabase AfterClientClose") // 打印客户端关闭后的日志信息
}

// Close 函数用于关闭数据库，记录日志
func (e EchoDatabase) Close() {
	logger.Info("EchoDatabase Close") // 打印数据库关闭的日志信息
}
