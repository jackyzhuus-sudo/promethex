package server

import (
	"context"
	"fmt"
	"runtime/debug"

	"block-listener/internal/biz"
	"block-listener/pkg/alarm"

	"github.com/go-kratos/kratos/v2/log"
)

// BlockScannerServer 实现了Kratos的Server接口
type BlockScannerServer struct {
	scanner *biz.BlockScanner
	log     *log.Helper
}

// NewBlockScannerServer 创建区块扫描服务
func NewBlockScannerServer(scanner *biz.BlockScanner, logger log.Logger) *BlockScannerServer {
	return &BlockScannerServer{
		scanner: scanner,
		log:     log.NewHelper(logger),
	}
}

// Start 启动区块扫描服务
func (s *BlockScannerServer) Start(ctx context.Context) error {
	s.log.Info("区块扫描服务启动中")
	ctx = context.WithValue(ctx, "server", "BLOCK_SCANNER")
	go func() {
		defer func() {
			if err := recover(); err != nil {
				s.log.Errorf("block scanner panic: %v, stack: %s", err, string(debug.Stack()))
				msg := fmt.Sprintf("block scanner panic: [%v], stack: [%s]", err, string(debug.Stack()))
				alarm.Lark.Send(msg)
			}
		}()
		s.scanner.Start(ctx)
	}()
	return nil
}

// Stop 停止区块扫描服务
func (s *BlockScannerServer) Stop(ctx context.Context) error {
	s.log.Info("区块扫描服务停止中")
	return s.scanner.Stop(ctx)
}
