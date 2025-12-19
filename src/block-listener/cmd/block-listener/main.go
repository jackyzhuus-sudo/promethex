package main

import (
	"flag"
	"net/http"
	"os"

	"block-listener/internal/conf"
	"block-listener/internal/server"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/go-kratos/kratos/v2/log"

	"block-listener/pkg/alarm"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name string
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config flag.
	flagconf string

	id, _ = os.Hostname()
)

// newApp 创建Kratos应用
func newApp(scannerSrv *server.BlockScannerServer, eventProcessorSrv *server.EventProcessorServer, schedulerSrv *server.SchedulerServer, logger log.Logger) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Logger(logger),
		kratos.Server(
			scannerSrv,
			eventProcessorSrv,
			schedulerSrv,
		),
	)
}

func init() {
	flag.StringVar(&flagconf, "conf", "./configs", "config path, eg: -conf config.yaml")
}

func main() {
	flag.Parse()

	logger := log.With(log.NewStdLogger(os.Stdout),
		"ts", log.DefaultTimestamp,
		"caller", log.DefaultCaller,
		"service.id", id,
		"service.name", Name,
	)
	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}

	var custom conf.Custom
	if err := c.Scan(&custom); err != nil {
		panic(err)
	}

	app, cleanup, err := wireApp(&bc, &custom, logger)
	if err != nil {
		panic(err)
	}
	defer cleanup()
	alarm.InitLarkAlarm(bc.Custom, logger)

	// 启动健康检查HTTP服务
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"ok"}`))
		})
		if err := http.ListenAndServe(":8080", nil); err != nil {
			logger.Log(log.LevelError, "msg", "健康检查服务启动失败", "error", err)
		}
	}()

	// 运行主应用
	if err := app.Run(); err != nil {
		panic(err)
	}
}
