package main


import (
	"flag"
	"log"
	"time"
	"github.com/chengcxy/gotools/configor"
	"github.com/chengcxy/gosqltask/scheduler"
)

var ConfigPath string
var Env string
var TaskId string

var Debug bool

func init() {
	flag.StringVar(&ConfigPath, "c", "./config/", "配置文件目录")
	flag.StringVar(&Env, "e", "dev", "运行的环境-json文件前缀 dev/test/prod/local")
	flag.StringVar(&TaskId, "id", "1",  "任务id")
	flag.BoolVar(&Debug, "d", false, "debug执行的sql")
	flag.BoolVar(&Debug, "debug", false, "debug执行的sql")
	flag.Parse()
	log.Printf("ConfigPath: %s ,Env: %s ,TaskId: %s Debug:%v ",ConfigPath, Env,TaskId,Debug)
}


func main(){
	startTime := time.Now()
	config := configor.NewConfig(ConfigPath, Env)
	sd := scheduler.NewScheduler(config,TaskId)
	sd.Run(Debug)
	endTime := time.Now()
	log.Println("costs ",endTime.Sub(startTime))
}




