package main


import (
	"flag"
	"log"
	"github.com/chengcxy/gotools/configor"
	"github.com/chengcxy/gosqltask/scheduler"
)

var ConfigPath string
var Env string
var TaskId string

var Debug bool

func init() {
	flag.StringVar(&ConfigPath, "c", "./config/", "配置文件目录")
	flag.StringVar(&Env, "e", "test", "运行的环境-json文件前缀 dev/test/prod/local")
	flag.StringVar(&TaskId, "id", "1",  "任务id")
	flag.BoolVar(&Debug, "d", false, "debug执行的sql")
	flag.Parse()
	log.Printf("ConfigPath: %s ,Env: %s ,TaskId: %s Debug:%v ",ConfigPath, Env,TaskId,Debug)
}


func main(){
	config := configor.NewConfig(ConfigPath, Env)
	sd := scheduler.NewScheduler(config,TaskId)
	sd.Run(false)
}




