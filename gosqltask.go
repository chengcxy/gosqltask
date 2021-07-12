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

var Cmd string

func init() {
	flag.StringVar(&ConfigPath, "c", "./config/", "配置文件目录")
	flag.StringVar(&Env, "e", "test", "运行的环境-json文件前缀 dev/test/prod/local")
	flag.StringVar(&TaskId, "id", "1",  "任务id")
	flag.StringVar(&Cmd, "cmd", " cat ",  "")
	flag.Parse()
	log.Printf("ConfigPath: %s ,Env: %s ,TaskId: %s ",ConfigPath, Env,TaskId)
}


func main(){
	config := configor.NewConfig(ConfigPath, Env)
	sd := scheduler.NewScheduler(config,TaskId)
	sd.Run(Cmd)
}




