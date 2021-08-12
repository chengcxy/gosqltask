package main

import (
	"flag"
	"github.com/chengcxy/gosqltask/scheduler"
	"github.com/chengcxy/gotools/configor"
	"log"
	"time"
)

var ConfigPath string
var Env string
var UsedEnv bool
var TaskId string
var Debug bool

func init() {
	flag.StringVar(&ConfigPath, "c", "./config/", "配置文件目录")
	flag.StringVar(&Env, "e", "dev", "运行的环境-json文件前缀 dev/test/prod/local")
	flag.StringVar(&TaskId, "id", "1",  "任务id")
	flag.BoolVar(&Debug, "d", false, "debug执行的sql")
	flag.BoolVar(&Debug, "debug", false, "debug执行的sql")
	flag.BoolVar(&UsedEnv, "UsedEnv", true, "是否走环境变量")
	flag.Parse()
	log.Printf("ConfigPath: %s ,Env: %s ,TaskId: %s Debug:%v ",ConfigPath, Env,TaskId,Debug)
}


func main(){
	StartTime := time.Now()
	config := configor.NewConfig(ConfigPath, Env,UsedEnv)
	sd := scheduler.NewScheduler(config,TaskId,StartTime)
	sd.Run(Debug)
}




