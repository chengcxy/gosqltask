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
var Cmd string
var Dir string
var Fid string
func init() {
	flag.StringVar(&ConfigPath, "c", "../config/", "配置文件目录")
	flag.StringVar(&Env, "e", "dev", "运行的环境-json文件前缀 dev/test/prod/local")
	flag.StringVar(&TaskId, "id", "1",  "任务id")
	flag.BoolVar(&Debug, "d", false, "debug执行的sql")
	flag.BoolVar(&Debug, "debug", false, "debug执行的sql")
	flag.BoolVar(&UsedEnv, "UsedEnv", true, "是否走环境变量")
	flag.StringVar(&Cmd, "cmd", "execute",  "走计算任务还是导出任务")
	flag.StringVar(&Dir, "dir", "../data",  "excel数据存放的目录 默认 ../data")
	flag.StringVar(&Fid, "fid", "",  "时间戳")
	flag.Parse()
	log.Printf("ConfigPath: %s ,Env: %s ,TaskId: %s Debug:%v ",ConfigPath, Env,TaskId,Debug)
}


func main(){
	StartTime := time.Now()
	config := configor.NewConfig(ConfigPath, Env,UsedEnv)
	sd := scheduler.NewScheduler(config,TaskId,StartTime)
	result := sd.Run(Debug,Cmd,Dir,Fid).(*scheduler.MessageResult)
	log.Printf("result:%s",result.Message())
}