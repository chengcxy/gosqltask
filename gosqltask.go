package main


import (

	"github.com/chengcxy/gotools/configor"
	"github.com/chengcxy/gosqltask/scheduler"
)



func main(){
	taskId := "1"
	configPath := "/Users/chengxinyao/go/src/github.com/chengcxy/gosqlboy/config"
	Env := "dev"
	config := configor.NewConfig(configPath,Env)
	sd := scheduler.NewScheduler(config,taskId)
	sd.Run()
}




