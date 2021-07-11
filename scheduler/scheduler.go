package scheduler


import (
	"log"
	"encoding/json"
    "github.com/chengcxy/gotools/configor"
	"github.com/chengcxy/gotools/roboter"
) 


type Scheduler struct {
	config *configor.Config
	taskId string
	robot roboter.Roboter
	taskInfo *TaskInfo

}

func NewScheduler(config *configor.Config,taskId string) *Scheduler{
	sd := &Scheduler{
		config:config,
		taskId:taskId,
		robot:roboter.GetRoboter(config),
	}
	sd.GetTaskInfo()
	return sd
}

func (sd *Scheduler)GetTaskInfo(){
	qt,ok := sd.config.Get("taskmeta.query_task")
	if !ok{
		log.Fatal("get taskmeta.query_task error")
	}
	QueryTaskSql := qt.(string)
	log.Println("QueryTaskSql is ",QueryTaskSql)
	mysql := NewMysqlClient(sd.config,"taskmeta.conn")
	defer func(){
		mysql.Close()
		log.Println("ok! query taskmeta mysql client closed")
	}()
	datas,_, err := mysql.Query(QueryTaskSql,sd.taskId)
	if err != nil{
		log.Println("query taskmeta err",err)
		panic("query taskmeta err")
	}
	if len(datas) != 1{
		panic("taskId not in taskTable")
	}
	meta := datas[0]//map[string]string
	metaBytes,err := json.Marshal(meta)
	if err != nil{
		log.Println("taskmeta get,but trans bytes error",err)
		panic("taskmeta get,but trans bytes error")
	}
	err = json.Unmarshal(metaBytes,&sd.taskInfo)
	if err != nil{
		log.Println("taskmeta Unmarshal for taskInfo error ",err)
		panic("taskmeta Unmarshal for taskInfo error ")
	}
}


func (sd *Scheduler)Run(){
	log.Printf("taskInfo.Params is \n %s",sd.taskInfo.Params)
}