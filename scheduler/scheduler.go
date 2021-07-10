package scheduler


import (
	"log"
	"encoding/json"
	"github.com/chengcxy/gotools/backends"
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
	McInterface, ok := sd.config.Get("taskmeta")
	if !ok {
		panic("taskmeta key not in json_file")
	}
	conf := McInterface.(map[string]interface{})
	taskDbConfig := conf["conn"]
	QueryTaskSql := conf["query_task"].(string)
	log.Println("QueryTaskSql is ",QueryTaskSql)
	mc := backends.NewMysqlConfig(taskDbConfig)
	mysql := backends.NewMysqlClient(mc)
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
	log.Printf("sd.taskInfo.Params is \n %s",sd.taskInfo.Params)
	
}