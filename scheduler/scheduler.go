package scheduler


import (
	"fmt"
	"log"
	"encoding/json"
	"strings"
	"strconv"
    "github.com/chengcxy/gotools/configor"
	"github.com/chengcxy/gotools/roboter"
) 


type Scheduler struct {
	config *configor.Config // 配置
	taskId string			// 任务id
	robot roboter.Roboter   // 机器人
	taskInfo *TaskInfo		// 任务信息
	runQuery bool           // 是否执行数据库查询
	isCrossDbInstance bool 	// 是否跨越数据库实例
	IsExecutedPool bool     // 是否使用协程池 params非空的且含有worker_num
	taskPoolParams TaskPoolParams // 任务params
	globalDbConfig *configor.Config // 全局数据库配置
	reader *MysqlClient
	writer *MysqlClient

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
		log.Fatal("query taskmeta err")
	}
	if len(datas) != 1{
		log.Fatal("taskId not in taskTable")
	}
	meta := datas[0]//map[string]string
	metaBytes,err := json.Marshal(meta)
	if err != nil{
		log.Println("taskmeta get,but trans bytes error",err)
		log.Fatal("taskmeta get,but trans bytes error")
	}
	err = json.Unmarshal(metaBytes,&sd.taskInfo)
	if err != nil{
		log.Println("taskmeta Unmarshal for taskInfo error ",err)
		log.Fatal("taskmeta Unmarshal for taskInfo error ")
	}
}


func (sd *Scheduler)parseTask(){
	RuleLower := strings.ToLower(strings.TrimSpace(sd.taskInfo.StaticRule))
	log.Printf("clean static_rule :%s",RuleLower)
	sd.runQuery = true
	if strings.HasPrefix(RuleLower,"update")  || strings.HasPrefix(RuleLower,"insert")  || strings.HasPrefix(RuleLower,"delete") || strings.HasPrefix(RuleLower,"replace"){
		sd.runQuery = false
	}
	sd.isCrossDbInstance = true
	if strings.HasPrefix(RuleLower,"update")  || strings.HasPrefix(RuleLower,"insert")  || strings.HasPrefix(RuleLower,"delete") || strings.HasPrefix(RuleLower,"replace") || sd.taskInfo.FromApp == sd.taskInfo.ToApp{
		sd.isCrossDbInstance = false
	}
	sd.IsExecutedPool = false
	if sd.taskInfo.Params != "NULL" && strings.Contains(sd.taskInfo.Params,"worker_num"){
		sd.IsExecutedPool = true
	}
	if sd.IsExecutedPool{
		var f map[string]interface{}
		err := json.Unmarshal([]byte(sd.taskInfo.Params),&f)
		if err != nil{
			log.Fatal("task.params Json Unmarshal err")
		}
		mapBytes,err := json.Marshal(f["split"])
		if err != nil{
			log.Fatal("task.params.split Json Marshal err")
		}
		err = json.Unmarshal(mapBytes,&sd.taskPoolParams)
		if err != nil{
			log.Fatal("task.params.split bytes trans for sd.taskPoolParams err")
		}
		
	}
	sd.RenderSql()
	//获取全局数据库连接
	sd.globalDbConfig = configor.NewConfig(sd.config.ConfigPath,GlobalDBConfigJsonFile)
}


//from $table where $pk>$start and $pk<=$end and xx like "%ttt"
func (sd *Scheduler)RenderSql()string{
	query := strings.Replace(sd.taskInfo.StaticRule,`%`,`%%`,-1)
	if sd.IsExecutedPool{
		query = strings.Replace(query,`$table`,sd.taskPoolParams.Table,-1)
		query = strings.Replace(query,`$pk`,sd.taskPoolParams.Pk,-1)
	}
	sd.taskInfo.StaticRule = query
	return query
}


func (sd *Scheduler)SplitSql(start,end int)string{
	q := sd.taskInfo.StaticRule
	q = strings.Replace(q,"$start",strconv.Itoa(start),-1)
	q = strings.Replace(q,"$end",strconv.Itoa(end),-1)
	return q
}



func (sd *Scheduler)SingleThread()(int64,bool,int){
	//执行update/delete/insert/replace语句时 reader.Execute() 其他判断是否跨库执行
	if sd.runQuery{
		is_create_table := true
		datas,columns,err := sd.reader.Query(sd.taskInfo.StaticRule)
		if err != nil{
			log.Fatal(err)
		}
		if sd.isCrossDbInstance{
			return sd.writer.Write(sd.taskInfo.ToDb,sd.taskInfo.ToTable,columns,is_create_table,datas,sd.taskPoolParams.WriteBatch)
		}else{
			return sd.reader.Write(sd.taskInfo.ToDb,sd.taskInfo.ToTable,columns,is_create_table,datas,sd.taskPoolParams.WriteBatch)
		}
	}else{
		num,err := sd.reader.Execute(sd.taskInfo.StaticRule)
		if err != nil{
			log.Fatal(err)
		}
		return num,false,0
	}
}

func (sd *Scheduler)ThreadPool()string{
	return ""
}



func(sd *Scheduler)SubmitTask(debug bool){
	if debug{
		if strings.Contains(sd.taskInfo.StaticRule,"$start"){
			log.Printf("debugsql is:\n%s",sd.SplitSql(0,10000))
		}else{
			log.Printf("debugsql is:\n%s",sd.taskInfo.StaticRule)
		}
		return 
	}
	readerKey := fmt.Sprintf("from.mysql.%s_%s",sd.taskInfo.FromApp,sd.taskInfo.FromDb)
	sd.reader = NewMysqlClient(sd.globalDbConfig,readerKey)
	//跨越数据库实例 一般跨库执行的是select语句 
	if sd.isCrossDbInstance{
		writerKey := fmt.Sprintf("to.mysql.%s_%s",sd.taskInfo.ToApp,sd.taskInfo.ToDb)
		sd.writer = NewMysqlClient(sd.globalDbConfig,writerKey)	
	}
	//根据params参数判断是单线程执行还是多线程执行
	if !sd.IsExecutedPool{
		sd.SingleThread()
	}else{
		sd.ThreadPool()
	}

	

	// pool := NewWorkerPool(sd)
	// pool.run()

	

}

func (sd *Scheduler)Run(debug bool){
	log.Printf("taskInfo.Params is \n %s",sd.taskInfo.Params)
	sd.parseTask()
	sd.SubmitTask(debug)
}
