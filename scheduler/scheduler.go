package scheduler


import (
	"fmt"
	"log"
	"time"
	"encoding/json"
	"strings"
	"strconv"
    "github.com/chengcxy/gotools/configor"
	"github.com/chengcxy/gotools/roboter"
) 




func NewScheduler(config *configor.Config,taskId string,startTime time.Time) *Scheduler{
	sd := &Scheduler{
		config:config,
		taskId:taskId,
		startTime:startTime,
		robot:roboter.GetRoboter(config),
	}
	sd.GetTaskInfo()
	return sd
}



func (sd *Scheduler)getTimeValue(v string) string{
	if strings.TrimSpace(v) == "$today"{
		return GetDateFromToday(0)
	}else if strings.Contains(v,"$today-"){
		num := strings.TrimSpace(strings.Replace(v,"$today-","",-1))
		n,err := strconv.Atoi(num)
		if err != nil{
			log.Fatal(err)
		}
		return GetDateFromToday(-n)
	}else{
		num := strings.TrimSpace(strings.Replace(v,"$today+","",-1))
		n,err := strconv.Atoi(num)
		if err != nil{
			log.Fatal(err)
		}
		return GetDateFromToday(n)
	}
}

func (sd *Scheduler)parseTimeIncrease(){
	if sd.IsUseTimeIncrease{
		m := make(map[string]string)
		for k,v := range sd.timeIncreaseParams{
			m[k] = sd.getTimeValue(v)
		}
		sd.timeIncreaseParams = m
	}
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
	if strings.HasPrefix(RuleLower,"update")  || strings.HasPrefix(RuleLower,"insert")  || strings.HasPrefix(RuleLower,"delete") || strings.HasPrefix(RuleLower,"replace"){
		sd.isCrossDbInstance = false
	}
	sd.IsExecutedPool = false
	sd.IsUseTimeIncrease = false
	if sd.taskInfo.Params != "NULL"{
		var f map[string]interface{}
		err := json.Unmarshal([]byte(sd.taskInfo.Params),&f)
		if err != nil{
			log.Fatal("task.params Json Unmarshal err")
		}
		if strings.Contains(sd.taskInfo.Params,"time_increase"){
			sd.IsUseTimeIncrease = true
			mapBytes,err := json.Marshal(f["time_increase"])
			if err != nil{
				log.Fatal("task.params.time_increase Json Marshal err")
			}
			err = json.Unmarshal(mapBytes,&sd.timeIncreaseParams)
			if err != nil{
				log.Fatal("task.params.time_increase bytes trans for sd.timeIncreaseParams err")
			}
			log.Println(sd.timeIncreaseParams)
			sd.parseTimeIncrease()

		}
	   	if strings.Contains(sd.taskInfo.Params,"worker_num"){
			sd.IsExecutedPool = true
			mapBytes,err := json.Marshal(f["split"])
			if err != nil{
				log.Fatal("task.params.split Json Marshal err")
			}
			err = json.Unmarshal(mapBytes,&sd.taskPoolParams)
			if err != nil{
				log.Fatal("task.params.split bytes trans for sd.taskPoolParams err")
			}

	  }
	}
	if sd.isCrossDbInstance{
		if sd.taskInfo.ToDb == "NULL" || sd.taskInfo.ToDb == ""  || sd.taskInfo.ToTable == "NULL" || sd.taskInfo.ToTable == ""{
			log.Fatal("taskInfo.ToDb or taskInfo.ToTable is null or empty")
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
	if sd.IsUseTimeIncrease{
		for k,v := range sd.timeIncreaseParams{
			query = strings.Replace(query,k,v,-1)
		}
	}
	sd.taskInfo.StaticRule = query
	return query
}

//切分后续需使用算法切分 针对数据倾斜的做处理
func (sd *Scheduler) GetStartEnds(MinId,MaxId int)([][]int){
	start_ends := make([][]int,0)
	if strings.ToLower(sd.taskPoolParams.Pk) == "id" || strings.ToLower(sd.taskPoolParams.Pk) == "entid"{
		ls := make([]int,2)
		ls[0] = MinId
		ls[1] = MaxId 
		start_ends = append(start_ends,ls)
		return start_ends
	}
	ls := make([]int,2)
	ls[0] = MinId
	ls[1] = 100012000000 
	start_ends = append(start_ends,ls)
	ls2 := make([]int,2)
	ls2[0] = 1000100000000000
	ls2[1] = MaxId
	start_ends= append(start_ends,ls2)
	return start_ends
}
func (sd *Scheduler)GenJobs(Jobchan chan *Job){
	db := strings.Split(sd.taskPoolParams.Table, ".")[0]
	table := strings.Split(sd.taskPoolParams.Table, ".")[1]
	MinId := sd.reader.GetMinId(db,table,strings.ToLower(sd.taskPoolParams.Pk))
	MaxId := sd.reader.GetMaxId(db,table,strings.ToLower(sd.taskPoolParams.Pk))
	start_ends := sd.GetStartEnds(MinId,MaxId)
	for _,ls := range start_ends{
		start,end := ls[0],ls[1]
		log.Println("start,end ",start,end)
		for start < end {
			_end := start + sd.taskPoolParams.ReadBatch
			if _end > end {
				_end = end
			}
			p := &Job{
				Start: start,
				End:   _end,
			}
			Jobchan <- p
			log.Println("producer job params ", p)
			start = _end
		}
	}
	log.Println("producer job params finished ")
	close(Jobchan)
}

func (sd *Scheduler)SplitSql(start,end int)string{
	q := sd.taskInfo.StaticRule
	q = strings.Replace(q,"$start",strconv.Itoa(start),-1)
	q = strings.Replace(q,"$end",strconv.Itoa(end),-1)
	return q
}



func (sd *Scheduler)SingleThread(start,end int)(int64,bool,int){
	//执行update/delete/insert/replace语句时 reader.Execute() 其他判断是否跨库执行
	stam := sd.SplitSql(start,end )
	if sd.runQuery{
		is_create_table := true
		datas,columns,err := sd.reader.Query(stam)
		if err != nil{
			log.Fatal(err)
		}
		if sd.isCrossDbInstance{
			return sd.writer.Write(sd.taskInfo.WriteMode,sd.taskInfo.ToDb,sd.taskInfo.ToTable,datas,columns,sd.taskPoolParams.WriteBatch,is_create_table)
		}else{
			return sd.reader.Write(sd.taskInfo.WriteMode,sd.taskInfo.ToDb,sd.taskInfo.ToTable,datas,columns,sd.taskPoolParams.WriteBatch,is_create_table)
		}
	}else{
		num,err := sd.reader.Execute(stam)
		if err != nil{
			log.Fatal(err)
		}
		return num,false,0
	}
}

func (sd *Scheduler)ThreadPool()(int64,bool,int){
	p := NewWorkerPool(sd)
	return p.run()
}

func(sd *Scheduler)SubmitTask(debug bool){
	defer func(debug bool){
		sd.Close(debug)
	}(debug)
	if debug {
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
		if sd.taskInfo.IsTruncate == "0"{
			truncateTbale := fmt.Sprintf("truncate table %s.%s",sd.taskInfo.ToDb,sd.taskInfo.ToTable)
			_,err := sd.writer.Execute(truncateTbale)
			if err != nil{
				log.Fatal("truncate table error")
			}else{
				log.Printf("%s success",truncateTbale)
			}
		}
	}
	
	//根据params参数判断是单线程执行还是多线程执行
	var num int64 
	status := 0
	if !sd.IsExecutedPool{
		num,_,status = sd.SingleThread(0,0)
	}else{
		num,_,status = sd.ThreadPool()
	}
	endTime := time.Now() 
	Costs := endTime.Sub(sd.startTime)
	strStatus := "成功"
	if status == 1{
		strStatus = "失败"
	}
	
	printLog := fmt.Sprintf(PrintLogTemplate,
		sd.taskId,
		sd.taskInfo.TaskDesc,
		sd.startTime.Format(DayTimeSecondFormatter),
		endTime.Format(DayTimeSecondFormatter),
		Costs,
		strStatus,
		num)
	log.Println(printLog)
	sd.robot.SendMsg(printLog,sd.taskInfo.Owner)
	
	
}
func (sd *Scheduler)Close(debug bool){
	if !debug{
		sd.reader.Close()
		if sd.isCrossDbInstance{
			sd.writer.Close()
		}
		log.Printf("close reader and writer if writer is not null")
	}
	
}
func (sd *Scheduler)Run(debug bool){
	log.Printf("taskInfo.Params is \n %s",sd.taskInfo.Params)
	sd.parseTask()
	sd.SubmitTask(debug)
	
}
