package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chengcxy/gotools/backends"
	"github.com/chengcxy/gotools/configor"
	"github.com/chengcxy/gotools/roboter"
	zeronewp "github.com/chengcxy/gotools/workpool"
	"github.com/xuri/excelize/v2"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

func init(){
	log.SetFlags(log.Llongfile|log.Ltime)
}
func NewScheduler(config *configor.Config,taskId string,startTime time.Time) *Scheduler{
	sd := &Scheduler{
		config:config,
		taskId:taskId,
		startTime:startTime,
		robot:roboter.GetRoboter(config),
	}
	sd.mr = &MessageResult{
		TaskId: sd.taskId,
		Env:config.Env,
		StartTime:sd.startTime.Format(DayTimeSecondFormatter),
		Status:"init 失败",
	}
	return sd
}



func (sd *Scheduler)getTimeValue(v string) (string,error){
	if strings.TrimSpace(v) == "$today"{
		return GetDateFromToday(0),nil
	}else if strings.Contains(v,"$today-"){
		num := strings.TrimSpace(strings.Replace(v,"$today-","",-1))
		n,err := strconv.Atoi(num)
		if err != nil{
			return "",errors.New(fmt.Sprintf("getTimeValue $today- for int error:%v",err))
		}
		return GetDateFromToday(-n),nil
	}else{
		num := strings.TrimSpace(strings.Replace(v,"$today+","",-1))
		n,err := strconv.Atoi(num)
		if err != nil{
			return "",errors.New(fmt.Sprintf("getTimeValue $today+ for int error:%v",err))
		}
		return GetDateFromToday(n),nil
	}
}

func (sd *Scheduler)parseTimeIncrease()error{
	if sd.IsUseTimeIncrease{
		m := make(map[string]string)
		for k,v := range sd.timeIncreaseParams{
			v,err := sd.getTimeValue(v)
			if err != nil{
				return err
			}
			m[k] = v
		}
		sd.timeIncreaseParams = m
	}
	return nil
}

func (sd *Scheduler)GetTaskInfo()error{
	qt,ok := sd.config.Get("taskmeta.query_task")
	if !ok{
		return errors.New("config.taskmeta.query_task not exists")
	}
	QueryTaskSql := qt.(string)
	log.Println("QueryTaskSql is ",QueryTaskSql)
	mysql,err := backends.NewMysqlClient(sd.config,"taskmeta.conn")
	if err != nil{
		return err
	}
	sd.taskClient = mysql
	datas,_, err := sd.taskClient.Query(QueryTaskSql,sd.taskId)
	if err != nil{
		log.Println("query taskmeta err",err)
		return errors.New(fmt.Sprintf("taskmeta get error:%v",err))
	}
	if len(datas) != 1{
		log.Println("datas is sd.taskId,",datas,"id=",sd.taskId)
		return errors.New(fmt.Sprintf("task id not exists or online_status=0 or len(datas)!=1: is %d",len(datas)))
	}
	meta := datas[0]//map[string]string
	log.Println("ok! query taskmeta mysql client closed")
	metaBytes,err := json.Marshal(meta)
	if err != nil{
		log.Println("taskmeta get,but trans bytes error",err)
		return errors.New(fmt.Sprintf("taskmeta success get,but trans bytes error:%v",err))
	}
	err = json.Unmarshal(metaBytes,&sd.taskInfo)
	if err != nil{
		log.Println("taskmeta Unmarshal for taskInfo error ",err)
		return errors.New(fmt.Sprintf("taskmeta Unmarshal for taskInfo error:%v",err))
	}
	return nil
}


func (sd *Scheduler)parseTask()error{
	RuleLower := strings.ToLower(strings.TrimSpace(sd.taskInfo.StaticRule))
	log.Printf("clean static_rule :%s",RuleLower)
	sd.isCreateTable = true
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
			return errors.New(fmt.Sprintf("task.params Json Unmarshal error:%v",err))
		}
		if strings.Contains(sd.taskInfo.Params,"time_increase"){
			sd.IsUseTimeIncrease = true
			mapBytes,err := json.Marshal(f["time_increase"])
			if err != nil{
				return errors.New(fmt.Sprintf("task.params.time_increase Json Marshal error:%v",err))
			}
			err = json.Unmarshal(mapBytes,&sd.timeIncreaseParams)
			if err != nil{
				return errors.New(fmt.Sprintf("task.params.time_increase bytes trans for sd.timeIncreaseParams err:%v",err))
			}
			log.Println(sd.timeIncreaseParams)
			err = sd.parseTimeIncrease()
			if err != nil{
				return err
			}

		}
	   	if strings.Contains(sd.taskInfo.Params,"worker_num"){
			sd.IsExecutedPool = true
			mapBytes,err := json.Marshal(f["split"])
			if err != nil{
				return errors.New(fmt.Sprintf("task.params.split Json Marshal  error:%v",err))
			}
			err = json.Unmarshal(mapBytes,&sd.taskPoolParams)
			if err != nil{
				return errors.New(fmt.Sprintf("task.params.split bytes trans for sd.taskPoolParams error:%v",err))
			}

	  }
	}
	if sd.isCrossDbInstance{
		if sd.taskInfo.ToDb == "NULL" || sd.taskInfo.ToDb == ""  || sd.taskInfo.ToTable == "NULL" || sd.taskInfo.ToTable == ""{
			return errors.New("taskInfo.ToDb or taskInfo.ToTable is null or empty ")
		}
	}
	sd.RenderSql()
	jsonFile := path.Join(sd.config.ConfigPath,GlobalDBConfigJsonFile+ ".json")
	_,err := os.Stat(jsonFile)
	if os.IsNotExist(err){
		return errors.New(fmt.Sprintf("%s is not exists",jsonFile))
	}
	sd.mr.TaskDesc = sd.taskInfo.TaskDesc
	//获取全局数据库连接
	sd.globalDbConfig = configor.NewConfig(sd.config.ConfigPath,GlobalDBConfigJsonFile,sd.config.UsedEnv)
	readerKey := fmt.Sprintf("from.mysql.%s_%s",sd.taskInfo.FromApp,sd.taskInfo.FromDb)
	reader,err := backends.NewMysqlClient(sd.globalDbConfig,readerKey)
	if err != nil{
		sd.mr.ErrMsg = fmt.Sprintf("获取reader对象失败:%v",err)
		return errors.New(fmt.Sprintf("获取reader对象失败:%v",err))
	}
	sd.reader = reader
	if sd.isCrossDbInstance{
		writerKey := fmt.Sprintf("to.mysql.%s_%s",sd.taskInfo.ToApp,sd.taskInfo.ToDb)
		writer,err := backends.NewMysqlClient(sd.globalDbConfig,writerKey)
		if err != nil{
			sd.mr.ErrMsg = fmt.Sprintf("获取writer对象失败:%v",err)
			return errors.New(fmt.Sprintf("获取writer对象失败:%v",err))
		}
		sd.writer = writer
	}
	return nil
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
	if len(strconv.Itoa(MaxId)) >= 16{
		ls := make([]int,2)
		ls[0] = MinId
		ls[1] = 100012000000
		start_ends = append(start_ends,ls)
		ls2 := make([]int,2)
		ls2[0] = 1000100000000000
		ls2[1] = MaxId
		start_ends= append(start_ends,ls2)
		return start_ends
	}else{
		ls := make([]int,2)
		ls[0] = MinId
		ls[1] = MaxId 
		start_ends = append(start_ends,ls)
		return start_ends
	}
}
func (sd *Scheduler)GenJobs(Jobchan chan zeronewp.JobInterface){
	defer func(){
		close(Jobchan)
	}()
	db := strings.Split(sd.taskPoolParams.Table, ".")[0]
	table := strings.Split(sd.taskPoolParams.Table, ".")[1]
	MinId,err := sd.reader.GetMinId(db,table,strings.ToLower(sd.taskPoolParams.Pk))
	if err != nil{
		return
	}
	MaxId,err := sd.reader.GetMaxId(db,table,strings.ToLower(sd.taskPoolParams.Pk))
	if err != nil{
		return
	}
	start_ends := sd.GetStartEnds(MinId,MaxId)
	for _,ls := range start_ends{
		start,end := ls[0],ls[1]
		for start < end {
			_end := start + sd.taskPoolParams.ReadBatch
			if _end > end {
				_end = end
			}
			p := NewJob(start,_end,sd)
			Jobchan <- p
			start = _end
		}
	}
	log.Println("producer job params finished ")

}

func (sd *Scheduler)SplitSql(start,end int)string{
	q := sd.taskInfo.StaticRule
	q = strings.Replace(q,"$start",strconv.Itoa(start),-1)
	q = strings.Replace(q,"$end",strconv.Itoa(end),-1)
	return q
}





func (sd *Scheduler)SingleThread(start,end int)(int64,bool,error){
	//执行update/delete/insert/replace语句时 reader.Execute() 其他判断是否跨库执行
	stam := sd.SplitSql(start,end)
	if sd.runQuery{
		datas,columns,err := sd.reader.Query(stam)
		if err != nil{
			return 0,false,err
		}
		if sd.isCrossDbInstance{
			num,isCreateTable,err := sd.writer.Write(sd.taskInfo.WriteMode,sd.taskInfo.ToDb,sd.taskInfo.ToTable,datas,columns,sd.taskPoolParams.WriteBatch,sd.isCreateTable)
			if sd.isCreateTable != isCreateTable{
				sd.isCreateTable = isCreateTable
			}
			return num,isCreateTable,err
		}else{
			num,isCreateTable,err := sd.reader.Write(sd.taskInfo.WriteMode,sd.taskInfo.ToDb,sd.taskInfo.ToTable,datas,columns,sd.taskPoolParams.WriteBatch,sd.isCreateTable)
			if sd.isCreateTable != isCreateTable{
				sd.isCreateTable = isCreateTable
			}
			return num,isCreateTable,err
		}
	}else{
		num,err := sd.reader.Execute(stam)
		if err != nil{
			return num,true,errors.New(fmt.Sprintf("executed sql:%s error:%v",stam,err))
		}
		return num,false,nil
	}
}

func NewJob(start,end int,sd *Scheduler)zeronewp.JobInterface{
	return &Job{
		Start:start,
		End:end,
		Sd:sd,
	}
}



func (sd *Scheduler)ThreadPool()(string,string){
	wp := zeronewp.NewWorkerPool(sd.taskPoolParams.WorkerNum)
	wp.Run(sd.GenJobs)
	for result := range wp.ResultChan{
		mr := result.(*MessageResult)
		if mr.Status == "失败"{
			sd.mr.Status = "失败"
			sd.mr.ErrMsg = mr.ErrMsg
			return mr.Status,mr.ErrMsg
		}else{
			sd.mr.AffectNum += mr.AffectNum
		}
	}
	return "成功",""
}

func (sd *Scheduler) WriteTableExist()bool{
	querySql := fmt.Sprintf(QueryResultTableExists,sd.taskInfo.ToDb,sd.taskInfo.ToTable)
	datas,_,_ := sd.writer.Query(querySql)
	if len(datas) == 1{
		return true
	}else{
		log.Println("result table:",sd.taskInfo.ToDb,sd.taskInfo.ToTable,"not exists next will be created auto")
		return false
	}

}

func(sd *Scheduler)SubmitTask(debug bool) *MessageResult{
	log.Println("SubmitTask debug:",debug)
	if debug {
		sd.mr.Status = "成功"
		return sd.mr
	}
	var err error
	//更新任务状态 为runing
	sd.UpdateTaskStatus("5")
	//跨越数据库实例 一般跨库执行的是select语句 
	if sd.isCrossDbInstance{
		if sd.taskInfo.IsTruncate == "1"{
			writeTableExist := sd.WriteTableExist()
			if writeTableExist{
				truncateTbale := fmt.Sprintf("truncate table %s.%s",sd.taskInfo.ToDb,sd.taskInfo.ToTable)
				_,err := sd.writer.Execute(truncateTbale)
				if err != nil{
					sd.mr.ErrMsg = fmt.Sprintf("truncate table error:%v",err)
					return sd.mr
				}else {
					log.Printf("%s success", truncateTbale)
				}
			}

		}
	}
	
	//根据params参数判断是单线程执行还是多线程执行
	var num int64
	if !sd.IsExecutedPool{
		num,_,err = sd.SingleThread(0,0)
		sd.mr.AffectNum = num
		if err != nil{
			sd.mr.Status = "失败"
			sd.mr.ErrMsg = fmt.Sprintf("失败原因:%v",err)
		}else{
			sd.mr.Status = "成功"
		}
	}else{
		poolStatus,poolError := sd.ThreadPool()
		sd.mr.Status = poolStatus
		sd.mr.ErrMsg = poolError
	}
	endTime := time.Now() 
	Costs := fmt.Sprintf("%s",endTime.Sub(sd.startTime))
	sd.mr.EndTime = endTime.Format(DayTimeSecondFormatter)
	sd.mr.Costs = Costs
	if sd.mr.Status != "成功"{
		//更新任务状态 为2=任务运行失败
		sd.UpdateTaskStatus("2")
	}else{
		//更新任务状态 为1=任务运行成功
		sd.UpdateTaskStatus("1")
	}
	if err != nil{
		sd.mr.Status = "失败"
		sd.mr.ErrMsg = "更新任务最后状态失败"
	}
	sd.robot.SendMsg(sd.mr.Message(),sd.taskInfo.Owner)
	return sd.mr
}

func (sd *Scheduler)UpdateTaskStatus(status string){
	sd.taskClient.Execute(UpdateTaskStatus,status,sd.taskId)
}

func (sd *Scheduler)Close(debug bool){

	sd.taskClient.Close()
	if sd.reader != nil {
		sd.reader.Close()
	}
	if !debug{
		if sd.isCrossDbInstance{
			if sd.writer != nil{
				sd.writer.Close()
			}
		}
		log.Printf("close reader and writer if writer is not null")
	}
	
}
func (sd *Scheduler)Run(debug bool,cmd,dir,fid string)interface{}{
	defer func(debug bool){
		sd.Close(debug)
	}(debug)
	err := sd.ValidTask()
	if err != nil{
		//更新任务状态 为参数校验失败
		sd.UpdateTaskStatus("4")
		sd.mr.ErrMsg =  fmt.Sprintf("任务未通过校验,原因:%v",err)
		return sd.mr
	}
	sd.UpdateTaskStatus("3")
	if cmd == "execute"{
		return sd.SubmitTask(debug)
	}else{
		return sd.ExportExcel(debug,dir,fid)
	}

}

//导出状态 1开始导出 2导出中 3导出成功 4导出失败
func (sd *Scheduler)ExportExcel(debug bool,dir,fid string) *MessageResult{
	log.Println("ExportExcel dir,fid is :",dir,fid)
	now := time.Now().Format(ExcelTimeEndFormat)
	var uid string
	if fid == ""{
		uid = fmt.Sprintf("%s_%s",sd.taskId,now)
	}else{
		uid = fmt.Sprintf("%s_%s",sd.taskId,fid)
	}
	excelName := fmt.Sprintf("%s.xlsx",uid)
	if dir == ""{
		dir = "../data/"
	}
	ExcelFullName := path.Join(dir,excelName)
	if debug {
		sd.mr.Status = "成功"
		log.Println("ExcelFullName is ",ExcelFullName)
		return sd.mr
	}
	err := sd.ThreadPoolExport(ExcelFullName,uid)
	if err != nil{
		sd.UpdateTaskStatus("6")// 任务表的状态导出失败
		sd.mr.ErrMsg =  fmt.Sprintf("导出excel失败,原因:%v",err)
		return sd.mr
	}
	sd.mr.Status = "成功"
	sd.UpdateTaskStatus("7")//任务表状态导出成功
	return sd.mr
}
//导出状态 1开始导出 2导出中 3导出成功 4导出失败
func  (sd *Scheduler)ThreadPoolExport(ExcelFullName,uid string)error{
	//插入下载表记录
	_,err := sd.taskClient.Execute(InsertExcelDownloadTaskStatus,uid,sd.taskInfo.Owner,0)
	if err != nil{
		return errors.New(fmt.Sprintf("下载表初始化状态失败,err:%v",err))
	}

	toDb,toTable := sd.taskInfo.ToDb,sd.taskInfo.ToTable
	tm,err := sd.writer.GetTableMeta("export",toDb,toTable)
	if err != nil{
		return errors.New(fmt.Sprintf("%s.%s获取元数据失败",toDb,toTable))
	}
	pk := tm.Pk
	total,err := sd.writer.GetTotalCount(toDb,toTable)
	if err != nil{
		return errors.New(fmt.Sprintf("%s.%s表获取数据量失败",toDb,toTable))
	}

	if total > 1040000{
		sd.taskClient.Execute(UpdateExcelDownloadTaskStatus,total,0,"4",uid)
		return errors.New(fmt.Sprintf("%s.%s表数据量>104万无法导出",toDb,toTable))
	}
	BaseQueryBatchRule := fmt.Sprintf("select * from %s.%s where %s>%s and %s<=%s",toDb,toTable,pk,"%d",pk,"%d")
	wp := zeronewp.NewWorkerPool(20)
	FuncProducerJob := func(JobChan chan zeronewp.JobInterface){
		defer func(){
			close(JobChan)
		}()
		start,_ := sd.writer.GetMinId(toDb,toTable,pk)
		end,_ := sd.writer.GetMaxId(toDb,toTable,pk)
		read_batch := 1000
		for start < end{
			_end := start + read_batch
			if _end > end{
				_end = end
			}
			ej := &ExcelExportJob{
				Start: start,
				End:_end,
				Query: fmt.Sprintf(BaseQueryBatchRule,start,_end),
				Sd:sd,
				Wp:wp,
			}
			JobChan <- ej
			start = _end
		}

	}
	wp.Run(FuncProducerJob)
	xlsx := excelize.NewFile()
	fields := tm.Fields
	excelFields := make(map[int]string)
	pos := 1
	for i := 'A'; i <= 'Z'; i++ {
		t := fmt.Sprintf("%c", i)
		excelFields[pos] = t
		pos++
	}
	for index,col := range fields{
		position := fmt.Sprintf("%s1",excelFields[index+1])
		xlsx.SetCellValue("Sheet1", position,col)
	}

	//第二行开始写 写入A2 B2 C2
	line := 2
	var exportStatus string
	exportStatus = "3"
	runingStatus := "2"
	for result := range wp.ResultChan{
		r := result.(*ExcelData)
		data := r.Data
		if r.Error != ""{
			exportStatus = "4"
		}
		if len(data) >0{
			for j:=0;j<len(fields);j++{
				position := fmt.Sprintf("%s%d",excelFields[j+1],line)
				if data[j] == "NULL"{
					xlsx.SetCellValue("Sheet1", position,nil)
				}else{
					xlsx.SetCellValue("Sheet1", position,data[j])
				}
			}
			if (line-1) % 1000 ==0 {
				log.Println("导出了",line-1,"条")
				sd.taskClient.Execute(UpdateExcelDownloadTaskStatus,total,line-2,runingStatus,uid)
			}
			r = nil
			data = nil
			line ++
		}

	}
	if err := xlsx.SaveAs(ExcelFullName); err != nil {
		log.Println("export excel task failed filename is:\n",ExcelFullName)
	}
	sd.taskClient.Execute(UpdateExcelDownloadTaskStatus,total,line-2,exportStatus,uid)
	log.Println("export excel task success filename is:\n",ExcelFullName)
	return nil
}

func (sd *Scheduler)ValidTask()error{
	err := sd.GetTaskInfo()
	if err != nil{
		return err
	}
	err = sd.parseTask()
	if err != nil{
		return err
	}
	sd.mr.TaskDesc = sd.taskInfo.TaskDesc
	log.Printf("taskInfo.Params is \n %s",sd.taskInfo.Params)
	err = sd.DebugSqlIsError()
	if err != nil{
		log.Println("explain sql error:",err)
		return err
	}
	return nil
}

func (sd *Scheduler)GetDebugSql()string{
	var debugSql string
	if strings.Contains(sd.taskInfo.StaticRule,"$start"){
		debugSql = strings.ReplaceAll(sd.SplitSql(0,10000),"%%","%")
	}else{
		debugSql = strings.ReplaceAll(sd.taskInfo.StaticRule,"%%","%")
	}
	log.Printf("debugsql is:\n%s",debugSql)
	return debugSql
}

func (sd *Scheduler)DebugSqlIsError()error{
	debugSql := sd.GetDebugSql()
	log.Println(fmt.Sprintf("sql执行计划:explain %s",debugSql))
	_,_,err := sd.reader.Query(fmt.Sprintf("explain %s",debugSql))
	return err
}
