package scheduler

import (
	"fmt"
	"github.com/chengcxy/gotools/backends"
	"github.com/chengcxy/gotools/configor"
	"github.com/chengcxy/gotools/roboter"
	zeronewp "github.com/chengcxy/gotools/workpool"
	"time"
)

var QueryResultTableExists = `
select table_name
from information_schema.tables
where table_schema="%s" and table_name="%s"
`

var UpdateTaskStatus = `
update task_def_sql_automation
set task_status=?
where id=?
`

type Scheduler struct {
	config *configor.Config // 配置
	taskId string			// 任务id
	startTime time.Time     // 程序运行开始时间
	robot roboter.Roboter   // 机器人
	taskInfo *TaskInfo		// 任务信息
	runQuery bool           // 是否执行数据库查询
	isCrossDbInstance bool 	// 是否跨越数据库实例
	IsExecutedPool bool     // 是否使用协程池 params非空的且含有worker_num
	IsUseTimeIncrease bool  // 是否含有日期增量
	isCreateTable bool
	taskPoolParams TaskPoolParams // 任务worker池params
	timeIncreaseParams map[string]string //增量时间params
	globalDbConfig *configor.Config // 全局数据库配置
	reader *backends.MysqlClient
	writer *backends.MysqlClient
	taskClient *backends.MysqlClient
	mr *MessageResult

}

var ExcelTimeEndFormat = "20060102150405"
var DayTimeSecondFormatter = "2006-01-02 15:04:05"
var GlobalDBConfigJsonFile = "db_global"

//excel下载
var UpdateExcelDownloadTaskStatus = `
update task_def_sql_automation_download 
set 
total=?,
write_count=?,
status=?
where uid=?
`
//导出状态 1开始导出 2导出成功 3导出失败
var InsertExcelDownloadTaskStatus = `
insert task_def_sql_automation_download(uid,status,owner,total)
values(?,"1",?,?)
`
var QueryExcelDownloadTaskStatus = `
select uid 
from task_def_sql_automation_download  
where status=?
`



type TaskInfo  struct {
	Id             string `json:"id"`              // 统计任务id
  	FromApp        string `json:"from_app"`        // 读取的业务系统
  	FromDbType     string `json:"from_db_type"`    // 读取的数据源类型mysql/oracle等
	FromDb         string `json:"from_db"`         // 读取的数据库
	StaticRule     string `json:"static_rule"`     // 统计规则
	Params         string `json:"params"`          // 增量规则
  	ToApp          string `json:"to_app"`          // 写入的业务系统
	ToDbType       string `json:"to_db_type"`      // 写入数据源类型
	ToDb           string `json:"to_db"`           // 写入数据库
	ToTable        string `json:"to_table"`        // 写入数据表
	OnlineStatus   string `json:"online_status"`   // 统计状态0统计1不统计
	WriteMode      string `json:"write_mode"`      // 写入模式replace/insert/update/delete/append/overwrite
	TaskDesc       string `json:"task_desc"`       // 统计描述
	IsTruncate     string `json:"is_truncate"`     // 是否truncate表(0-truncate)
	Owner          string `json:"owner"`           // 取数人
	TaskStatus     string `json:"task_status"`     // 任务状态
}



type TaskPoolParams struct {
	Table string `json:"table"`
	Pk string `json:"pk"`
	WorkerNum  int `json:"worker_num"`
	ReadBatch  int `json:"read_batch"`
	WriteBatch  int `json:"write_batch"`
}


var PrintLogTemplate = `
gosqltask任务id:%s,执行完毕
运行环境:%s
任务描述:%s
开始时间:%s
结束时间:%s
任务耗时:%s
任务状态:%s
影响的数据库行数:%d
`



type MessageResult struct {
	TaskId string
	Env string
	TaskDesc string
	StartTime string
	EndTime string
	Costs string
	Status string
	AffectNum int64
	ErrMsg string
	Flag int
}

func NewMessageResult(TaskId,Env,TaskDesc,StartTime,EndTime,Costs,Status,ErrMsg string,AffectNum int64,)*MessageResult{
	return &MessageResult{
		TaskId:TaskId,
		Env:Env,
		TaskDesc:TaskDesc,
		StartTime:StartTime,
		EndTime:EndTime,
		Costs:Costs,
		Status:Status,
		AffectNum:AffectNum,
		ErrMsg:ErrMsg,
	}
}



func(m *MessageResult)Message()string{
	if m.Status == "成功"{
		return fmt.Sprintf(PrintLogTemplate,
			m.TaskId,
			m.Env,
			m.TaskDesc,
			m.StartTime,
			m.EndTime,
			m.Costs,
			m.Status,
			m.AffectNum,
		)
	}else{
		return m.Error()
	}
}

var PrintFailedLogTemplate = `
gosqltask任务id:%s,执行完毕
运行环境:%s
任务描述:%s
开始时间:%s
任务状态:失败
原因:%s
`
func (m *MessageResult)Error()string{
	return fmt.Sprintf(PrintFailedLogTemplate,
		m.TaskId,
		m.Env,
		m.TaskDesc,
		m.StartTime,
		m.ErrMsg,
	)
}

type Job struct{
	Start int
	End int
	Sd *Scheduler
}

func (j *Job)Executed()interface{}{
	mr := &MessageResult{
		TaskId: j.Sd.taskId,
		Env:j.Sd.config.Env,
		TaskDesc:j.Sd.taskInfo.TaskDesc,
		StartTime:j.Sd.startTime.Format(DayTimeSecondFormatter),
		Status:"失败",
	}
	num,_,err := j.Sd.SingleThread(j.Start,j.End)
	mr.AffectNum = num
	if err != nil{
		mr.ErrMsg = fmt.Sprintf("执行区间(%d,%d]有异常:%v",j.Start,j.End,err)
		return mr
	}else{
		mr.Status = "成功"
	}
	return mr
}

type ExcelExportJob struct{
	Start int
	End int
	Query string
	Sd *Scheduler
	Wp *zeronewp.WorkerPool
}
type ExcelData struct{
	Data []string
	Error string
}
func (e *ExcelExportJob)Executed()interface{}{
	empty := make([]string,0)
	defaultExcelData := &ExcelData{
		Data:empty,
	}
	datas,fields,err := e.Sd.writer.Query(e.Query)
	if err != nil{
		defaultExcelData.Error = fmt.Sprintf("(%d,%s]export error,%v",e.Start,e.End,err)
		return defaultExcelData
	}
	for len(datas)>0{
		data := datas[0]
		datas = append(datas[:0],datas[1:]...)
		temp := make([]string,len(fields))
		for i,k := range fields{
			temp[i] = data[k]
		}
		r := &ExcelData{
			Data:temp,
		}
		e.Wp.ResultChan <- r
	}
	return defaultExcelData
}



