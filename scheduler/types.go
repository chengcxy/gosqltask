package scheduler

import (
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
	IsUseTimeIncrease bool  // 是否含有日期增量
	taskPoolParams TaskPoolParams // 任务worker池params
	timeIncreaseParams map[string]string //增量时间params
	globalDbConfig *configor.Config // 全局数据库配置
	reader *MysqlClient
	writer *MysqlClient

}



var GlobalDBConfigJsonFile = "db_global"

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
TaskId:%s
sql:%s,
params:pass

`

type Job struct{
	Start int
	End int
}

type Result struct {
	TaskId string // 任务id
	WorkerId int //worker
	Start int
	End int
	Num int64
	Status int
	
}
type WorkerPool struct{
	sd *Scheduler
	JobChan chan *Job
	ResultChan chan *Result
	CollectResult chan bool
}


