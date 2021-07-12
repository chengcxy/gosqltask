package scheduler





var GlobalDBConfigJsonFile = "db_global"

type TaskInfo  struct {
	Id             string `json:"id"`              // 统计任务id
	FromApp        string `json:"from_app"`        // 读取的业务系统
	FromDb         string `json:"from_db"`         // 读取的数据库
	StaticRule     string `json:"static_rule"`     // 统计规则
	Params         string `json:"params"`          // 增量规则
	ToDbType       string `json:"to_db_type"`      // 写入数据源类型
	ToApp          string `json:"to_app"`          // 写入的业务系统
	ToDb           string `json:"to_db"`           // 写入数据库
	ToTable        string `json:"to_table"`        // 写入数据表
	OnlineStatus   string `json:"online_status"`   // 统计状态0统计1不统计
	WriteMode      string `json:"write_mode"`      // 写入模式replace/insert/append/overwrite
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


