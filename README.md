## 一.gosqltask

gosqltask,使用go执行sql任务,面向数据开发/数据分析者的可配置化自动化工具。当执行的sql读取的是超千万数据量级别的大表时,可通过配置表的切分条件,一般是主键自增id或者业务id(整数)索引进行切分,按batch切分读取数据,利用go的通道和协程特性去快速执行读取写入任务。

gosqltask 适用于数据开发和数据分析人员以及经常写sql的同学,暂时支持的读写客户端限于mysql(由于spark官方还未支持go,经常写hivesql/sparksql的可以用python/scala语言实现)

开发这个工具的初衷是作者实际工作当中需要经常性的写一些sql脚本去取数据或者生成中间表再去加工数据,充斥着重复的逻辑开发。常规写脚本的方式可能是这样,下面以python开发需求作为案例


```python
QUERY  = "select a,b,c from db.table"

INSERT = "insert into *.*(a,b,c)values(?,?,?)"

class Scheduler(object):
      def __init__(self,config):
      	   self.config = config
      	   self.db = mysqlclient.from_settings(self.config)

      def read(self):
          datas = self.db.execute(QUERY)
          return datas

      def write(self,datas):
      	  batch = 1000
      	  values = []
      	  while datas:
              data = datas.pop()
              values.append(data)
              if len(values) == batch:
                 self.db.write(INSERT,values)
                 values.clear()
      	  if values:
              self.db.write(INSERT,values)
              values.clear()

      def process(self):
          datas = self.read()
          self.write(datas)
          self.db.close()

if __name__ == "__main__":
     config = {
       "host":"xx",
       "user":"xxx"
     }
     obj = Scheduler(config)
     obj.process()
```

那么当再来需求的时候,怎么办呢？将QUERY变量和INSERT变量的值换一下,重新建一个py脚本.
从上面的过程可以看出,每次变化的是读取的规则和写入的规则,处理流程也无非是这样:读到数据->处理数据->写入数据.
有没有什么办法可以只关注业务逻辑的开发


### 二.需要解决的问题

- 2.1 如何定义一个sql任务,如何管理sql任务？

``` go
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

这里使用mysql表来存储任务,后面对这个表可以写api,实现任务的创建/提交执行/导出数据
demo数据见data/sql_tasks.sql,这个表创建以后,dev.json里面taskmeta.conn需要修改为sql_tasks表所在的数据库信息
gosqltask虽然暂时只支持mysql2mysql的sql任务,这个表2个字段FromDbType和ToDbType 后面可以使用其他语言进行拓展,作者之前经常写spark任务,使用pyspark做成了配置的工具
```
- 2.2 大表查询慢问题/增量条件如何传递？

```
```
- 2.3 任务执行状态？通知?




