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
- 2.2 大表查询慢问题
```
params字段设置分割条件 json格式
{
  "split":{
    "table":"test.userinfo", //分割的表
    "pk":"id",               //根据哪个字段分割
    "worker_num":20,         //多少个worker执行
    "read_batch":10000,        //根据$pk的值多少一个区间读取 拼接 where条件 $pk>0 and $pk<=$read_batch
    "write_batch":300        //批量写入的值
  }
}

params字段设置完毕,static_rule 统计规则设置 举例:
test.userinfo表如果有6000万数据,想对每个id的区间有多少数据做一个统计,我们可能会这样写sql

select 
   case 
     when id>0 and id<=10000 then "(0,10000]"
     when id>10000 and id<=20000 then "(10000,20000]"
     ... else "[600000000,600010000]" end as section,count(1) as users
from test.userinfo
group by 
  case 
     when id>0 and id<=10000 then "(0,10000]"
     when id>10000 and id<=20000 then "(10000,20000]"
     ... else "[600000000,600010000]" end

实际执行上面这个sql的时候 由于数据量过大会很慢,这张表900多万数据运行了近20s
当按如下规则配置时由于命中了索引,执行会很快 
select $start as start,$end as end,users
from (
	  select count(1) as users
	  from $table 
    where $pk>$start and $pk<=$end 
) as a
 
我们约定 $start 代表切分键的起始值, $end 代表切分键的结束值,当params.split.read_batch=20000时,上面的执行sql被分成这样的计划执行,根据id主键按20000一个区间进行分批次读取
select 0 as start,20000 as end,num
from (
	select count(1) as num
	from test.userinfo
  where id>0 and id<=20000
) as a
...
...
select 20000 as start,40000 as end,num
from (
	select count(1) as num
	from test.userinfo
  where id>20000 and id<=40000
) as a

最终结果是运行了4s,性能提升近5倍

```
- 2.3 增量条件如何传递
```
如果时候我们需要跑一些增量统计,对增量表添加时间限制是最常用的办法,举例 每天的订单量/订单额

select order_date,count(order_id) as orders,sum(order_amount) as order_amount
from test.orders
where order_time >= "2021-07-13" and order_time < "2021-07-14"
group by order_date

我们可以将order_date>="2021-07-06" 这个条件 根据业务场景 配置到params
params字段 json格式 我们约定  $today 是获取北京时间的一个特殊变量 $today为今天,$today-n 就是今天往前推n天 $today+n就是今天往后推n天
{
  "time_increase":{
      "$1":"$today-1",
      "$2":"$today"
}
}

params字段设置完毕,static_rule 统计规则设置为:

select order_date,count(order_id) as orders,sum(order_amount) as order_amount
from test.orders
where order_time>="$1" and order_time < "$2"
group by order_date

程序会自动匹配$1 和$2的值去执行
```
- 2.4 任务执行状态？通知?
```
机器人报警支持了钉钉和企业微信,在dev.json配置roboter参数
```

- 2.5 自动建表功能
```
实际开发我们可能需要先创建表结构,然后再编写代码,gosqltask考虑到了这些经常做的问题,针对select语句可以自动获取schema,会默认在$to_db数据库创建$table表(如果已经建好表也不会报错),数据类型默认都是varchar(255),并且添加了默认主键和入库时间/更新时间字段,如果sql里select语句很长,可以先让程序建表,开发者对自动创建的表结构修改一下数据类型即可,
```

