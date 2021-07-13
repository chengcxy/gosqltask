## gosqltask 介绍

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



