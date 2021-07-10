:# gosqltask

## gosqltask 介绍

gosqltask 是一个执行mysql任务的工具,实际工作当中使用的场景是数据开发或者数据分析的同学需要经常性的写一些sql脚本去取数据或者生成中间表再去加工数据,常规写脚本的方式可能是这样,以python为例

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



