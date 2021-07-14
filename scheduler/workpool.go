package scheduler


import(
	"sync"
	"log"
)

var wg sync.WaitGroup

//var mutex sync.Mutex

func NewWorkerPool(sd *Scheduler)*WorkerPool{
	return &WorkerPool{
		sd:sd,
		JobChan:make(chan *Job),
		ResultChan:make(chan *Result),
		CollectResult:make(chan *LastResult),
		
	}
}


func (p *WorkerPool)worker(workerId int){
	defer wg.Done()
	for job := range p.JobChan{
		start,end := job.Start,job.End
		//mutex.Lock()
		num,_,status := p.sd.SingleThread(start,end)
		result := &Result{
			TaskId:p.sd.taskId,
			WorkerId:workerId,
			Start:start,
			End:end,
			Num:num,
			Status:status,
		}
		p.ResultChan <- result
		//mutex.Unlock()
	}
}

func (p *WorkerPool)run()(int64,bool,int){
	//worker
	for i:=1;i<= p.sd.taskPoolParams.WorkerNum;i++{
		wg.Add(1)
		go p.worker(i)
	}
	//发布任务
	go p.sd.GenJobs(p.JobChan)
	go func(){
		wg.Wait()
		close(p.ResultChan)
	}()
	go func(){
		var n int64
		status := 0
		for result := range p.ResultChan{
			log.Println(result)
			n += result.Num
			if result.Status == 1{
				status = 1
			}
		}
		lr := &LastResult{
			Num:n,
			Status:status,
		}
		p.CollectResult <- lr
	}()
	lr := <- p.CollectResult
	return lr.Num,false,lr.Status
}




