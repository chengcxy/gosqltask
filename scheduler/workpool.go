package scheduler


import(
	"sync"
	"log"
)

var wg sync.WaitGroup

func NewWorkerPool(sd *Scheduler)*WorkerPool{
	return &WorkerPool{
		sd:sd,
		JobChan:make(chan *Job),
		ResultChan:make(chan *Result),
		CollectResult:make(chan bool),
		
	}
}


func (p *WorkerPool)worker(workerId int){
	defer wg.Done()
	for job := range p.JobChan{
		start,end := job.Start,job.End
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
	}
}

func (p *WorkerPool)run(){
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
		for result := range p.ResultChan{
			log.Println(result)
		}
		p.CollectResult <- true
	}()
	<- p.CollectResult

}




