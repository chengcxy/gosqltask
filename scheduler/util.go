package scheduler



import (
	"time"
)

var DayFormatter = "2006-01-02"

func GetDateFromToday(interval int) string {
	return time.Now().AddDate(0, 0, interval).Format(DayFormatter)
}