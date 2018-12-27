package congos

import (
	"context"
	"github.com/qiniu/log"
	"math/rand"
	"testing"
	"time"
)

func TestConGos(t *testing.T) {
	c := NewConGos(100, 10, func(task interface{}) {
		val := task.(int32)
		sl := rand.Int31n(4)
		time.Sleep(time.Duration(sl) * time.Second)
		log.Info("task", val, "sleep", sl)
	})
	ctx, cancelFn := context.WithCancel(context.Background())
	go c.Start(ctx)
	go func() {
		var i int32 = 0
		for {
			c.Append(i)
			i++
		}
	}()

	quit := time.NewTimer(20 * time.Second)
	<-quit.C
	cancelFn()
}