package task

import (
	"fmt"
	"github.com/pkg/errors"
	"sync"
)

type ParallelTaskError map[string]error

func (p ParallelTaskError) Error() string {
	msg := ""
	for key, err := range p {
		msg += fmt.Sprintf("%s: %s;", key, err)
	}
	return msg
}

// Task 单个任务, 指定任务的 key, 执行失败返回对应的错误
type Task func(key string) error

// Execute 执行一个并发任务，如果有错误则一定返回 ParallelTaskError
func Execute(keys []string, thread int, task Task) error {
	// 输入检查
	if len(keys) == 0 || task == nil || thread <= 0 {
		return nil
	}
	if thread > len(keys) {
		thread = len(keys)
	}
	task = safeTask(task)
	// 错误与 channel 的初始化
	errs := make(ParallelTaskError)
	errMu := sync.Mutex{}
	keyChan := make(chan string, thread)

	// key 生产者
	go func() {
		for _, key := range keys {
			keyChan <- key
		}
		close(keyChan)
	}()

	// key 消费者
	wg := sync.WaitGroup{}
	wg.Add(thread)
	for i := 0; i < thread; i++ {
		go func(t int) {
			for key := range keyChan {
				if err := task(key); err != nil {
					errMu.Lock()
					errs[key] = err
					errMu.Unlock()
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	if len(errs) == 0 {
		return nil
	}
	return errs
}

func safeTask(t Task) Task {
	return func(key string) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errors.Errorf("panic: %v", r)
			}
		}()
		return t(key)
	}
}
