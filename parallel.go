package task

import (
	"fmt"
	"github.com/pkg/errors"
	"runtime/debug"
	"sync"
)

type ParallelTaskError[Key comparable] map[Key]error

func (p ParallelTaskError[Key]) Error() string {
	msg := ""
	for key, err := range p {
		msg += fmt.Sprintf("%v: %s;", key, err)
	}
	return msg
}

// Task 单个任务, 指定任务的 key, 执行失败返回对应的错误
type Task[Key comparable] func(key Key) error

// TaskFunc 单个任务, 指定任务的 ID 与 key, 执行失败返回对应的错误
type TaskFunc[Key comparable] func(id int, key Key) error

// Execute 执行一个并发任务，如果有错误则一定返回 ParallelTaskError
func Execute[Key comparable](keys []Key, thread int, task Task[Key]) error {
	return ParallelExecute(keys, thread, func(id int, key Key) error { return task(key) })
}

func ParallelExecute[Key comparable](keys []Key, thread int, task TaskFunc[Key]) error {
	// 输入检查
	if len(keys) == 0 || task == nil || thread <= 0 {
		return nil
	}
	if thread > len(keys) {
		thread = len(keys)
	}
	task = safeTask(task)
	// 错误与 channel 的初始化
	errs := make(ParallelTaskError[Key])
	errMu := sync.Mutex{}
	keyChan := make(chan Key, thread)

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
				if err := task(t, key); err != nil {
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

func safeTask[Key comparable](t TaskFunc[Key]) TaskFunc[Key] {
	return func(id int, key Key) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = errors.Errorf("task %d panic: %v\nstack: %s", id, r, debug.Stack())
			}
		}()
		return t(id, key)
	}
}
