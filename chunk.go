package task

func ExecuteChunk[T any](arr []T, size, thread int, handler func([]T) error) error {
	chunked := chunk(arr, size)
	keyList := make([]int, 0)
	for i := range chunked {
		keyList = append(keyList, i)
	}
	return Execute(keyList, thread, func(key int) error {
		return handler(chunked[key])
	})
}

func chunk[T any](collection []T, size int) [][]T {
	if size <= 0 {
		return make([][]T, 0)
	}

	chunksNum := len(collection) / size
	if len(collection)%size != 0 {
		chunksNum += 1
	}

	result := make([][]T, 0, chunksNum)

	for i := 0; i < chunksNum; i++ {
		last := (i + 1) * size
		if last > len(collection) {
			last = len(collection)
		}
		result = append(result, collection[i*size:last])
	}
	return result
}
