package internal

func SpawnWorkers(workers int, taskChannel chan func()) {
	for i := 0; i < workers; i++ {
		go func() {
			for task := range taskChannel {
				task()
			}
		}()
	}
}
