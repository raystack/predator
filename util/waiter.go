package util

func Wait(execf func()) <-chan bool {
	doneChannel := make(chan bool, 1)
	go func() {
		execf()
		doneChannel <- true
	}()
	return doneChannel
}
