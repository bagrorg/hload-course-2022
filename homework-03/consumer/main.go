package main

func main() {
	go KafkaDvij()
	r := SetupWorker()
	err := r.Run(":8081")
	if err != nil {
		panic("Something wrong with router: " + err.Error())
	}

}
