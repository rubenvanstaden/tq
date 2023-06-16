fmt:
	go mod tidy -compat=1.17
	gofmt -l -s -w .

client:
	go run ./client/*

worker:
	go run ./worker/*

up:
	docker-compose up -d

down:
	docker-compose down
