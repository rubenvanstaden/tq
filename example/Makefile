fmt:
	go mod tidy -compat=1.17
	gofmt -l -s -w .

client:
	go run ./client/*

server:
	go run ./server/*

up:
	docker-compose up -d

down:
	docker-compose down
