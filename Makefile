fmt:
	go mod tidy -compat=1.17
	gofmt -l -s -w .

producer:
	go run ./producer/*

worker:
	go run ./worker/*

docker.up:
	docker-compose up -d

docker.down:
	docker-compose down
