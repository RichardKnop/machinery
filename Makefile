DEPS=go list -f '{{range .TestImports}}{{.}} {{end}}' ./...

update-deps:
	rm -rf Godeps
	rm -rf vendor
	go get github.com/tools/godep
	godep save ./...

install-deps:
	go get github.com/tools/godep
	godep restore
	$(DEPS) | xargs -n1 go get -d

fmt:
	bash -c 'git ls-files "**.go" | grep -v ^vendor/ | xargs -n1 gofmt -e -s -w'

ci:
	bash -c '(docker-compose -f docker-compose.test.yml -p machinery_ci up --build -d) && (docker logs -f machinery_sut &) && (docker wait machinery_sut)'

test:
	bash -c 'go list ./... | grep -v vendor | grep -v example | xargs -n1 go test -timeout=30s'
