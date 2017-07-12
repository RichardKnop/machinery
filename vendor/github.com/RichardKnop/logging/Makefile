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
	bash -c 'go list ./... | grep -v vendor | xargs -n1 go fmt'

lint:
	bash -c 'gometalinter --disable-all -E vet -E gofmt -E misspell -E ineffassign -E goimports -E deadcode --tests --vendor ./...'

golint:
	# TODO: When Go 1.9 is released vendor folder should be ignored automatically
	bash -c 'go list ./... | grep -v vendor | grep -v mocks | xargs -n1 golint'

test:
	# TODO: When Go 1.9 is released vendor folder should be ignored automatically
	bash -c 'go list ./... | grep -v vendor | xargs -n1 go test -timeout=10s'

test-with-coverage:
	# TODO: When Go 1.9 is released vendor folder should be ignored automatically
	bash -c 'go list ./... | grep -v vendor | xargs -n1 go test -timeout=10s -coverprofile=coverage.txt -covermode=set'
