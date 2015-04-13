DEPS=go list -f '{{range .TestImports}}{{.}} {{end}}' ./v1

deps:
	go get -d -v ./v1
	$(DEPS) | xargs -n1 go get -d

test: deps
	go list ./... | xargs -n1 go test -timeout=3s
