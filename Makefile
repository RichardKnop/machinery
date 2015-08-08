DEPS=go list -f '{{range .TestImports}}{{.}} {{end}}' ./...

deps:
	go get github.com/tools/godep
	godep restore
	$(DEPS) | xargs -n1 go get -d

test: deps
	go list ./... | xargs -n1 go test -timeout=3s
