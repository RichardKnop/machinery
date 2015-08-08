DEPS=go list -f '{{range .TestImports}}{{.}} {{end}}' ./...

freeze:
	go get github.com/tools/godep
	go list ./... | xargs -n1 godep save -r
deps:
	go get github.com/tools/godep
	godep restore
	$(DEPS) | xargs -n1 go get -d

test:
	go list ./... | xargs -n1 go test -timeout=3s
