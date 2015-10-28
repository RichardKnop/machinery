DEPS=go list -f '{{range .TestImports}}{{.}} {{end}}' ./...

export GO15VENDOREXPERIMENT=1

update-deps:
	rm -R Godeps
	rm -R vendor
	go get github.com/tools/godep
	godep save ./...

restore-deps:
	go get github.com/tools/godep
	godep restore
	$(DEPS) | xargs -n1 go get -d

test:
	go list ./... | grep -v vendor | xargs -n1 go test -timeout=3s
