.PHONY: fmt lint golint test test-with-coverage ci
# TODO: When Go 1.9 is released vendor folder should be ignored automatically
PACKAGES=`go list ./... | grep -v vendor | grep -v mocks`

fmt:
	for pkg in ${PACKAGES}; do \
		go fmt $$pkg; \
	done;

lint:
	gometalinter --exclude=vendor/ --tests --config=gometalinter.json --disable-all -E vet -E gofmt -E misspell -E ineffassign -E goimports -E deadcode ./...

golint:
	for pkg in ${PACKAGES}; do \
		golint $$pkg; \
	done;

test:
	for pkg in ${PACKAGES}; do \
		go test $$pkg; \
	done;

test-with-coverage:
	echo "" > coverage.out
	echo "mode: set" > coverage-all.out
	for pkg in ${PACKAGES}; do \
		go test -coverprofile=coverage.out -covermode=set $$pkg; \
		tail -n +2 coverage.out >> coverage-all.out; \
	done;
	#go tool cover -html=coverage-all.out

ci:
	bash -c 'docker-compose -f docker-compose.test.yml -p machinery_ci up --build --abort-on-container-exit --exit-code-from sut'
