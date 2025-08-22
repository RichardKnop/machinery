.PHONY: fmt lint golint test test-with-coverage ci
# TODO: When Go 1.9 is released vendor folder should be ignored automatically
PACKAGES=`go list ./... | grep -v vendor | grep -v mocks`

fmt:
	for pkg in ${PACKAGES}; do \
		go fmt $$pkg; \
	done;

lint:
	gometalinter --tests --disable-all --deadline=120s -E vet -E gofmt -E misspell -E ineffassign -E goimports -E deadcode ./...

golint:
	for pkg in ${PACKAGES}; do \
		golint -set_exit_status $$pkg || GOLINT_FAILED=1; \
	done; \
	[ -z "$$GOLINT_FAILED" ]

test:
	TEST_FAILED= ; \
	for pkg in ${PACKAGES}; do \
		go test $$pkg || TEST_FAILED=1; \
	done; \
	[ -z "$$TEST_FAILED" ]

test-with-coverage:
	echo "" > coverage.out
	echo "mode: set" > coverage-all.out
	TEST_FAILED= ; \
	for pkg in ${PACKAGES}; do \
		go test -coverprofile=coverage.out -covermode=set $$pkg || TEST_FAILED=1; \
		tail -n +2 coverage.out >> coverage-all.out; \
	done; \
	[ -z "$$TEST_FAILED" ]
	#go tool cover -html=coverage-all.out

ci:
	bash -c 'docker-compose -f docker-compose.test.yml -p machinery_ci up --build --abort-on-container-exit --exit-code-from sut'
