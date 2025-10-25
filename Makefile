.PHONY: lint
lint:
	docker run --rm -i -v ${PWD}:/src -w /src golangci/golangci-lint:v2.5.0-alpine golangci-lint run ./...

.PHONY: lint-fix
lint-fix:
	docker run --rm -i -v ${PWD}:/src -w /src golangci/golangci-lint:v2.5.0-alpine golangci-lint run --fix ./...