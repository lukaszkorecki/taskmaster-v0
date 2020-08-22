SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules


help:
	@egrep ".+:.+##" $(MAKEFILE_LIST) | grep -v help | sed 's/##/\n  /g'


start-services:  ## start backing services
	docker-compose up

start-services-bg:  ## start backing services
	docker-compose up -d



fmt: ## format clojure source code
	cljstyle fix .

test: start-services-bg
	lein test


lint: fmt
	clj-kondo --lint ./src/
