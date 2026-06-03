# Project:   dfe-loader
# File:      Makefile
# Purpose:   Standard CI targets via hyperi-ci
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED

.PHONY: check quality test build ci

check: ## Pre-push validation (quality + test)
	hyperi-ci check

quality: ## Run all quality checks
	hyperi-ci run quality

test: ## Run tests
	hyperi-ci run test

build: ## Build artifacts
	hyperi-ci run build

ci: quality test build ## Run full CI pipeline locally
