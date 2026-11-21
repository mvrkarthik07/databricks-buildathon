PYTHON ?= python3
REF ?= $(PYTHON) ref/reference.py
FIXTURES ?= starter-duckdb/fixtures
ADVERSARIAL ?= tests/adversarial
DEV ?= dev
OUT ?= out
ANSWER ?= dev-answer

.PHONY: check-fixtures check-adversarial run-ref-dev check-dev

check-fixtures: CASES = $(FIXTURES)
check-adversarial: CASES = $(ADVERSARIAL)
check-fixtures check-adversarial:
	@set -eu; out=$$(mktemp -d); trap 'rm -rf "$$out"' EXIT; count=0; \
	for case in "$(CASES)"/*; do \
		test -d "$$case" || continue; \
		result="$$out/$${case##*/}"; \
		$(REF) "$$case/input" "$$result"; \
		if report=$$($(PYTHON) tools/diff.py "$$result" "$$case/expected"); then \
			echo "ok   $${case##*/}"; \
		else \
			echo "FAIL $${case##*/}"; echo "$$report"; exit 1; \
		fi; \
		count=$$((count + 1)); \
	done; \
	test "$$count" -gt 0; echo "$$count fixtures passed"

run-ref-dev:
	$(REF) "$(DEV)" "$(OUT)"

check-dev:
	$(PYTHON) tools/diff.py "$(OUT)" "$(ANSWER)"
