PYTHON ?= python3

.PHONY: all verify
all: verify
verify:
	$(PYTHON) -B scripts/verify_artifact.py
	$(PYTHON) -B scripts/verify_evidence.py
	$(PYTHON) -B scripts/verify_analysis.py
