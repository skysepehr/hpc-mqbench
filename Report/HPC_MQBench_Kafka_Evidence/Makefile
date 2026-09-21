PYTHON ?= python3
LATEX ?= pdflatex
BIBTEX ?= bibtex
PAPER := hpc_mqbench_paper

.PHONY: all pdf verify figures
all: verify
pdf:
	mkdir -p build
	$(LATEX) -output-directory=build -interaction=nonstopmode -halt-on-error $(PAPER).tex
	$(BIBTEX) build/$(PAPER)
	$(LATEX) -output-directory=build -interaction=nonstopmode -halt-on-error $(PAPER).tex
	$(LATEX) -output-directory=build -interaction=nonstopmode -halt-on-error $(PAPER).tex
	cp build/$(PAPER).pdf $(PAPER).pdf
verify:
	$(PYTHON) -B scripts/verify_artifact.py
	$(PYTHON) -B scripts/build_referee_supplement.py --check
	$(PYTHON) -B scripts/verify_paper.py
	$(PYTHON) -B scripts/verify_claims.py
figures:
	$(PYTHON) -B scripts/plot_operating_limits.py
