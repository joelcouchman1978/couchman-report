LATEXMK = latexmk
TEXFLAGS = -pdf -interaction=nonstopmode -file-line-error -cd
BUILD ?= RELEASE

all: tex

data:
	python3 scripts/gen_tables.py

buildinfo:
	bash scripts/gen_buildinfo.sh

preview:
	BUILD=DRAFT $(MAKE) data tex

tex: buildinfo
	$(LATEXMK) $(TEXFLAGS) tex/main.tex -jobname=couchman_report -outdir=..

clean:
	$(LATEXMK) -C
	@rm -f couchman_report.pdf

.PHONY: all tex clean data buildinfo preview
