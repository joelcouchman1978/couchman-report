.PHONY: report clean
report:
	tectonic -X compile report/tex/main.tex --outdir report/build
clean:
	rm -rf report/build/*
