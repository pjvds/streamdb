benchdeps:
	go get -d -v golang.org/x/perf/cmd/benchstat
	go install golang.org/x/perf/cmd/benchstat
benchmark: benchdeps
	go test github.com/pjvds/streamdb/storage/ -test.bench='.*' -test.benchmem | tee results && benchstat results && rm results

