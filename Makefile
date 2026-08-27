# Run everything from the repository root. No AWS credentials required —
# the pipeline runs the real Lambda handler against moto-mocked AWS.
.PHONY: all devices pipeline dashboard test clean

all: devices pipeline dashboard test

devices:
	python3 scripts/make_devices.py

pipeline:
	python3 demo/run_pipeline.py

dashboard:
	python3 -m eventlog.dashboard

test:
	python3 -m pytest -q

clean:
	rm -f projections.duckdb ops_snapshot.json dashboard.html
