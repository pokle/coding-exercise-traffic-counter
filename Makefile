test: deps
	pytest --doctest-modules

deps:
	which pytest || python3 -m pip install pytest
