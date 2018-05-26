
test:
	which pytest || python3 -m pip install pytest
	pytest --doctest-modules