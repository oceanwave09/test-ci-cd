lint:
	# mypy smartmapper --ignore-missing-imports
	flake8 smartmapper --max-line-length 99

dev:
	pip install -e .

test: dev
	pytest --doctest-modules --junitxml=junit/test-results.xml

build: clean
	pip install wheel
	python setup.py bdist_wheel

clean:
	@rm -rf .pytest_cache/ .mypy_cache/ junit/ build/ dist/
	@find . -not -path './.venv*' -path '*/__pycache__*' -delete
	@find . -not -path './.venv*' -path '*/*.egg-info*' -delete
