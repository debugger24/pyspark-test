test:
	tox

build:
	python3 -m build

upload:
	python3 -m twine upload dist/*
