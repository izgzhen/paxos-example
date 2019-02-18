check:
	pylint -d W -d C -d R -d U *.py
	mypy *.py
