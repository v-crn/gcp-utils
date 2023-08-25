FILE=tests/
FUNC=""


.PHONY: test
test:
	pytest -s ${FILE} -k ${FUNC}
