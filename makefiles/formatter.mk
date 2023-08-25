.PHONY: lint
lint:
	pflake8 ${PACKAGE_NAME} .
	mypy ${PACKAGE_NAME} .


.PHONY: format
format:
	black --exclude=.venv ${PACKAGE_NAME} .
	autoflake -ri --remove-all-unused-imports --ignore-init-module-imports --remove-unused-variables ${PACKAGE_NAME} .
	isort --profile=black ${PACKAGE_NAME} .
