include makefiles/formatter.mk
include makefiles/pytest.mk


.PHONY: build_pkg
build_pkg:
	poetry build
