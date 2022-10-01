#
# This Makefile is used to manage development and distribution.
#
# Created: 2022-09-26
# Updated: 2022-09-26
#

.PHONY: create-venv help update-venv

help:
	@echo "Usage: make [<target>]"
	@echo
	@echo "General Targets:"
	@echo "  help      Display this help message."
	@echo
	@echo "Development Targets:"
	@echo "  create-venv  Create the development Python virtual environment."
	@echo "  update-venv  Update the development Python virtual environment."

create-venv: dev-venv-create

update-venv: dev-venv-install


################################################################################
# Development
################################################################################

SRC_DIR := ./
VENV_DIR := ./dev/venv

PYTHON := python3
VENV := ./dev/venv.sh "${VENV_DIR}"

.PHONY: dev-venv-base dev-venv-create dev-venv-install

dev-venv-base:
	${PYTHON} -m venv --clear "${VENV_DIR}"

dev-venv-create: dev-venv-base dev-venv-install

dev-venv-install:
	${VENV} pip install --upgrade pip setuptools wheel
	${VENV} pip install --upgrade pyside6-essentials
	${VENV} pip install -e "${SRC_DIR}"
