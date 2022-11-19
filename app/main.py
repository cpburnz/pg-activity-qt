"""
This module defines the entry point to the application.
"""

import logging
import sys
from typing import (
	List)

import qasync

from app.gui.activity import (
	ActivityController)


def main(argv: List[str]) -> int:
	"""
	Runs the program.
	"""
	# Setup logging.
	log = logging.getLogger()
	log.setLevel(logging.DEBUG)

	handler = logging.StreamHandler(sys.stdout)
	handler.setFormatter(logging.Formatter(
		"%(asctime)s [%(threadName)s %(name)s] %(levelname)s: %(message)s",
		"%Y-%m-%d %H:%M:%S",
	))
	log.addHandler(handler)

	# Create application.
	controller = ActivityController()
	return qasync.run(controller.run())


if __name__ == '__main__':
	sys.exit(main(sys.argv))
