"""
This module defines the entry point to the application.
"""

import logging
import sys
from typing import (
	List)

from PySide6.QtWidgets import (
	QApplication)

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
	app = QApplication(argv)
	controller = ActivityController()
	controller.open()
	return app.exec_()


if __name__ == '__main__':
	sys.exit(main(sys.argv))
