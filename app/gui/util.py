"""
This module defines GUI utilities.
"""

from typing import (
	NamedTuple,
	Type)

from PySide6.QtCore import (
	QObject)


class ObjectSel(NamedTuple):
	"""
	The :class:`ObjectSel` class is used as a selector for finding a descendant
	:class:`QObject`.
	"""
	type: Type[QObject]
	name: str
