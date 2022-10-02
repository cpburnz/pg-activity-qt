"""
This module defines GUI utilities.
"""

from typing import (
	NamedTuple,
	Optional,
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


def find_child(parent: QObject, sel: 'ObjectSel') -> Optional[QObject]:
	"""
	Find the descendant object. This wraps the `QObject.findChild()` method to
	fix type hinting.
	"""
	return parent.findChild(sel.type, sel.name)
