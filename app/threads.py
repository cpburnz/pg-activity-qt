"""
This module defines utilities for dealing with threads from Qt.
"""

import dataclasses
import traceback
from typing import (
	Callable)

from PySide6.QtCore import (
	QObject,
	QRunnable,
	Signal,
	SignalInstance)


class Worker(QRunnable):
	"""
	The :class:`Worker` class is used to run a blocking function in a separate
	thread.
	"""

	def __init__(self, fn: Callable) -> None:
		"""
		Initializes the :class:`Worker` instance.

		*fn* (:class:`callable`) is the function to call.
		"""
		super().__init__()

		self.__fn: Callable = fn
		"""
		*__fn* (:class:`callable`) is the function to call.
		"""

		self.signals = WorkerSignals()
		"""
		*signals* (:class:`WorkerSignals`) contains the signals used by the worker.
		"""

	def run(self) -> None:
		"""
		Runs the worker.
		"""
		try:
			result = self.__fn()
		except BaseException as e:
			tb = traceback.format_exc()
			error = WorkerError(
				traceback=tb,
				value=e,
			)
			self.signals.error.emit(error)
		else:
			self.signals.result.emit(result)


class WorkerSignals(QObject):
	"""
	The :class:`WorkerSignals` class defines the signals used by the
	:class:`Worker` class.

	- NOTICE: Only a :class:`QObject` can define signals.
	"""

	error = Signal()
	"""
	*error* (:class:`Signal`) is the signal emitted when an exception occurs. This
	will be emitted with a :class:`tuple` containing the exception object
	(:class:`Exception`) and traceback text (:class:`str`).
	"""

	result = Signal()
	"""
	*result* (:class:`Signal`) is the signal emitted when the function returns
	successfully.
	"""

	# Fix type hints.
	error: SignalInstance
	result: SignalInstance


@dataclasses.dataclass(frozen=True)
class WorkerError(object):
	traceback: str
	value: BaseException
