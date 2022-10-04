"""
This module defines utilities for dealing with threads from Qt.
"""

import dataclasses
import logging
import traceback
from typing import (
	Any,
	Callable,
	List,
	Optional)

from PySide6.QtCore import (
	QObject,
	QRunnable,
	Signal,
	SignalInstance)

LOG = logging.getLogger(__name__)
"""
The module logger.
"""


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

	def make_future(self) -> 'WorkerFuture':
		"""
		Create a future for this worker.

		Returns the future (:class:`WorkerFuture`).
		"""
		future = WorkerFuture()
		self.signals.result.connect(future.set_result)
		self.signals.error.connect(future.set_error)
		return future

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


class WorkerFuture(object):
	"""
	The :class:`WorkerFuture` class is used to encapsulate the asynchronous
	execution of a worker.
	"""

	def __init__(self) -> None:
		"""
		Initializes the :class:`WorkerFuture` instance.
		"""

		self.__error: Optional[WorkerError] = None
		"""
		*__error* (:class:`WorkerError` or :data:`None`) is the set error.
		"""

		self.__error_callbacks: List[Callable[[WorkerError], None]] = []
		"""
		*__error_callbacks* (:class:`list` of :class:`callable`) contains the
		registered error callbacks.
		"""

		self.__has_result = False
		"""
		*__has_result* (:class:`bool`) is whether the result has been set.
		"""

		self.__result = None
		"""
		*__result* is is the set result.
		"""

		self.__result_callbacks: List[Callable[[Any], None]] = []
		"""
		*__result_callbacks* (:class:`list` of :class:`callable`) contains the
		registered result callbacks.
		"""

	def add_error_callback(
		self,
		callback: Callable[['WorkerError'], None],
	) -> None:
		"""
		Register a callback to be called when the worker fails with an error.

		*callback* (:class:`callable`) is the error handler. On failure, this will
		be called with the error (:class:`WorkerError`). The return value is
		ignored.
		"""
		self.__error_callbacks.append(callback)
		if self.__error is not None:
			self.__invoke_error_callbacks()

	def add_result_callback(
		self,
		callback: Callable[[Any], None],
	) -> None:
		"""
		Register a callback to be called when the worker succeeds.

		*callback* (:class:`callable`) is the result handler. On success, this will
		be called with the worker result. The return value is ignored.
		"""
		self.__result_callbacks.append(callback)
		if self.__has_result:
			self.__invoke_result_callbacks()

	def __invoke_error_callbacks(self) -> None:
		"""
		Invoke the registered error callbacks.
		"""
		assert self.__error is not None, f"Error {self.__error!r} is not set"
		while self.__error_callbacks:
			callback = self.__error_callbacks.pop(0)
			try:
				callback(self.__error)
			except:  # noqa
				LOG.exception(f"Unhandled exception in error callback {callback!r}.")

	def __invoke_result_callbacks(self) -> None:
		"""
		Invoke the registered result callbacks.
		"""
		assert self.__has_result, f"Result {self.__has_result!r} is not set."
		while self.__result_callbacks:
			callback = self.__result_callbacks.pop(0)
			try:
				callback(self.__result)
			except:  # noqa
				LOG.exception(f"Unhandled exception in result callback {callback!r}.")

	def set_error(self, error: 'WorkerError') -> None:
		"""
		Set the error of the future.

		*error* (:class:`WorkerError`) is the error to set.
		"""
		self.__error = error
		self.__invoke_error_callbacks()

	def set_result(self, result: Any) -> None:
		"""
		Set the result of the future.

		*result* is the value to set.
		"""
		self.__has_result = True
		self.__result = result
		self.__invoke_result_callbacks()


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
