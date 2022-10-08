"""
This module defines the connection dialog.
"""

import dataclasses
import importlib.resources
import logging
from typing import (
	cast)

from PySide6.QtCore import (
	QObject,
	Signal,
	SignalInstance)
from PySide6.QtWidgets import (
	QDialog,
	QDialogButtonBox,
	QLineEdit,
	QPushButton,
	QSpinBox,
	QWidget)
from PySide6.QtUiTools import (
	QUiLoader)

import app.gui
from app.activity import (
	PostgresConnectionParams)
from .util import (
	ObjectSel,
	find_child)

_DIALOG_UI_FILE = "connect.ui"
"""
The path to the activity window UI file.
"""

_INPUT_DATABASE = ObjectSel(QLineEdit, "lineEdit_Database")
"""
The selector for the database input.
"""

_INPUT_HOST = ObjectSel(QLineEdit, "lineEdit_Host")
"""
The selector for the host input.
"""

_INPUT_PASSWORD = ObjectSel(QLineEdit, "lineEdit_Password")
"""
The selector for the password input.
"""

_INPUT_PORT = ObjectSel(QSpinBox, "spinBox_Port")
"""
The selector for the port input.
"""

_INPUT_USER = ObjectSel(QLineEdit, "lineEdit_User")
"""
The selector for the user input.
"""

LOG = logging.getLogger(__name__)
"""
The module logger.
"""

_WIDGET_BUTTON_BOX = ObjectSel(QDialogButtonBox, "buttonBox")
"""
The selector for the dialog button box.
"""

_WIDGET_CONNECT_BUTTON = ObjectSel(QPushButton, "pushButton_Connect")
"""
The selector for the connect button.
"""


class ConnectDialogController(object):
	"""
	The :class:`ConnectDialogController` class manages the connection dialog.
	"""

	def __init__(self) -> None:
		"""
		Initializes the :class:`ConnectDialogController` instance.
		"""

		self.__dialog = cast(QDialog, None)
		"""
		*dialog* (:class:`QDialog`) is the connect dialog.
		"""

		self.signals = ConnectDialogSignals()
		"""
		*signals* (:class:`ConnectDialogSignals`) contains the signals used by the
		controller.
		"""

	def __get_child(self, sel: ObjectSel) -> QObject:
		"""
		Get the dialog child object.

		*sel* (:class:`ObjectSel`) is the object selector.

		Returns the child (:class:`QObject`).
		"""
		child = find_child(self.__dialog, sel)
		assert child is not None, "Failed to find child {type}:{name}.".format(
			type=sel.type.__name__, name=sel.name,
		)
		return child

	def __on_dialog_accepted(self) -> None:
		"""
		Called when the dialog is accepted.
		"""
		LOG.debug("Dialog accepted.")
		form_data = self.__parse_form()
		self.signals.accepted.emit(form_data)

	def __on_dialog_rejected(self) -> None:
		"""
		Called when the dialog is rejected.
		"""
		LOG.debug("Dialog rejected.")
		self.signals.rejected.emit()

	def open(self, parent: QWidget) -> None:
		"""
		Open the connection dialog.

		*parent* (:class:`QWidget`) is the parent widget.
		"""
		LOG.debug("Create dialog.")

		# Create dialog.
		with importlib.resources.path(app.gui, _DIALOG_UI_FILE) as ui_file:
			ui_result = QUiLoader(parent).load(ui_file)

		assert isinstance(ui_result, QDialog), ui_result
		self.__dialog = ui_result

		# Setup connect button.
		connect_button: QPushButton = self.__get_child(_WIDGET_CONNECT_BUTTON)
		button_box: QDialogButtonBox = self.__get_child(_WIDGET_BUTTON_BOX)
		button_box.addButton(connect_button, QDialogButtonBox.ButtonRole.AcceptRole)

		# Bind signals.
		self.__dialog.accepted: SignalInstance  # noqa
		self.__dialog.accepted.connect(self.__on_dialog_accepted)
		self.__dialog.rejected: SignalInstance  # noqa
		self.__dialog.rejected.connect(self.__on_dialog_rejected)

		# Display dialog.
		self.__dialog.open()

	def __parse_form(self) -> 'ConnectDialogData':
		"""
		Parse the connection form.

		Returns the form data (:class:`ConnectDialogData`).
		"""
		# Parse form.
		database = self.__parse_input_line_edit(_INPUT_DATABASE)
		host = self.__parse_input_line_edit(_INPUT_HOST)
		password = self.__parse_input_line_edit(_INPUT_PASSWORD)
		port = self.__parse_input_spin_box(_INPUT_PORT)
		user = self.__parse_input_line_edit(_INPUT_USER)
		params = PostgresConnectionParams(
			database=database,
			host=host,
			password=password,
			port=port,
			user=user,
		)

		return ConnectDialogData(
			params=params,
		)

	def __parse_input_line_edit(self, input_sel: ObjectSel) -> str:
		"""
		Parse the line-edit input.

		*input_sel* (:class:`ObjectSel`) is the input selector.

		Returns the value (:class:`str` or :data:`None`).
		"""
		assert input_sel.type is QLineEdit, input_sel
		line_edit: QLineEdit = self.__get_child(input_sel)
		return line_edit.text().strip()

	def __parse_input_spin_box(self, input_sel: ObjectSel) -> int:
		"""
		Parse the spin-box input.

		*input_sel* (:class:`ObjectSel`) is the input selector.

		Returns the value (:class:`int` or :data:`None`).
		"""
		assert input_sel.type is QSpinBox, input_sel
		spin_box: QSpinBox = self.__get_child(input_sel)
		return spin_box.value()


class ConnectDialogSignals(QObject):
	"""
	The :class:`ConnectDialogSignals` class defines the signals used by the
	:class:`ConnectDialogController` class.

	- NOTICE: Only a :class:`QObject` can define signals.
	"""

	accepted = Signal(object)
	"""
	*accepted* (:class:`Signal`) is the signal emitted when the dialog is
	accepted. This will be emitted with the form data (:class:`ConnectDialogData`).
	"""

	rejected = Signal()
	"""
	*rejected* (:class:`Signal`) is the signal emitted when the dialog is
	rejected. This will not be emitted with any arguments.
	"""

	# Fix type hints.
	accepted: SignalInstance
	rejected: SignalInstance


@dataclasses.dataclass(frozen=True)
class ConnectDialogData(object):
	params: PostgresConnectionParams
