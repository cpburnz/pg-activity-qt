"""
This module defines the activity window.
"""

import logging
from typing import (
	List,
	Optional,
	cast)

from PySide6.QtCore import (
	QObject,
	SignalInstance)
from PySide6.QtGui import (
	QAction)
from PySide6.QtWidgets import (
	QMainWindow,
	QStatusBar,
	QTableWidget,
	QTextEdit)
from PySide6.QtUiTools import (
	QUiLoader)

from app.activity import (
	PostgresActivityModel)
from .connect import (
	ConnectDialogController,
	ConnectDialogData)
from .util import (
	ObjectSel,
	find_child)

_ACTION_CANCEL_QUERY = ObjectSel(QAction, "action_CancelQuery")
"""
The selector for the cancel query action.
"""

_ACTION_CONNECT = ObjectSel(QAction, "action_Connect")
"""
The selector for the connect action.
"""

_ACTION_DISCONNECT = ObjectSel(QAction, "action_Disconnect")
"""
The selector for the disconnect action.
"""

_ACTION_KILL_QUERY = ObjectSel(QAction, "action_KillQuery")
"""
The selector for the kill query action.
"""

_ACTION_REFRESH = ObjectSel(QAction, "action_Refresh")
"""
The selector for the refresh action.
"""

LOG = logging.getLogger(__name__)
"""
The module logger.
"""

_WIDGET_ACTIVITY_TABLE = ObjectSel(QTableWidget, "table_Activity")
"""
The selector for the activity table widget.
"""

_WIDGET_QUERY_TEXT = ObjectSel(QTextEdit, "textEdit_Query")
"""
The selector for the query text widget.
"""

_WIDGET_STATUS_BAR = ObjectSel(QStatusBar, "statusbar")
"""
The selector for the status bar widget.
"""

_WINDOW_UI_FILE = "activity.ui"
"""
The path to the activity window UI file.
"""

_MENU_CONNECTED_ACTIONS = [
	_ACTION_DISCONNECT,
	_ACTION_REFRESH,
]
"""
The selectors for the menu actions that require an active connection.
"""

_MENU_SELECTED_QUERY_ACTIONS = [
	_ACTION_CANCEL_QUERY,
	_ACTION_KILL_QUERY,
]
"""
The selectors for the menu actions that require a specific query to be selected.
"""


class ActivityController(object):
	"""
	The :class:`ActivityController` class manages the activity window.
	"""

	def __init__(self) -> None:
		"""
		Initializes the :class:`ActivityController` instance.
		"""

		self.__activity_model: Optional[PostgresActivityModel] = None
		"""
		*__activity_model* (:class:`PostgresActivityModel`) is used to monitor the
		activity of the PostgreSQL database.
		"""

		self.__activity_table = cast(QTableWidget, None)
		"""
		*__activity_table* (:class:`QTableWidget`) is the activity table widget.
		"""

		self.__base_title = cast(str, None)
		"""
		*base_title* (:class:`str`) is the base window title.
		"""

		self.__query_text = cast(QTextEdit, None)
		"""
		*__query_text* (:class:`QTextEdit`) is the query text widget.
		"""

		self.__status_bar = cast(QStatusBar, None)
		"""
		*__status_bar* (:class:`QStatusBar`) is the status bar widget.
		"""

		self.__window = cast(QMainWindow, None)
		"""
		*window* (:class:`QMainWindow`) is the activity window.
		"""

	def __disable_connected_actions(self) -> None:
		"""
		Disable the menu actions that require an active connection.
		"""
		self.__enable_actions(_MENU_CONNECTED_ACTIONS, False)

	def __disable_selected_query_actions(self) -> None:
		"""
		Disable the menu actions that require a specific query to be selected from
		the activity table.
		"""
		self.__enable_actions(_MENU_SELECTED_QUERY_ACTIONS, False)

	def __enable_actions(
		self,
		selectors: List[ObjectSel],
		enable: bool,
	) -> None:
		"""
		Enables or disables the specified actions.

		*selectors* (:class:`list` of :class:`ObjectSel`) contains the selectors for
		the actions.

		*enable* (:class:`bool`) is whether the action should be enabled
		(:data:`True`), or disabled (:data:`False`).
		"""
		for sel in selectors:
			action: QAction = self.__get_child(sel)
			action.setEnabled(enable)

	def __get_child(self, sel: ObjectSel) -> QObject:
		"""
		Get the window child object.

		*sel* (:class:`ObjectSel`) is the object selector.

		Returns the child (:class:`QObject`).
		"""
		child = find_child(self.__window, sel)
		assert child is not None, "Failed to find child {type}:{name}.".format(
			type=sel.type.__name__, name=sel.name,
		)
		return child

	def __get_selected_pid(self) -> Optional[int]:
		"""
		Get the PID of the selected connection.

		Returns the PID (:class:`int` or :data:`None`).
		"""
		# TODO: Get PID from activity table.

	def __on_action_cancel_query(self) -> None:
		"""
		Called when the cancel query action is triggered.
		"""
		LOG.debug("Cancel query.")
		pid = self.__get_selected_pid()
		if pid is not None:
			self.__activity_model.cancel_query(pid)

	def __on_action_connect(self) -> None:
		"""
		Called when the connect action is triggered.
		"""
		LOG.debug("Open connect dialog.")
		connect = ConnectDialogController()
		connect.open()

		# Bind signals.
		# - NOTE: This is nothing to do if the connect dialog is canceled.
		connect.signals.accepted.connect(self.__on_connect_submit)

	def __on_action_disconnect(self) -> None:
		"""
		Called when the disconnect action is triggered.
		"""
		LOG.debug("Disconnect.")
		activity_model, self.__activity_model = self.__activity_model, None
		activity_model.close()

	def __on_action_kill_query(self) -> None:
		"""
		Called when the kill query action is triggered.
		"""
		LOG.debug("Kill query.")
		pid = self.__get_selected_pid()
		if pid is not None:
			self.__activity_model.kill_query(pid)

	def __on_action_refresh(self) -> None:
		"""
		Called when the refresh action is triggered.
		"""
		LOG.debug("Refresh.")
		self.__activity_model.refresh()

	def __on_connect_submit(self, data: ConnectDialogData) -> None:
		"""
		Called when the connect dialog is submitted.

		*data* (:class:`ConnectDialogData`) is the dialog data.
		"""
		LOG.debug("Connect submit.")
		# TODO: On success:
		# - Disconnect active connection.
		# - Establish new connection.
		# - Start activity refresh.




	def open(self) -> None:
		"""
		Open the activity window.
		"""
		LOG.debug("Create window.")

		# Create window.
		ui_result = QUiLoader().load(_WINDOW_UI_FILE)
		assert isinstance(ui_result, QMainWindow), ui_result
		self.__window = ui_result
		self.__base_title = self.__window.windowTitle()

		# Bind actions.
		action_sel: ObjectSel
		for action_sel, callback in [
			(_ACTION_CANCEL_QUERY, self.__on_action_cancel_query),
			(_ACTION_CONNECT, self.__on_action_connect),
			(_ACTION_DISCONNECT, self.__on_action_disconnect),
			(_ACTION_KILL_QUERY, self.__on_action_kill_query),
			(_ACTION_REFRESH, self.__on_action_refresh),
		]:
			action: QAction = self.__get_child(action_sel)
			action.triggered: SignalInstance  # noqa
			action.triggered.connect(callback)

		# Initialize actions.
		self.__disable_connected_actions()
		self.__disable_selected_query_actions()

		# Get widgets.
		self.__activity_table = self.__get_child(_WIDGET_ACTIVITY_TABLE)
		self.__query_text = self.__get_child(_WIDGET_QUERY_TEXT)
		self.__status_bar = self.__get_child(_WIDGET_STATUS_BAR)

		# Display window.
		self.__window.show()

		LOG.debug("Window created.")

	def __set_title(self, prefix: Optional[str] = None) -> None:
		"""
		Set the window title.

		*prefix* (:class:`str` or :data:`None`) is the title prefix.
		"""
		# Build title.
		title_parts = [prefix, self.__base_title]
		title = " - ".join(filter(None, title_parts))

		# Set title.
		self.__window.setWindowTitle(title)
