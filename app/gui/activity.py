"""
This module defines the activity window.
"""

import logging
from typing import (
	List,
	Optional,
	cast)

from PySide6.QtCore import (
	SignalInstance)
from PySide6.QtGui import (
	QAction,)
from PySide6.QtWidgets import (
	QMainWindow,
	QStatusBar,
	QTableWidget,
	QTextEdit)
from PySide6.QtUiTools import (
	QUiLoader)

from .util import (
	ObjectSel)

ACTION_CANCEL_QUERY = ObjectSel(QAction, "action_CancelQuery")
"""
The selector for the cancel query action.
"""

ACTION_CONNECT = ObjectSel(QAction, "action_Connect")
"""
The selector for the connect action.
"""

ACTION_DISCONNECT = ObjectSel(QAction, "action_Disconnect")
"""
The selector for the disconnect action.
"""

ACTION_KILL_QUERY = ObjectSel(QAction, "action_KillQuery")
"""
The selector for the kill query action.
"""

ACTION_REFRESH = ObjectSel(QAction, "action_Refresh")
"""
The selector for the refresh action.
"""

LOG = logging.getLogger(__name__)
"""
The module logger.
"""

TITLE = "PostgreSQL Activity"
"""
The window title.
"""

WIDGET_ACTIVITY_TABLE = ObjectSel(QTableWidget, "table_Activity")
"""
The selector for the activity table widget.
"""

WIDGET_QUERY_TEXT = ObjectSel(QTextEdit, "text_Query")
"""
The selector for the query text widget.
"""

WIDGET_STATUS_BAR = ObjectSel(QStatusBar, "statusbar")
"""
The selector for the status bar widget.
"""

WINDOW_UI_FILE = "activity.ui"
"""
The path to the activity window UI file.
"""

MENU_CONNECTED_ACTIONS = [
	ACTION_DISCONNECT,
	ACTION_REFRESH,
]
"""
The selectors for the menu actions that require an active connection.
"""

MENU_SELECTED_QUERY_ACTIONS = [
	ACTION_CANCEL_QUERY,
	ACTION_KILL_QUERY,
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

		self.__activity_table = cast(QTableWidget, None)
		"""
		*__activity_table* (:class:`QTableWidget`) is the activity table widget.
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
		self.__enable_actions(MENU_CONNECTED_ACTIONS, False)

	def __disable_selected_query_actions(self) -> None:
		"""
		Disable the menu actions that require a specific query to be selected from
		the activity table.
		"""
		self.__enable_actions(MENU_SELECTED_QUERY_ACTIONS, False)

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
			action: QAction = self.__window.findChild(*sel)  # type: ignore
			action.setEnabled(enable)

	def __on_action_cancel_query(self) -> None:
		"""
		Called when the cancel query action is triggered.
		"""
		LOG.debug("Cancel query.")
		# TODO: Get PID of selected query.
		# TODO: Cancel query.

	def __on_action_connect(self) -> None:
		"""
		Called when the connect action is triggered.
		"""
		LOG.debug("Open connect dialog.")
		connect = ConnectController()
		connect.open()

		# TODO: Bind to signal to get dialog result.
		# TODO: On success:
		# - Disconnect active connection.
		# - Establish new connection.
		# - Start activity refresh.

	def __on_action_disconnect(self) -> None:
		"""
		Called when the disconnect action is triggered.
		"""
		LOG.debug("Disconnect.")
		# TODO: Disconnect active connection.
		# TODO: Disable connected actions.
		# TODO: Stop activity refresh.

	def __on_action_kill_query(self) -> None:
		"""
		Called when the kill query action is triggered.
		"""
		LOG.debug("Kill query.")
		# TODO: Get PID of selected query.
		# TODO: Terminate query.

	def __on_action_refresh(self) -> None:
		"""
		Called when the refresh action is triggered.
		"""
		LOG.debug("Refresh.")
		# TODO: Query for activity.
		# TODO: Update activity table.

	def open(self) -> None:
		"""
		Open the activity window.
		"""
		LOG.debug("Create window.")

		# Create window.
		self.__window = QUiLoader().load(WINDOW_UI_FILE)
		self.__set_title()

		# Bind actions.
		action_sel: ObjectSel
		for action_sel, callback in [
			(ACTION_CANCEL_QUERY, self.__on_action_cancel_query),
			(ACTION_CONNECT, self.__on_action_connect),
			(ACTION_DISCONNECT, self.__on_action_disconnect),
			(ACTION_KILL_QUERY, self.__on_action_kill_query),
			(ACTION_REFRESH, self.__on_action_refresh),
		]:
			action: Optional[QAction] = self.__window.findChild(*action_sel)  # type: ignore
			if action is not None:
				action.triggered: SignalInstance  # noqa
				action.triggered.connect(callback)
			else:
				LOG.warning("Failed to find {cls}:{name}.".format(
					cls=action_sel.type.__name__, name=action_sel.name,
				))

		# Initialize actions.
		self.__disable_connected_actions()
		self.__disable_selected_query_actions()

		# Get widgets.
		self.__activity_table = self.__window.findChild(*WIDGET_ACTIVITY_TABLE)
		self.__query_text = self.__window.findChild(*WIDGET_QUERY_TEXT)
		self.__status_bar = self.__window.findChild(*WIDGET_STATUS_BAR)

		# Display window.
		self.__window.show()

		LOG.debug("Window created.")

	def __set_title(self, prefix: Optional[str] = None) -> None:
		"""
		Set the window title.

		*prefix* (:class:`str` or :data:`None`) is the title prefix.
		"""
		# Build title.
		title_parts = [prefix, TITLE]
		title = " - ".join(filter(None, title_parts))

		# Set title.
		self.__window.setWindowTitle(title)
