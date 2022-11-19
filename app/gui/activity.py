"""
This module defines the activity window.
"""

import asyncio
import datetime
import importlib.resources
import logging
from typing import (
	Any,
	List,
	Optional,
	Union,
	cast)

from PySide6.QtCore import (
	QAbstractTableModel,
	QModelIndex,
	QObject,
	QSortFilterProxyModel,
	Qt,
	SignalInstance)
from PySide6.QtGui import (
	QAction)
from PySide6.QtWidgets import (
	QApplication,
	QMainWindow,
	QStatusBar,
	QTableView,
	QTextEdit)
from PySide6.QtUiTools import (
	QUiLoader)
from qasync import (
	asyncSlot)

import app.gui
from app.activity import (
	ACTIVITY_HEADER,
	ActivityRow,
	PostgresActivityManager)
from .connect import (
	ConnectDialogController)
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

_WIDGET_ACTIVITY_TABLE = ObjectSel(QTableView, "tableView_Activity")
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

		self.__activity_model = cast(ActivityTableModel, None)
		"""
		*__activity_model* (:class:`ActivityTableModel`) is the table model.
		"""

		self.__activity_proxy_model = cast(QSortFilterProxyModel, None)
		"""
		*__activity_proxy_model* (:class:`QSortFilterProxyModel`) is the sort proxy model.
		"""

		self.__activity_table = cast(QTableView, None)
		"""
		*__activity_table* (:class:`QTableView`) is the activity table widget.
		"""

		self.__base_title = cast(str, None)
		"""
		*base_title* (:class:`str`) is the base window title.
		"""

		self.__pg_activity: Optional[PostgresActivityManager] = None
		"""
		*__pg_activity* (:class:`PostgresActivityManager`) is used to monitor the
		activity of the PostgreSQL database.
		"""

		self.__query_text = cast(QTextEdit, None)
		"""
		*__query_text* (:class:`QTextEdit`) is the query text widget.
		"""

		self.__refresh_interval = 10.0
		"""
		*__refresh_interval* (:class:`float`) is the refresh interval (in seconds).
		"""

		self.__refresh_task: Optional[asyncio.Task] = None
		"""
		*__refresh_task* (:class:`asyncio.TimerHandle` or :data:`None`) is the
		active refresh task.
		"""

		self.__status_bar = cast(QStatusBar, None)
		"""
		*__status_bar* (:class:`QStatusBar`) is the status bar widget.
		"""

		self.__window = cast(QMainWindow, None)
		"""
		*window* (:class:`QMainWindow`) is the activity window.
		"""

	def __clear_table(self) -> None:
		"""
		Clear the activity table.
		"""
		self.__activity_model.set_data([])

	async def __delayed_refresh(self) -> None:
		"""
		Refresh the PostgreSQL activity after a delay.
		"""
		# Delay before refresh.
		LOG.debug("Refresh delay.")
		try:
			await asyncio.sleep(self.__refresh_interval)
		except asyncio.CancelledError:
			return

		# Refresh activity.
		LOG.debug("Refresh activity.")
		try:
			results = await self.__pg_activity.fetch_activity()

		except Exception:
			LOG.exception("Refresh error.")

			# Clear refresh state.
			self.__refresh_task = None

		else:
			LOG.debug(f"Refresh done: {len(results)} rows.")

			# Clear refresh state.
			self.__refresh_task = None

			# Populate activity table.
			self.__populate_table(results)

			# Schedule next refresh.
			self.__start_refresh()

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

	async def __disconnect_pg(self) -> None:
		"""
		Disconnect the PostgreSQL activity manager.
		"""
		pg_activity, self.__pg_activity = self.__pg_activity, None
		if pg_activity is not None:
			await pg_activity.close()

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

	@asyncSlot()
	async def __on_action_cancel_query(self) -> None:
		"""
		Called when the cancel query action is triggered.
		"""
		LOG.debug("Cancel query action.")
		pid = self.__get_selected_pid()
		if pid is not None:
			await self.__pg_activity.cancel_query(pid)

	@asyncSlot()
	async def __on_action_connect(self) -> None:
		"""
		Called when the connect action is triggered.
		"""
		LOG.debug("Connect action.")

		# Open connect dialog.
		dialog = ConnectDialogController()
		data = await dialog.open(self.__window)

		if data is not None:
			LOG.debug("Connect dialog accepted.")

			# Disconnect active connection.
			self.__disable_connected_actions()
			self.__disable_selected_query_actions()
			self.__stop_refresh()
			self.__clear_table()
			self.__set_title()
			await self.__disconnect_pg()

			# Establish new connection.
			self.__pg_activity = PostgresActivityManager(data.params)
			try:
				await self.__pg_activity.connect()

			except Exception:
				LOG.debug("Connect error.")
				LOG.exception("Failed to establish connection.")

			else:
				LOG.debug("Connect done.")
				params = self.__pg_activity.params
				self.__set_title(f"{params.user}@{params.host}/{params.database}")
				self.__enable_actions(_MENU_CONNECTED_ACTIONS, True)
				self.__start_refresh()

		else:
			LOG.debug("Connect dialog rejected.")

	@asyncSlot()
	async def __on_action_disconnect(self) -> None:
		"""
		Called when the disconnect action is triggered.
		"""
		LOG.debug("Disconnect action.")

		# Disconnect active connection.
		self.__disable_connected_actions()
		self.__disable_selected_query_actions()
		self.__stop_refresh()
		self.__clear_table()
		self.__set_title()
		await self.__disconnect_pg()

	@asyncSlot()
	async def __on_action_kill_query(self) -> None:
		"""
		Called when the kill query action is triggered.
		"""
		LOG.debug("Kill query action.")
		pid = self.__get_selected_pid()
		if pid is not None:
			await self.__pg_activity.terminate_query(pid)

	@asyncSlot()
	async def __on_action_refresh(self) -> None:
		"""
		Called when the refresh action is triggered.
		"""
		LOG.debug("Refresh action.")
		self.__start_refresh()

	async def run(self) -> int:
		"""
		Open the activity window.

		Returns the status code (:class:`int`).
		"""
		LOG.debug("Create window.")

		# Create window.
		with importlib.resources.path(app.gui, _WINDOW_UI_FILE) as ui_file:
			ui_result = QUiLoader().load(ui_file)

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
		self.__activity_table: QTableView = self.__get_child(_WIDGET_ACTIVITY_TABLE)
		self.__query_text = self.__get_child(_WIDGET_QUERY_TEXT)
		self.__status_bar = self.__get_child(_WIDGET_STATUS_BAR)

		# Setup activity table.
		self.__activity_model = ActivityTableModel(self.__activity_table)
		pid_column = self.__activity_model.get_fields().index("pid")

		self.__activity_proxy_model = QSortFilterProxyModel(self.__activity_table)
		self.__activity_proxy_model.setDynamicSortFilter(True)
		self.__activity_proxy_model.setSourceModel(self.__activity_model)

		self.__activity_table.setModel(self.__activity_proxy_model)
		self.__activity_table.sortByColumn(pid_column, Qt.AscendingOrder)

		# Display window.
		self.__window.show()

		LOG.debug("Window created.")

		# Wait for application to close.
		stop_future = asyncio.Future()
		qapp = QApplication.instance()
		qapp.aboutToQuit: SignalInstance  # noqa
		qapp.aboutToQuit.connect(lambda: stop_future.set_result(None))
		await stop_future

		return 1

	def __populate_table(self, data: List[ActivityRow]) -> None:
		"""
		Populate the activity table.

		*data* (:class:`list` of :class:`ActivityRow`) is the activity data.
		"""
		LOG.debug(f"Populate table: {len(data)} rows.")

		# Get PID for the selected row.
		pid: Optional[int] = None
		for proxy_index in self.__activity_table.selectionModel().selectedRows():
			LOG.debug(f"Old proxy index: {proxy_index.row()}")
			source_index = self.__activity_proxy_model.mapToSource(proxy_index)
			LOG.debug(f"Old source index: {source_index.row()}")
			activity_row = self.__activity_model.get_data()[source_index.row()]
			pid = activity_row.pid

		LOG.debug(f"Selected PID: {pid}")

		# Update activity data.
		self.__activity_model.set_data(data)
		self.__activity_table.resizeColumnsToContents()

		# Reselect the row with the PID.
		if pid is not None:
			for i, activity_row in enumerate(data):
				if pid == activity_row.pid:
					source_index = self.__activity_model.index(i, 0)
					LOG.debug(f"New source index: {source_index.row()}")
					proxy_index = self.__activity_proxy_model.mapFromSource(source_index)
					LOG.debug(f"New proxy index: {proxy_index.row()}")
					self.__activity_table.selectRow(proxy_index.row())
					break

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

	def __start_refresh(self) -> None:
		"""
		Start monitoring the activity of PostgreSQL.
		"""
		if self.__refresh_task is not None:
			LOG.debug("Refresh in progress.")
			return

		LOG.debug("Start refresh.")
		self.__refresh_task = asyncio.create_task(self.__delayed_refresh())

	def __stop_refresh(self) -> None:
		"""
		Stop monitoring the activity of PostgreSQL.
		"""
		LOG.debug("Stop refresh.")
		task, self.__refresh_task = self.__refresh_task, None
		if task is not None:
			task.cancel()


# noinspection PyMethodOverriding
class ActivityTableModel(QAbstractTableModel):
	"""
	The :class:`ActivityTableModel` class provides the Qt table model for the
	PostgreSQL activity data.
	"""

	# Fix type hints.
	layoutChanged: SignalInstance

	def __init__(self, parent: QObject) -> None:
		"""
		Initializes the :class:`ActivityTableModel` instance.

		*parent* (:class:`QObject`) is the parent.
		"""
		super().__init__(parent)

		self.__column_fields: List[str] = list(ActivityRow._fields)
		"""
		*__column_fields* (:class:`list`) maps column index (:class:`int`) to column
		field name (:class:`str`).
		"""

		self.__column_titles: List[str] = [
			ACTIVITY_HEADER.get(__field, __field)
			for __field in ActivityRow._fields
		]
		"""
		*__column_titles* (:class:`list`) maps column index (:class:`int`) to column
		title (:class:`str`).
		"""

		self.__data: List[ActivityRow] = []
		"""
		*__data* (:class:`list` of :class:`ActivityRow`) is the activity data.
		"""

	def columnCount(self, index: QModelIndex) -> int:
		"""
		Get the number of columns.

		*index* (:class:`QModelIndex`) is the table index.

		Returns the column count (:class:`int`).
		"""
		return len(self.__column_titles)

	def data(self, index: QModelIndex, role: int) -> Optional[Any]:
		"""
		Get the datum for the role at the index.

		*index* (:class:`QModelIndex`) is the table index.

		*role* (:class:`int`) is the role enum value (:data:`Qt.DisplayRole`, etc.).

		Returns the datum for the index.
		"""
		#if not index.isValid():
		#	return None

		if role == Qt.DisplayRole:
			value = self.__data[index.row()][index.column()]
			if isinstance(value, datetime.datetime):
				return value.strftime("%Y-%m-%d %H:%M:%S %z")

			return value

	def get_data(self) -> List[ActivityRow]:
		"""
		Get the activity data.

		Returns the activity data (:class:`list` of :class:`ActivityRow`).
		"""
		return self.__data

	def get_fields(self) -> List[str]:
		"""
		Get the column field names.

		Returns the field names (:class:`list`) which maps column index
		(:class:`int`) to field name (:class:`str`).
		"""
		return self.__column_fields

	def headerData(
		self,
		column: int,
		orientation: Qt.Orientation,
		role: int,
	) -> Optional[Union[str]]:
		"""
		Get the header datum for the role at the section (column).

		*column* (:class:`int`) is the column index.

		*orientation* (:class:`Qt.Orientation`) is the header orientation.

		*role* (:class:`int`) is the role enum value (:data:`Qt.DisplayRole`, etc.).

		Returns the datum for the column (:class:`str` or :data:`None`).
		"""
		if orientation == Qt.Horizontal and role == Qt.DisplayRole:
			return self.__column_titles[column]

	def rowCount(self, index: QModelIndex) -> int:
		"""
		Get the number of rows.

		*index* (:class:`QModelIndex`) is the table index.

		Returns the row count (:class:`int`).
		"""
		return len(self.__data)

	def set_data(self, data: List[ActivityRow]) -> None:
		"""
		Set the activity data.

		*data* (:class:`list` of :class:`ActivityRow`) is the activity data.
		"""
		self.__data = data

		# Emit signal that the model rows have changed.
		self.layoutChanged.emit()


class SortProxyModel(QSortFilterProxyModel):
	"""
	The :class:`SortProxyModel` class provides sorting over the underlying table
	model.
	"""

	def __init__(self, parent: QObject) -> None:
		"""
		Initializes the :class:`ActivityTableModel` instance.

		*parent* (:class:`QObject`) is the parent.
		"""
		super().__init__(parent)
