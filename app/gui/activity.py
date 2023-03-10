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
	QItemSelection,
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
	asyncClose,
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

_ACTION_CANCEL_BACKEND = ObjectSel(QAction, "action_CancelBackend")
"""
The selector for the cancel backend action.
"""

_ACTION_CONNECT = ObjectSel(QAction, "action_Connect")
"""
The selector for the connect action.
"""

_ACTION_DISCONNECT = ObjectSel(QAction, "action_Disconnect")
"""
The selector for the disconnect action.
"""

_ACTION_KILL_BACKEND = ObjectSel(QAction, "action_KillBackend")
"""
The selector for the kill backend action.
"""

_ACTION_REFRESH = ObjectSel(QAction, "action_Refresh")
"""
The selector for the refresh action.
"""

LOG = logging.getLogger(__name__)
"""
The module logger.
"""

_TAB_WIDTH = 4
"""
The tab width (in spaces).
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

_MENU_SELECTED_BACKEND_ACTIONS = [
	_ACTION_CANCEL_BACKEND,
	_ACTION_KILL_BACKEND,
]
"""
The selectors for the menu actions that require a specific backend to be selected.
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

		self.__quit_future = cast(asyncio.Future, None)
		"""
		*__quit_future* (:class:`asyncio.Future`) is the future that will be
		canceled when the Qt application is about to quit.
		"""

		self.__refresh_interval = 10.0
		"""
		*__refresh_interval* (:class:`float`) is the refresh interval (in seconds).
		"""

		self.__refresh_task: Optional[asyncio.Task] = None
		"""
		*__refresh_task* (:class:`asyncio.Task` or :data:`None`) is the active
		refresh task.
		"""

		self.__refresh_timer: Optional[asyncio.TimerHandle] = None
		"""
		*__refresh_timer* (:class:`asyncio.TimerHandle` or :data:`None`) is the
		timer to start the next scheduled refresh.
		"""

		self.__status_bar = cast(QStatusBar, None)
		"""
		*__status_bar* (:class:`QStatusBar`) is the status bar widget.
		"""

		self.__window = cast(QMainWindow, None)
		"""
		*window* (:class:`QMainWindow`) is the activity window.
		"""

	def __cancel_refresh_task(self) -> None:
		"""
		Cancel the active refresh task.
		"""
		task, self.__refresh_task = self.__refresh_task, None
		if task is not None:
			LOG.debug("Refresh task canceled.")
			task.cancel()

	def __cancel_refresh_timer(self) -> None:
		"""
		Cancel the active refresh timer.
		"""
		timer, self.__refresh_timer = self.__refresh_timer, None
		if timer is not None:
			LOG.debug("Refresh timer canceled.")
			timer.cancel()

	def __clear_query_text(self) -> None:
		"""
		Clear the query text.
		"""
		self.__query_text.clear()

	def __clear_table(self) -> None:
		"""
		Clear the activity table.
		"""
		self.__activity_model.set_data([])

	def __disable_connected_actions(self) -> None:
		"""
		Disable the menu actions that require an active connection.
		"""
		self.__enable_actions(_MENU_CONNECTED_ACTIONS, False)

	def __disable_selected_backend_actions(self) -> None:
		"""
		Disable the menu actions that require a specific backend to be selected from
		the activity table.
		"""
		self.__enable_actions(_MENU_SELECTED_BACKEND_ACTIONS, False)

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
		pid: Optional[int] = None
		for proxy_index in self.__activity_table.selectionModel().selectedRows():
			LOG.debug(f"Get PID proxy index: {proxy_index.row()}")
			source_index = self.__activity_proxy_model.mapToSource(proxy_index)
			LOG.debug(f"Get PID source index: {source_index.row()}")
			activity_row = self.__activity_model.get_data()[source_index.row()]
			pid = activity_row.pid
			break

		LOG.debug(f"Selected PID: {pid}")
		return pid

	@asyncSlot()
	async def __on_action_cancel_backend(self) -> None:
		"""
		Called when the cancel backend action is triggered.
		"""
		LOG.debug("Cancel backend action.")
		pid = self.__get_selected_pid()
		if pid is not None:
			await self.__pg_activity.cancel_backend(pid)

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
			self.__disable_selected_backend_actions()
			self.__stop_refresh()
			self.__clear_table()
			self.__clear_query_text()
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
				self.__start_refresh(delay=False)

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
		self.__disable_selected_backend_actions()
		self.__stop_refresh()
		self.__clear_table()
		self.__clear_query_text()
		self.__set_title()
		await self.__disconnect_pg()

	@asyncSlot()
	async def __on_action_kill_backend(self) -> None:
		"""
		Called when the kill backend action is triggered.
		"""
		LOG.debug("Kill backend action.")
		pid = self.__get_selected_pid()
		if pid is not None:
			await self.__pg_activity.terminate_backend(pid)

	@asyncSlot()
	async def __on_action_refresh(self) -> None:
		"""
		Called when the refresh action is triggered.
		"""
		LOG.debug("Refresh action.")
		self.__start_refresh(delay=False)

	@asyncSlot()
	async def __on_activity_selection_changed(
		self,
		selected: QItemSelection,
		deselected: QItemSelection,
	) -> None:
		"""
		Called when the selection changes on the activity table.

		*selected* (:class:`QItemSelection`) is the newly selected items.

		*deselected* (:class:`QItemSelection`) is the previously selected items.
		"""
		# NOTICE: On refresh, this is called twice. First, to deselect a "-1" row.
		# Second, to select the active row.
		if selected.isEmpty():
			return

		pid = self.__get_selected_pid()

		LOG.debug(f"Activity selection changed: {pid}.")
		#LOG.debug("Activity selection changed: {pid} (S={s}, D={d}).".format(
		#	pid=pid,
		#	s=[r for isr in selected.toList() for r in range(isr.top(), isr.bottom() + 1)],
		#	d=[r for isr in deselected.toList() for r in range(isr.top(), isr.bottom() + 1)],
		#))

		if pid is not None:
			self.__enable_actions(_MENU_SELECTED_BACKEND_ACTIONS, True)
			await self.__update_query_text(pid)
		else:
			self.__disable_selected_backend_actions()

	def __on_app_about_to_quit(self) -> None:
		"""
		Called after the Qt application event loop stops, and is about to quit.
		"""
		LOG.debug("About to quit.")
		self.__quit_future.cancel()

	@asyncClose
	async def __on_app_close(self) -> None:
		"""
		Called before the Qt application event loop stops.
		"""
		LOG.debug("Close.")
		self.__stop_refresh()
		await self.__disconnect_pg()

	def __on_refresh_timer_done(self) -> None:
		"""
		Called when the refresh timer is done.
		"""
		LOG.debug("Refresh timer.")

		# Clear refresh timer.
		self.__refresh_timer = None

		# Start refresh task.
		self.__start_refresh_task()

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
			(_ACTION_CANCEL_BACKEND, self.__on_action_cancel_backend),
			(_ACTION_CONNECT, self.__on_action_connect),
			(_ACTION_DISCONNECT, self.__on_action_disconnect),
			(_ACTION_KILL_BACKEND, self.__on_action_kill_backend),
			(_ACTION_REFRESH, self.__on_action_refresh),
		]:
			action: QAction = self.__get_child(action_sel)
			action.triggered: SignalInstance  # noqa
			action.triggered.connect(callback)

		# Initialize actions.
		self.__disable_connected_actions()
		self.__disable_selected_backend_actions()

		# Get widgets.
		self.__activity_table: QTableView = self.__get_child(_WIDGET_ACTIVITY_TABLE)
		self.__status_bar = self.__get_child(_WIDGET_STATUS_BAR)
		self.__query_text: QTextEdit = self.__get_child(_WIDGET_QUERY_TEXT)

		# Setup query text.
		font_metrics = self.__query_text.fontMetrics()
		width = font_metrics.horizontalAdvance(" " * _TAB_WIDTH)
		self.__query_text.setTabStopDistance(width)

		# Setup activity table.
		self.__activity_model = ActivityTableModel(self.__activity_table)
		pid_column = self.__activity_model.get_fields().index("pid")

		self.__activity_proxy_model = QSortFilterProxyModel(self.__activity_table)
		self.__activity_proxy_model.setDynamicSortFilter(True)
		self.__activity_proxy_model.setSourceModel(self.__activity_model)

		self.__activity_table.setModel(self.__activity_proxy_model)
		self.__activity_table.sortByColumn(pid_column, Qt.AscendingOrder)

		sel_model = self.__activity_table.selectionModel()
		sel_model.selectionChanged: SignalInstance  # noqa
		sel_model.selectionChanged.connect(self.__on_activity_selection_changed)

		# Display window.
		self.__window.show()

		LOG.debug("Window created.")

		# Wait for application to close.
		# - NOTICE: When the Qt application is about to quit, the event loop will be
		#   stopped. The only appropriate way to finish the future is to cancel it.
		self.__quit_future = asyncio.get_running_loop().create_future()
		qapp = QApplication.instance()
		qapp.aboutToQuit: SignalInstance  # noqa
		qapp.aboutToQuit.connect(self.__on_app_about_to_quit)

		try:
			await self.__quit_future
		except asyncio.CancelledError:
			pass

		return 0

	def __populate_table(self, data: List[ActivityRow]) -> None:
		"""
		Populate the activity table.

		*data* (:class:`list` of :class:`ActivityRow`) is the activity data.
		"""
		LOG.debug(f"Populate table: {len(data)} rows.")

		# Get PID for the selected row.
		pid = self.__get_selected_pid()

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

	async def __refresh_activity(self) -> None:
		"""
		Refresh the PostgreSQL activity.
		"""
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
			self.__start_refresh(delay=True)

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

	def __start_refresh(self, delay: bool) -> None:
		"""
		Start monitoring the activity of PostgreSQL.

		*delay* (:class:`bool`) is whether to delay the next refresh.
		"""
		if self.__refresh_task is not None:
			LOG.debug("Refresh in progress.")
			return

		if delay:
			# Start refresh timer.
			self.__start_refresh_timer()

		else:
			# Cancel any active timer.
			self.__cancel_refresh_timer()

			# Start refresh task.
			self.__start_refresh_task()

	def __start_refresh_task(self) -> None:
		"""
		Start the refresh task.
		"""
		if self.__refresh_task is not None:
			LOG.debug("Refresh in progress.")
			return

		self.__refresh_task = asyncio.create_task(self.__refresh_activity())

	def __start_refresh_timer(self) -> None:
		"""
		Start the refresh timer to start the next scheduled refresh.
		"""
		if self.__refresh_timer is not None:
			# Refresh already scheduled, do nothing.
			LOG.debug("Refresh is scheduled.")

		else:
			# Schedule next refresh.
			LOG.debug("Schedule refresh.")
			self.__refresh_timer = asyncio.get_running_loop().call_later(
				self.__refresh_interval,
				self.__on_refresh_timer_done,
			)

	def __stop_refresh(self) -> None:
		"""
		Stop monitoring the activity of PostgreSQL.
		"""
		LOG.debug("Stop refresh.")
		self.__cancel_refresh_timer()
		self.__cancel_refresh_task()

	async def __update_query_text(self, pid: int) -> None:
		"""
		Update the query text.

		*pid* (:class:`int`) is the PID of the backend process.
		"""
		# Get query text.
		query = await self.__pg_activity.fetch_query(pid)
		if query is None:
			query = ""

		# Set query text.
		if query != self.__query_text.toPlainText():
			LOG.debug("Set query text.")
			self.__query_text.setPlainText(query)


# noinspection PyMethodOverriding
class ActivityTableModel(QAbstractTableModel):
	"""
	The :class:`ActivityTableModel` class provides the Qt table model for the
	PostgreSQL activity data.
	"""

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
		# Remove all old rows, and emit required signals.
		self.beginRemoveRows(QModelIndex(), 0, len(self.__data))
		self.__data = []
		self.endRemoveRows()

		# Insert new rows, and emit required signals.
		self.beginInsertRows(QModelIndex(), 0, len(data))
		self.__data = data
		self.endInsertRows()


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
