"""
This module defines the PostgreSQL activity model. The activity model is used to
get the activity information from PostgreSQL.
"""

import dataclasses
import datetime
import logging
from typing import (
	List,
	NamedTuple,
	Optional,
	Tuple)

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from PySide6.QtCore import (
	QThreadPool)

from app.threads import (
	Worker,
	WorkerFuture)

LOG = logging.getLogger(__name__)
"""
The module logger.
"""


class PostgresActivityModel(object):
	"""
	The :class:`PostgresActivityModel` class is used to get the activity
	information from PostgreSQL.
	"""

	def __init__(self, params: 'PostgresConnectionParams') -> None:
		"""
		Initializes the :class:`PostgresActivityModel` instance.

		*params* (:class:`PostgresConnectionParams`) contains the PostgreSQL
		connection parameters.
		"""
		# Create thread pool.
		pool = QThreadPool()
		pool.setMaxThreadCount(1)

		self.__connection: Optional[psycopg2.extensions.connection] = None
		"""
		*__connection* (:class:`psycopg2.extensions.connection` or :data:`None`) is
		the PostgreSQL connection.
		"""

		self.params: PostgresConnectionParams = params
		"""
		*__params* (:class:`PostgresConnectionParams`) contains the PostgreSQL
		connection parameters.
		"""

		self.__pool: QThreadPool = pool
		"""
		*__pool* (:class:`QThreadPool`) is the thread worker pool.
		"""

		self.__version: Optional[Tuple[int, ...]] = None
		"""
		*__version* (:class:`tuple` of :class:`int`) is the version of the
		PostgreSQL database.
		"""

	def cancel_query(self, pid: int) -> WorkerFuture:
		"""
		Cancel the query.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns a :class:`WorkerFuture`. On success, the emitted result will be
		whether the process was terminated (:class:`True`), or not (:class:`False`).
		"""
		LOG.debug("Cancel query.")
		conn = self.__connection
		worker = Worker(lambda: self.__cancel_query_work(conn, pid))
		future = worker.make_future()
		self.__pool.start(worker)
		return future

	@staticmethod
	def __cancel_query_work(
		conn: psycopg2.extensions.connection,
		pid: int,
	) -> bool:
		"""
		Run the cancel query.

		- WARNING: This must be run within a worker thread.

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns whether the process was terminated (:class:`True`), or not
		(:class:`False`).
		"""
		LOG.debug("Cancel query work.")
		cursor = conn.cursor()
		cursor.execute("""
			SELECT pg_cancel_backend(%(pid)s) AS success;
		""", {'pid': pid})
		row: _QueryCancelRow = cursor.fetchone()
		return row.success

	def close(self) -> WorkerFuture:
		"""
		Disconnect from PostgreSQL.

		Returns a :class:`WorkerFuture`. On success, the emitted result will be
		:data:`None`.
		"""
		LOG.debug("Close.")
		if self.__connection is not None:
			conn, self.__connection = self.__connection, None
			worker = Worker(lambda: self.__close_work(conn))
			future = worker.make_future()
			self.__pool.start(worker)

		else:
			future = WorkerFuture()
			future.set_result(None)

		return future

	@staticmethod
	def __close_work(conn: psycopg2.extensions.connection) -> None:
		"""
		Close the PostgreSQL connection.

		- WARNING: This must be run within a worker thread.

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection.
		"""
		conn.close()

	def connect(self) -> WorkerFuture:
		"""
		Connect to PostgreSQL.

		Returns a :class:`WorkerFuture`. On success, the emitted result will be
		:data:`None`.
		"""
		LOG.debug("Connect.")
		if self.__connection:
			self.close()

		# Connect to PostgreSQL.
		future = WorkerFuture()
		params = self.params
		worker = Worker(lambda: self.__connect_work(params))
		worker.signals.result.connect(lambda conn: self.__on_connect(conn, future))
		worker.signals.error.connect(future.set_error)
		self.__pool.start(worker)

		return future

	@staticmethod
	def __connect_work(
		params: 'PostgresConnectionParams',
	) -> psycopg2.extensions.connection:
		"""
		Connect to PostgreSQL.

		- WARNING: This must be run within a worker thread.

		Returns the connection (:class:`psycopg2.extensions.connection`).
		"""
		conn = psycopg2.connect(
			cursor_factory=psycopg2.extras.NamedTupleCursor,
			database=params.database,
			password=params.password,
			port=params.port,
			user=params.user,
		)
		conn.set_session(
			autocommit=True,
			readonly=True,
		)
		return conn

	def fetch_activity(self) -> WorkerFuture:
		"""
		Fetch the current activity from PostgreSQL.

		Returns a :class:`WorkerFuture`. On success, the emitted result will be a
		:class:`list` of :class:`ActivityRow`.
		"""
		LOG.debug("Fetch activity.")
		conn = self.__connection

		if self.__version >= (9, 2):
			worker = Worker(lambda: self.__fetch_activity_work_ge_92(conn))
		else:
			worker = Worker(lambda: self.__fetch_activity_work_le_91(conn))

		future = worker.make_future()
		self.__pool.start(worker)
		return future

	@staticmethod
	def __fetch_activity_work_ge_92(
		conn: psycopg2.extensions.connection,
	) -> List['ActivityRow']:
		"""
		Run the fetch activity query for PostgreSQL 9.2 and above.

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection.

		Returns the activity (:class:`list` of :class:`ActivityRow`).
		"""
		cursor = conn.cursor()
		cursor.execute("""
			SELECT
				application_name,
				backend_start,
				client_addr,
				nullif(client_hostname, '') AS client_hostname,
				client_port,
				datname,
				pid,
				query_start,
				state,
				state_change,
				usename,
				wait_event,
				xact_start
			FROM pg_stat_activity
			ORDER BY backend_start ASC;
		""")
		return cursor.fetchall()  # type: ignore

	@staticmethod
	def __fetch_activity_work_le_91(
		conn: psycopg2.extensions.connection,
	) -> List['ActivityRow']:
		"""
		Run the fetch activity query for PostgreSQL 9.1 and below.

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection.

		Returns the activity (:class:`list` of :class:`ActivityRow`).
		"""
		cursor = conn.cursor()
		cursor.execute("""
			SELECT
				application_name,
				backend_start,
				client_addr,
				client_hostname,
				client_port,
				datname,
				procpid AS pid,
				query_start,
				(CASE
					WHEN current_query = '<IDLE>'
						THEN 'idle'
					WHEN current_query = '<IDLE> in transaction'
						THEN 'idle in transaction'
					WHEN current_query = '<IDLE> in transaction (aborted)'
						THEN 'idle in transaction (aborted)'
					ELSE
						(CASE WHEN current_query LIKE '<IDLE>%'
							THEN current_query
							ELSE 'active'
						END)
				END) AS state,
				NULL::text AS state_change,
				usename,
				waiting as wait_event,
				xact_start
			FROM pg_stat_activity
			ORDER BY backend_start ASC;
		""")
		return cursor.fetchall()  # type: ignore

	@staticmethod
	def __get_version_work(
		conn: psycopg2.extensions.connection,
	) -> Tuple[int, ...]:
		"""
		Run the get version query.

		- WARNING: This must be run within a worker thread.

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection

		Returns the version of the PostgreSQL version (:class:`tuple` of
		:class:`int`).
		"""
		cursor = conn.cursor()
		cursor.execute("""
			SHOW server_version;
		""")
		row: _QueryVersionRow = cursor.fetchone()
		version_parts = row.server_version.split(".", 2)[:2]
		version = tuple(map(int, version_parts))
		return version

	def __on_connect(
		self,
		conn: psycopg2.extensions.connection,
		future: WorkerFuture,
	) -> None:
		"""
		Called after the PostgreSQL connection has been established.

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection.

		*future* (:class:`WorkerFuture`) is the future for completing the
		connection.
		"""
		LOG.debug(f"On connect {conn}.")
		assert self.__connection is None, "Already connected."
		self.__connection = conn

		# Get PostgreSQL version.
		worker = Worker(lambda: self.__get_version_work(conn))
		worker.signals.result.connect(lambda ver: self.__on_connect_version(ver, future))
		worker.signals.error.connect(future.set_error)
		self.__pool.start(worker)

	def __on_connect_version(
		self,
		version: Tuple[int, ...],
		future: WorkerFuture,
	) -> None:
		"""
		Called after the PostgreSQL version of been retrieved.

		- WARNING: This must be run within a worker thread.

		*version* (:class:`tuple` of :class:`int`) is the PostgreSQL version.

		*future* (:class:`WorkerFuture`) is the future for completing the
		connection.
		"""
		LOG.debug(f"On connect version {version}.")
		self.__version = version
		future.set_result(None)

	def terminate_query(self, pid: int) -> WorkerFuture:
		"""
		Terminate the query.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns a :class:`WorkerFuture`. On success, the emitted result will be
		whether the process was terminated (:class:`True`), or not (:class:`False`).
		"""
		LOG.debug("Terminate query.")
		conn = self.__connection
		worker = Worker(lambda: self.__terminate_query_work(conn, pid))
		future = worker.make_future()
		self.__pool.start(worker)
		return future

	@staticmethod
	def __terminate_query_work(
		conn: psycopg2.extensions.connection,
		pid: int,
	) -> bool:
		"""
		Run the terminate query.

		- WARNING: This must be run within a worker thread.

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns whether the process was terminated (:class:`True`), or not
		(:class:`False`).
		"""
		cursor = conn.cursor()
		cursor.execute("""
			SELECT pg_terminate_backend(%(pid)s) AS success;
		""", {'pid': pid})
		row: _QueryTerminateRow = cursor.fetchone()
		return row.success


class ActivityRow(NamedTuple):
	application_name: Optional[str]
	backend_start: datetime.datetime
	client_addr: Optional[str]
	client_hostname: Optional[str]
	client_port: Optional[int]
	datname: str
	pid: int
	query_start: datetime.datetime
	state: str
	state_change: datetime.datetime
	usename: str
	wait_event: Optional[str]
	xact_start: Optional[datetime.datetime]


@dataclasses.dataclass(frozen=True)
class PostgresConnectionParams(object):
	database: str
	host: str
	password: str
	port: int
	user: str


class _QueryCancelRow(NamedTuple):
	success: bool


class _QueryFetchRow(NamedTuple):
	query: str


class _QueryTerminateRow(NamedTuple):
	success: bool


class _QueryVersionRow(NamedTuple):
	server_version: str
