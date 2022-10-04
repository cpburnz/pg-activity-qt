"""
This module defines the PostgreSQL activity model. The activity model is used to
get the activity information from PostgreSQL.
"""

import dataclasses
import logging
from typing import (
	Any,
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

# TODO: Implement connection.


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

		self.__conn: Optional[psycopg2.extensions.connection] = None
		"""
		*__conn* (:class:`psycopg2.extensions.connection` or :data:`None`) is the
		PostgreSQL connection.
		"""

		self.__params: PostgresConnectionParams = params
		"""
		*__params* (:class:`PostgresConnectionParams`) contains the PostgreSQL
		connection parameters.
		"""

		self.__pg_version: Optional[Tuple[int, int]] = None
		"""
		*__pg_version* (:class:`tuple` of :class:`int`) is the version of the
		PostgreSQL database.
		"""

		self.__pool: QThreadPool = pool
		"""
		*__pool* (:class:`QThreadPool`) is the thread worker pool.
		"""

	def cancel_query(self, pid: int) -> WorkerFuture:
		"""
		Cancel the query.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns a :class:`WorkerFuture`. On success, the emitted result will be
		whether the process was terminated (:class:`True`), or not (:class:`False`).
		"""
		LOG.debug("Cancel query.")
		conn = self.__conn
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

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns whether the process was terminated (:class:`True`), or not
		(:class:`False`).
		"""
		cursor = conn.cursor()
		cursor.execute("""
			SELECT pg_cancel_backend(%(pid)s) AS success;
		""", {'pid': pid})
		row: _QueryCancelRow = cursor.fetchone()
		return row.success

	def close(self) -> None:
		"""
		Disconnect from PostgreSQL.
		"""
		LOG.debug("Close.")
		if self.__conn is not None:
			conn, self.__conn = self.__conn, None
			worker = Worker(lambda: self.__close_work(conn))
			self.__pool.start(worker)

	@staticmethod
	def __close_work(conn: psycopg2.extensions.connection) -> None:
		"""
		Close the PostgreSQL connection.

		*conn* (:class:`psycopg2.extensions.connection`) is the PostgreSQL
		connection.
		"""
		conn.rollback()
		conn.close()

	def fetch_activity(self) -> WorkerFuture:
		"""
		Fetch the current activity from PostgreSQL.

		Returns a :class:`WorkerFuture`. On success, the emitted result will be a
		:class:`list` of :class:`ActivityRow`.
		"""
		LOG.debug("Fetch activity.")
		conn = self.__conn

		if self.__pg_version >= (9, 2):
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
				waiting,
				xact_start
			FROM pg_stat_activity;
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
				waiting,
				xact_start
			FROM pg_stat_activity;
		""")
		return cursor.fetchall()  # type: ignore

	def terminate_query(self, pid: int) -> WorkerFuture:
		"""
		Terminate the query.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns a :class:`WorkerFuture`. On success, the emitted result will be
		whether the process was terminated (:class:`True`), or not (:class:`False`).
		"""
		LOG.debug("Terminate query.")
		conn = self.__conn
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
	# TODO: Type hint these.
	application_name: Any  # TODO
	backend_start: Any  # TODO
	client_addr: Any  # TODO
	client_hostname: Optional[str]
	client_port: int
	datname: str
	pid: int
	query_start: Any  # TODO
	state: Any  # TODO
	state_change: Any  # TODO
	usename: Any  # TODO
	waiting: Any  # TODO
	xact_start: Any  # TODO


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
