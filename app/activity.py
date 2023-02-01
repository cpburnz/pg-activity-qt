"""
This module defines the PostgreSQL activity manager. The activity manager is
used to get the activity information from PostgreSQL.
"""

import collections
import dataclasses
import datetime
import logging
from typing import (
	List,
	NamedTuple,
	Optional,
	Tuple)

import aiopg
import psycopg2.extras

ACTIVITY_HEADER = collections.OrderedDict([
	('pid', "PID"),
	('application_name', "Application"),
	('datname', "Database"),
	('backend_start', "Backend Start"),
	('client_addr', "Client Address"),
	('client_hostname', "Client Host"),
	('client_port', "Client Port"),
	('query_start', "Query Start"),
	('state', "State"),
	('state_change', "State Change"),
	('usename', "User Name"),
	('wait_event', "Wait Event"),
	('xact_start', "Transaction Start"),
])
"""
Maps activity field name (:class:`str`) to header name (:class:`str`).
"""

_APP_NAME = "PostgreSQL Activity"
"""
The application name to use when connecting to PostgreSQL.
"""

LOG = logging.getLogger(__name__)
"""
The module logger.
"""


class PostgresActivityManager(object):
	"""
	The :class:`PostgresActivityManager` class is used to manage the connection to
	PostgreSQL and monitor the database activity.
	"""

	def __init__(self, params: 'PostgresConnectionParams') -> None:
		"""
		Initializes the :class:`PostgresActivityManager` instance.

		*params* (:class:`PostgresConnectionParams`) contains the PostgreSQL
		connection parameters.
		"""

		self.__connection: Optional[aiopg.Connection] = None
		"""
		*__connection* (:class:`aiopg.Connection` or :data:`None`) is the PostgreSQL
		connection.
		"""

		self.params: PostgresConnectionParams = params
		"""
		*__params* (:class:`PostgresConnectionParams`) contains the PostgreSQL
		connection parameters.
		"""

		self.__version: Optional[Tuple[int, ...]] = None
		"""
		*__version* (:class:`tuple` of :class:`int`) is the version of the
		PostgreSQL database.
		"""

	async def cancel_backend(self, pid: int) -> bool:
		"""
		Cancel the backend process.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns whether the process was terminated (:class:`True`), or not
		(:class:`False`).
		"""
		LOG.debug(f"Cancel backend {pid}.")
		async with self.__connection.cursor() as cursor:
			await cursor.execute("""
				SELECT pg_cancel_backend(%(pid)s) AS success;
			""", {'pid': pid})
			row: _CancelBackendRow = await cursor.fetchone()
			return row.success

	async def close(self) -> None:
		"""
		Disconnect from PostgreSQL.
		"""
		LOG.debug("Close.")
		conn, self.__connection = self.__connection, None
		if conn is not None:
			await conn.close()

	async def connect(self) -> None:
		"""
		Connect to PostgreSQL.
		"""
		LOG.debug("Connect.")
		if self.__connection is not None:
			await self.close()

		# Connect to PostgreSQL.
		self.__connection = await aiopg.connect(
			application_name=_APP_NAME,
			cursor_factory=psycopg2.extras.NamedTupleCursor,
			database=self.params.database,
			host=self.params.host,
			password=self.params.password,
			port=self.params.port,
			user=self.params.user,
		)

		# Get PostgreSQL version.
		self.__version = await self.__get_version()

	async def fetch_activity(self) -> List['ActivityRow']:
		"""
		Fetch the current activity from PostgreSQL.

		Returns a the activity (:class:`list` of :class:`ActivityRow`).
		"""
		LOG.debug("Fetch activity.")
		if self.__version >= (9, 6):
			return await self.__fetch_activity_ge_96()
		elif self.__version >= (9, 2):
			return await self.__fetch_activity_ge_92()
		else:
			return await self.__fetch_activity_le_91()

	async def __fetch_activity_ge_92(self) -> List['ActivityRow']:
		"""
		Run the fetch activity query for PostgreSQL 9.2 and above.

		Returns the activity (:class:`list` of :class:`ActivityRow`).
		"""
		LOG.debug("Fetch activity (v>=9.2).")
		async with self.__connection.cursor() as cursor:
			await cursor.execute("""
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
					(CASE WHEN waiting
						THEN 'Waiting'
					END) AS wait_event,
					xact_start
				FROM pg_stat_activity
				ORDER BY backend_start ASC;
			""")
			return await cursor.fetchall()

	async def __fetch_activity_ge_96(self) -> List['ActivityRow']:
		"""
		Run the fetch activity query for PostgreSQL 9.6 and above.

		Returns the activity (:class:`list` of :class:`ActivityRow`).
		"""
		LOG.debug("Fetch activity (v>=9.6).")
		async with self.__connection.cursor() as cursor:
			await cursor.execute("""
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
			return await cursor.fetchall()

	async def __fetch_activity_le_91(self) -> List['ActivityRow']:
		"""
		Run the fetch activity query for PostgreSQL 9.1 and below.

		Returns the activity (:class:`list` of :class:`ActivityRow`).
		"""
		LOG.debug("Fetch activity (v<=9.1).")
		async with self.__connection.cursor() as cursor:
			await cursor.execute("""
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
					(CASE WHEN waiting
						THEN 'Waiting'
					END) AS wait_event,
					xact_start
				FROM pg_stat_activity
				ORDER BY backend_start ASC;
			""")
			return await cursor.fetchall()

	async def fetch_query(self, pid: int) -> Optional[str]:
		"""
		Run the fetch "query" query.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns the query (:class:`str` or :data:`None`).
		"""
		LOG.debug(f"Fetch query {pid}.")
		if self.__version >= (9, 2):
			return await self.__fetch_query_ge_92(pid)
		else:
			return await self.__fetch_query_le_91(pid)

	async def __fetch_query_ge_92(self, pid: int) -> Optional[str]:
		"""
		Run the fetch "query" query for PostgreSQL 9.2 and above.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns the query (:class:`str` or :data:`None`).
		"""
		LOG.debug(f"Fetch query {pid} (v>=9.2).")
		async with self.__connection.cursor() as cursor:
			await cursor.execute("""
				SELECT
					(CASE WHEN state = 'active'
						THEN query
						ELSE NULL
					END) AS query
				FROM pg_stat_activity
				WHERE pid = %(pid)s;
			""", {'pid': pid})
			row: _FetchQueryRow = await cursor.fetchone()
			return row.query

	async def __fetch_query_le_91(self, pid: int) -> Optional[str]:
		"""
		Run the fetch "query" query for PostgreSQL 9.1 and below.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns the query (:class:`str` or :data:`None`).
		"""
		LOG.debug(f"Fetch query {pid} (v<=9.1).")
		async with self.__connection.cursor() as cursor:
			await cursor.execute("""
				SELECT
					(CASE WHEN current_query LIKE '<IDLE>%'
						THEN NULL
						ELSE current_query
					END) AS query
				FROM pg_stat_activity
				WHERE procpid = %(pid)s;
			""", {'pid': pid})
			row: _FetchQueryRow = await cursor.fetchone()
			return row.query

	async def __get_version(self) -> Tuple[int, ...]:
		"""
		Run the get version query.

		Returns the version of the PostgreSQL version (:class:`tuple` of
		:class:`int`).
		"""
		LOG.debug("Get version.")
		async with self.__connection.cursor() as cursor:
			await cursor.execute("""
				SHOW server_version;
			""")
			row: _GetVersionRow = await cursor.fetchone()
			LOG.debug(f"VERSION: {row.server_version}")
			version_parts = row.server_version.split(" ", 1)[0].split(".", 2)[:2]
			version = tuple(map(int, version_parts))
			return version

	async def terminate_backend(self, pid: int) -> bool:
		"""
		Terminate the backend process.

		*pid* (:class:`int`) is the PID of the backend process.

		Returns whether the process was terminated (:class:`True`), or not
		(:class:`False`).
		"""
		LOG.debug(f"Terminate backend {pid}.")
		async with self.__connection.cursor() as cursor:
			await cursor.execute("""
				SELECT pg_terminate_backend(%(pid)s) AS success;
			""", {'pid': pid})
			row: _TerminateBackendRow = await cursor.fetchone()
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


class _CancelBackendRow(NamedTuple):
	success: bool


class _FetchQueryRow(NamedTuple):
	query: Optional[str]


class _GetVersionRow(NamedTuple):
	server_version: str


@dataclasses.dataclass(frozen=True)
class PostgresConnectionParams(object):
	database: str
	host: str
	password: str
	port: int
	user: str


class _TerminateBackendRow(NamedTuple):
	success: bool
