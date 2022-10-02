"""
This module defines the PostgreSQL activity model. The activity model is used to
get the activity information from PostgreSQL.
"""

import dataclasses
import logging
from typing import (
	Optional)

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from PySide6.QtCore import (
	QObject,
	QThreadPool,
	Signal,
	SignalInstance)

from app.threads import (
	Worker)

LOG = logging.getLogger(__name__)
"""
The module logger.
"""

# TODO: I LEFT OFF HERE! 2022-10-02
# - Impelement require methods.

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

		self.__pool: QThreadPool = pool
		"""
		*__pool* (:class:`QThreadPool`) is the thread worker pool.
		"""

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


@dataclasses.dataclass(frozen=True)
class PostgresConnectionParams(object):
	database: str
	host: str
	password: str
	port: int
	user: str
