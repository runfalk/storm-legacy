__metaclass__ = type

__all__ = [
    'DatabaseWrapper', 'DatabaseError', 'IntegrityError',
    ]

from django.conf import settings
import transaction

from storm.django.stores import get_store, get_store_uri
from storm.exceptions import DatabaseError, IntegrityError


class StormDatabaseWrapperMixin(object):

    _store = None

    def _get_connection(self):
        if self._store is None:
            self._store = get_store(settings.DATABASE_NAME)
        return self._store._connection._raw_connection

    def _set_connection(self, connection):
        # Ignore attempts to set the connection.
        pass

    connection = property(_get_connection, _set_connection)

    def _commit(self):
        #print "commit"
        transaction.commit()

    def _rollback(self):
        #print "rollback"
        transaction.abort()

    def close(self):
        # As we are borrowing Storm's connection, we shouldn't close
        # it behind Storm's back.
        self._store = None


def DatabaseWrapper(*args, **kwargs):
    store_uri = get_store_uri(settings.DATABASE_NAME)

    # Create a DatabaseWrapper class that uses an underlying Storm
    # connection.
    if store_uri.startswith('postgres:'):
        from django.db.backends.postgresql_psycopg2.base import (
            DatabaseWrapper as PostgresDatabaseWrapper)
        class DatabaseWrapper(StormDatabaseWrapperMixin, PostgresDatabaseWrapper):
            pass
    elif store_uri.startswith('mysql:'):
        from django.db.backends.mysql.base import (
            DatabaseWrapper as MySQLDatabaseWrapper)
        class DatabaseWrapper(StormDatabaseWrapperMixin, MySQLDatabaseWrapper):
            pass
    else:
        assert False, (
            "Unsupported database backend: %s" % store_uri)

    return DatabaseWrapper(*args, **kwargs)
