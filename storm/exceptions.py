#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#


class StormError(Exception):
    pass


class CompileError(StormError):
    pass

class NoTableError(CompileError):
    pass


class URIError(StormError):
    pass


class ClosedError(StormError):
    pass

class UnsupportedError(StormError):
    pass
    
class UnsupportedDatabaseError(UnsupportedError):
    pass


class StoreError(StormError):
    pass

class NoStoreError(StormError):
    pass

class WrongStoreError(StoreError):
    pass

class NotFlushedError(StoreError):
    pass

class OrderLoopError(StoreError):
    pass

class SetError(StoreError):
    pass

class NotOneError(StoreError):
    pass

class UnorderedError(StoreError):
    pass


class Error(StormError, StandardError):
    pass

class Warning(StormError, StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class InternalError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class DataError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass


def install_exceptions(module):
    for exception in (Error, Warning, DatabaseError, InternalError, 
                      OperationalError, ProgrammingError, IntegrityError,
                      DataError, NotSupportedError):
        module_exception = getattr(module, exception.__name__, None)
        if module_exception is not None:
            module_exception.__bases__ += (exception,)
