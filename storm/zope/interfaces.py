from zope.interface import Interface


class ZStormError(Exception):
    pass


class IZStorm(Interface):
    pass


class ISQLObjectResultSet(Interface):

    def __getitem__(item):
       """List emulation."""

    def __getslice__(slice):
       """Slice support."""

    def __iter__():
       """List emulation."""

    def count():
       """Return the number of items in the result set."""

    def __nonzero__():
       """Boolean emulation."""

    def __contains__():
       """Support C{if FooObject in Foo.select(query)}."""

    def prejoin(prejoins):
       """Return a new L{SelectResults} with the list of attributes prejoined.

       @param prejoins: The list of attribute names to prejoin.
       """
