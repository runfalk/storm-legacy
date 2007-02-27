from storm.properties import PropertyPublisherMeta


__all__ = ["Storm"]


class Storm(object):
    """An optional base class for objects stored in a Storm Store.

    It causes your subclasses to be associated with a Storm
    PropertyRegistry. It is necessary to use this if you want to
    specify References with strings.
    """
    __metaclass__ = PropertyPublisherMeta

