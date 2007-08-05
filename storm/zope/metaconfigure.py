from zope import component

from storm.zope.interfaces import IZStorm


def set_default_uri(name, uri):
    """Register C{uri} as the default URI for stores called C{name}."""
    zstorm = component.getUtility(IZStorm)
    zstorm.set_default_uri(name, uri)


def store(_context, name, uri):
    _context.action(discriminator=("store", name),
                    callable=set_default_uri,
                    args=(name, uri))

