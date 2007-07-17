from zope.interface import Interface
from zope.schema import TextLine


class IStoreDirective(Interface):

    name = TextLine(title=u"Name", description=u"Store name")
    uri = TextLine(title=u"URI", description=u"Database URI")
