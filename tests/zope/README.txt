
Introduction
------------

The storm.zope package contains the ZStorm utility which provides
seamless integration between Storm and Zope 3's transaction system.
Setting up ZStorm is quite easy.  In most cases, you want to include
storm/zope/configure.zcml in your application.  For the purposes of
this doctest we'll register ZStorm manually.

  >>> from zope.component import provideUtility, getUtility
  >>> import transaction
  >>> from storm.zope.interfaces import IZStorm
  >>> from storm.zope.zstorm import global_zstorm

  >>> provideUtility(global_zstorm, IZStorm)
  >>> zstorm = getUtility(IZStorm)
  >>> zstorm
  <storm.zope.zstorm.ZStorm object at ...>

Awesome, now that the utility is in place we can start to use it!


Getting stores
--------------

The ZStorm utility allows us work with named stores.

  >>> zstorm.set_default_uri("test", "sqlite:")

Setting a default URI for stores isn't strictly required.  We could
pass it as the second argument to zstorm.get.  Providing a default URI
makes it possible to use zstorm.get more easily; this is especially
handy when multiple threads are used as we'll see further on.

  >>> store = zstorm.get("test")
  >>> store
  <storm.store.Store object at ...>

ZStorm has automatically created a store instance for us.  If we ask
for a store by name again, we should get the same instance.

  >>> same_store = zstorm.get("test")
  >>> same_store is store
  True

The stores provided by ZStorm are per-thread.  If we ask for the named
store in a different thread we should get a different instance.

  >>> import threading

  >>> thread_store = None
  >>> def get_thread_store():
  ...     thread_store = zstorm.get("test")

  >>> thread = threading.Thread(target=get_thread_store)
  >>> thread.start()
  >>> thread.join()
  >>> thread_store is not store
  True

Great!  ZStorm abstracts away the process of creating and managing
named stores.  Let's move on and use the stores with Zope's
transaction system.


Committing transactions
-----------------------

The primary purpose of ZStorm is to integrate with Zope's transaction
system.  Let's create a schema so we can play with some real data and
see how it works.

  >>> result = store.execute("""
  ...     CREATE TABLE person (
  ...         id INTEGER PRIMARY KEY,
  ...         name TEXT)
  ... """)
  >>> store.commit()

We'll need a Person class to use with this database.

  >>> from storm.locals import Storm, Int, Unicode

  >>> class Person(Storm):
  ...
  ...     __storm_table__ = "person"
  ...
  ...     id = Int(primary=True)
  ...     name = Unicode()
  ...
  ...     def __init__(self, name):
  ...         self.name = name

Great!  Let's try it out.

  >>> person = Person(u"John Doe")
  >>> store.add(person)
  <Person object at ...>
  >>> transaction.commit()

Notice that we're not using store.commit directly; we're using Zope's
transaction system.  Let's make sure it worked.

  >>> store.rollback()
  >>> same_person = store.find(Person).one()
  >>> same_person is person
  True

Awesome!


Aborting transactions
---------------------

Let's make sure aborting transactions works, too.

  >>> store.add(Person(u"Imposter!"))
  <Person object at ...>

At this point a store.find should return the new object.

  >>> sorted([person.name for person in store.find(Person)])
  [u'Imposter!', u'John Doe']

All this means is that the data has been flushed to the database; it's
still not committed.  If we abort the transaction the new Person
object should disappear.

  >>> transaction.abort()
  >>> [person.name for person in store.find(Person)]
  [u'John Doe']

Excellent!  As you can see, ZStorm makes working with SQL databases
and Zope 3 very natural.


ZCML
----

In the examples above we setup our stores manually.  In many cases,
setting up named stores via ZCML directives is more desirable.  Add a
stanza similar to the following to your ZCML configuration to setup a
named store.

  <store name="test" uri="sqlite:" />

With that in place getUtility(IZStorm).get("test") will return the
store named "test".


# vim:ts=4:sw=4:et
