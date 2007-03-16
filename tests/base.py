import weakref
import gc

from storm.properties import Property, PropertyPublisherMeta
from storm.info import get_info
from storm.base import *

from tests.helper import TestHelper


class BaseTest(TestHelper):

    def test_metaclass(self):
        class Class(Storm):
            __storm_table__ = "table_name"
            prop = Property(primary=True)
        self.assertEquals(type(Class), PropertyPublisherMeta)

    def test_class_is_collectable(self):
        class Class(Storm):
            __storm_table__ = "table_name"
            prop = Property(primary=True)
        obj = Class()
        get_info(obj) # Build all wanted meta-information.
        obj_ref = weakref.ref(obj)
        del obj
        gc.collect()
        self.assertEquals(obj_ref(), None)
