from storm.event import EventSystem

from tests.helper import TestHelper


marker = object()


class EventTest(TestHelper):

    def setUp(self):
        TestHelper.setUp(self)
        self.event = EventSystem(42)

    def test_hook_unhook_emit(self):
        called1 = []
        called2 = []
        def callback1(owner, arg1, arg2):
            called1.append((owner, arg1, arg2))
        def callback2(owner, arg1, arg2, data1, data2):
            called2.append((owner, arg1, arg2, data1, data2))

        self.event.hook("one", callback1)
        self.event.hook("one", callback1)
        self.event.hook("one", callback2, 10, 20)
        self.event.hook("two", callback2, 10, 20)
        self.event.hook("two", callback2, 10, 20)
        self.event.hook("two", callback2, 30, 40)
        self.event.hook("three", callback1)

        self.event.emit("one", 1, 2)
        self.event.emit("two", 3, 4)
        self.event.unhook("two", callback2, 10, 20)
        self.event.emit("two", 3, 4)
        self.event.emit("three", 5, 6)

        self.assertEquals(sorted(called1), [
                          (42, 1, 2),
                          (42, 5, 6),
                         ])
        self.assertEquals(sorted(called2), [
                          (42, 1, 2, 10, 20),
                          (42, 3, 4, 10, 20),
                          (42, 3, 4, 30, 40),
                          (42, 3, 4, 30, 40),
                         ])

    def test_unhook_by_returning_false(self):
        called = []
        def callback(owner):
            called.append(owner)
            return len(called) < 2

        self.event.hook("event", callback)

        self.event.emit("event")
        self.event.emit("event")
        self.event.emit("event")
        self.event.emit("event")

        self.assertEquals(called, [42, 42])

    def test_save_restore(self):
        called1 = []
        called2 = []
        def callback1(owner, arg):
            called1.append(arg)
        def callback2(owner, arg):
            called2.append(arg)

        self.event.hook("event", callback1)
        self.event.save()
        self.event.hook("event", callback2)
        self.event.unhook("event", callback1)
        self.event.emit("event", 1)
        self.event.emit("event", 2)
        self.event.restore()
        self.event.emit("event", 3)
        self.event.emit("event", 4)
        self.event.hook("event", callback2)
        self.event.unhook("event", callback1)
        self.event.emit("event", 5)
        self.event.emit("event", 6)
        self.event.restore()
        self.event.emit("event", 7)
        self.event.emit("event", 8)

        self.assertEquals(called1, [3, 4, 7, 8])
        self.assertEquals(called2, [1, 2, 5, 6])
