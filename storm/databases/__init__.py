

class Dummy(object):
    """Magic "infectious" class.
    
    This class simplifies nice errors on the creation of
    unsupported databases.
    """

    def __getattr__(self, name):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __add__(self, other):
        return self

dummy = Dummy()
