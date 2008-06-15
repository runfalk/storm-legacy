
_bound = []

def bind(object, depth=None):
    _bound.append((object, depth))

def enable():
    import psyco
    for object, depth in _bound:
        if depth is not None:
            psyco.bind(object, depth)
        else:
            psyco.bind(object)
