__all__ = [
    'has_twisted',
    'has_psycopg',
    ]

try:
    import twisted
except ImportError:
    has_twisted = False
else:
    has_twisted = True

try:
    import psycopg2
except ImportError:
    has_psycopg = False
else:
    has_psycopg = True
