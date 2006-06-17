from storm.uri import URI, URIError

from tests.helper import TestHelper


class URITest(TestHelper):

    def test_constructor(self):
        uri = URI("scheme")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.options, {})
        self.assertEquals(uri.username, None)
        self.assertEquals(uri.password, None)
        self.assertEquals(uri.host, None)
        self.assertEquals(uri.port, None)
        self.assertEquals(uri.database, None)

    def test_parse_no_colon(self):
        self.assertRaises(URIError, URI.parse, "scheme")

    def test_parse_just_colon(self):
        uri = URI.parse("scheme:")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.database, None)


    def test_parse_just_relative_database(self):
        uri = URI.parse("scheme:d%61ta/base")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.database, "data/base")

    def test_parse_just_absolute_database(self):
        uri = URI.parse("scheme:/d%61ta/base")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.database, "/data/base")

    def test_parse_host(self):
        uri = URI.parse("scheme://ho%73t")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.host, "host")

    def test_parse_username(self):
        uri = URI.parse("scheme://user%6eame@")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.username, "username")
        self.assertEquals(uri.host, None)

    def test_parse_username_password(self):
        uri = URI.parse("scheme://user%6eame:pass%77ord@")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.username, "username")
        self.assertEquals(uri.password, "password")
        self.assertEquals(uri.host, None)

    def test_parse_username_host(self):
        uri = URI.parse("scheme://user%6eame@ho%73t")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.username, "username")
        self.assertEquals(uri.host, "host")

    def test_parse_username_password_host(self):
        uri = URI.parse("scheme://user%6eame:pass%77ord@ho%73t")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.username, "username")
        self.assertEquals(uri.password, "password")
        self.assertEquals(uri.host, "host")

    def test_parse_username_password_host_port(self):
        uri = URI.parse("scheme://user%6eame:pass%77ord@ho%73t:1234")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.username, "username")
        self.assertEquals(uri.password, "password")
        self.assertEquals(uri.host, "host")
        self.assertEquals(uri.port, 1234)

    def test_parse_username_password_host_empty_port(self):
        uri = URI.parse("scheme://user%6eame:pass%77ord@ho%73t:")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.username, "username")
        self.assertEquals(uri.password, "password")
        self.assertEquals(uri.host, "host")
        self.assertEquals(uri.port, None)

    def test_parse_username_password_host_port_database(self):
        uri = URI.parse("scheme://user%6eame:pass%77ord@ho%73t:1234/d%61tabase")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.username, "username")
        self.assertEquals(uri.password, "password")
        self.assertEquals(uri.host, "host")
        self.assertEquals(uri.port, 1234)
        self.assertEquals(uri.database, "database")

    def test_parse_options(self):
        uri = URI.parse("scheme:?a%62c=d%65f&ghi=jkl")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.host, None)
        self.assertEquals(uri.database, None)
        self.assertEquals(uri.options, {"abc": "def", "ghi": "jkl"})

    def test_parse_host_options(self):
        uri = URI.parse("scheme://ho%73t?a%62c=d%65f&ghi=jkl")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.host, "host")
        self.assertEquals(uri.database, None)
        self.assertEquals(uri.options, {"abc": "def", "ghi": "jkl"})

    def test_parse_host_database_options(self):
        uri = URI.parse("scheme://ho%73t/d%61tabase?a%62c=d%65f&ghi=jkl")
        self.assertEquals(uri.scheme, "scheme")
        self.assertEquals(uri.host, "host")
        self.assertEquals(uri.database, "database")
        self.assertEquals(uri.options, {"abc": "def", "ghi": "jkl"})

    def test_copy(self):
        uri = URI.parse("scheme:db")
        uri_copy = uri.copy()
        self.assertTrue(uri_copy is not uri)
        self.assertTrue(uri_copy.__dict__ == uri.__dict__)
        self.assertTrue(uri_copy.options is not uri.options)

