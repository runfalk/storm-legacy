#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
import pytest

from storm.compat import ustr
from storm.uri import URI, URIError


def test_parse_defaults():
    uri = URI("scheme:")
    assert uri.scheme == "scheme"
    assert uri.options == {}
    assert uri.username == None
    assert uri.password == None
    assert uri.host == None
    assert uri.port == None
    assert uri.database == None


def test_parse_no_colon():
    with pytest.raises(URIError):
        URI("scheme")


def test_parse_just_colon():
    uri = URI("scheme:")
    assert uri.scheme == "scheme"
    assert uri.database == None


def test_parse_just_relative_database():
    uri = URI("scheme:d%61ta/base")
    assert uri.scheme == "scheme"
    assert uri.database == "data/base"


def test_parse_just_absolute_database():
    uri = URI("scheme:/d%61ta/base")
    assert uri.scheme == "scheme"
    assert uri.database == "/data/base"


def test_parse_host():
    uri = URI("scheme://ho%73t")
    assert uri.scheme == "scheme"
    assert uri.host == "host"


def test_parse_username():
    uri = URI("scheme://user%6eame@")
    assert uri.scheme == "scheme"
    assert uri.username == "username"
    assert uri.host == None


def test_parse_username_password():
    uri = URI("scheme://user%6eame:pass%77ord@")
    assert uri.scheme == "scheme"
    assert uri.username == "username"
    assert uri.password == "password"
    assert uri.host == None


def test_parse_username_host():
    uri = URI("scheme://user%6eame@ho%73t")
    assert uri.scheme == "scheme"
    assert uri.username == "username"
    assert uri.host == "host"


def test_parse_username_password_host():
    uri = URI("scheme://user%6eame:pass%77ord@ho%73t")
    assert uri.scheme == "scheme"
    assert uri.username == "username"
    assert uri.password == "password"
    assert uri.host == "host"


def test_parse_username_password_host_port():
    uri = URI("scheme://user%6eame:pass%77ord@ho%73t:1234")
    assert uri.scheme == "scheme"
    assert uri.username == "username"
    assert uri.password == "password"
    assert uri.host == "host"
    assert uri.port == 1234


def test_parse_username_password_host_empty_port():
    uri = URI("scheme://user%6eame:pass%77ord@ho%73t:")
    assert uri.scheme == "scheme"
    assert uri.username == "username"
    assert uri.password == "password"
    assert uri.host == "host"
    assert uri.port == None


def test_parse_username_password_host_port_database():
    uri = URI("scheme://user%6eame:pass%77ord@ho%73t:1234/d%61tabase")
    assert uri.scheme == "scheme"
    assert uri.username == "username"
    assert uri.password == "password"
    assert uri.host == "host"
    assert uri.port == 1234
    assert uri.database == "database"


def test_parse_username_password_database():
    uri = URI("scheme://user%6eame:pass%77ord@/d%61tabase")
    assert uri.scheme == "scheme"
    assert uri.username == "username"
    assert uri.password == "password"
    assert uri.host == None
    assert uri.port == None
    assert uri.database == "database"


def test_parse_options():
    uri = URI("scheme:?a%62c=d%65f&ghi=jkl")
    assert uri.scheme == "scheme"
    assert uri.host == None
    assert uri.database == None
    assert uri.options == {"abc": "def", "ghi": "jkl"}


def test_parse_host_options():
    uri = URI("scheme://ho%73t?a%62c=d%65f&ghi=jkl")
    assert uri.scheme == "scheme"
    assert uri.host == "host"
    assert uri.database == None
    assert uri.options == {"abc": "def", "ghi": "jkl"}


def test_parse_host_database_options():
    uri = URI("scheme://ho%73t/d%61tabase?a%62c=d%65f&ghi=jkl")
    assert uri.scheme == "scheme"
    assert uri.host == "host"
    assert uri.database == "database"
    assert uri.options == {"abc": "def", "ghi": "jkl"}


def test_copy():
    uri = URI("scheme:///db?opt=value")
    uri_copy = uri.copy()
    assert uri_copy is not uri
    assert uri_copy.__dict__ == uri.__dict__
    assert uri_copy.options is not uri.options


@pytest.mark.parametrize("uri", [
    u"scheme://us%2Fer:pa%2Fss@ho%2Fst:0/d%3Fb?a%2Fb=c%2Fd&ghi=jkl",
    u"scheme:/a/b/c",
    u"scheme:",
    u"scheme://username@/",
    u"scheme://:password@/",
    u"scheme://:0/",
    u"scheme://host/",
    u"scheme:db",
    u"scheme:?a=b",
])
def test_str_and_back(uri):
    assert ustr(URI(uri)) == uri
