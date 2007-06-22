#
# Copyright (c) 2006 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# <license text goes here>
#
from storm.exceptions import URIError


class URI(object):

    username = None
    password = None
    host = None
    port = None
    database = None

    def __init__(self, uri_str):
        try:
            self.scheme, rest = uri_str.split(":", 1)
        except ValueError:
            raise URIError("URI has no scheme: %s" % repr(uri_str))

        self.options = {}

        if "?" in rest:
            rest, options = rest.split("?", 1)
            for pair in options.split("&"):
                key, value = pair.split("=", 1)
                self.options[unescape(key)] = unescape(value)
        if rest:
            if not rest.startswith("//"):
                self.database = unescape(rest)
            else:
                rest = rest[2:]
                if "/" in rest:
                    rest, database = rest.split("/", 1)
                    self.database = unescape(database)
                if "@" in rest:
                    userpass, hostport = rest.split("@", 1)
                else:
                    userpass = None
                    hostport = rest
                if hostport:
                    if ":" in hostport:
                        host, port = hostport.rsplit(":", 1)
                        self.host = unescape(host)
                        if port:
                            self.port = int(port)
                    else:
                        self.host = unescape(hostport)
                if userpass is not None:
                    if ":" in userpass:
                        username, password = userpass.rsplit(":", 1)
                        self.username = unescape(username)
                        self.password = unescape(password)
                    else:
                        self.username = unescape(userpass)

    def copy(self):
        uri = object.__new__(self.__class__)
        uri.__dict__.update(self.__dict__)
        uri.options = self.options.copy()
        return uri


def unescape(s):
    if "%" not in s:
        return s
    i = 0
    j = s.find("%")
    r = []
    while j != -1:
        r.append(s[i:j])
        i = j+3
        r.append(chr(int(s[j+1:i], 16)))
        j = s.find("%", i)
    r.append(s[i:])
    return "".join(r)
