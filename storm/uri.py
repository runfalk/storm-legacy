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

    def __init__(self, scheme):
        self.scheme = scheme
        self.options = {}

    @classmethod
    def parse(cls, uri_str):
        try:
            scheme, rest = uri_str.split(":", 1)
        except ValueError:
            raise URIError("URI has no scheme")
        uri = cls(scheme)
        if "?" in rest:
            rest, options = rest.split("?", 1)
            for pair in options.split("&"):
                key, value = pair.split("=", 1)
                uri.options[unescape(key)] = unescape(value)
        if rest:
            if not rest.startswith("//"):
                uri.database = unescape(rest)
            else:
                rest = rest[2:]
                if "/" in rest:
                    rest, database = rest.split("/", 1)
                    uri.database = unescape(database)
                if "@" in rest:
                    userpass, hostport = rest.split("@", 1)
                else:
                    userpass = None
                    hostport = rest
                if hostport:
                    if ":" in hostport:
                        host, port = hostport.rsplit(":", 1)
                        uri.host = unescape(host)
                        if port:
                            uri.port = int(port)
                    else:
                        uri.host = unescape(hostport)
                if userpass is not None:
                    if ":" in userpass:
                        username, password = userpass.rsplit(":", 1)
                        uri.username = unescape(username)
                        uri.password = unescape(password)
                    else:
                        uri.username = unescape(userpass)
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
