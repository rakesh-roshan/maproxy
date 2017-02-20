#!/usr/bin/env python

import tornado.ioloop
import maproxy.proxyserver



# HTTP->HTTP
# On your computer, browse to "http://127.0.0.1:81/" and you'll get http://www.google.com
server = maproxy.proxyserver.ProxyServer("127.0.0.1", 20001, "127.0.0.1", 20002)
server.listen(10001)
print("http://127.0.0.1:10001 -> http://www.google.com")
tornado.ioloop.IOLoop.instance().start()
