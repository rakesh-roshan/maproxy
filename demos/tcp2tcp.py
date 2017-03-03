#!/usr/bin/env python
import argparse
import logging
import maproxy.proxyserver
import sys
import tornado.ioloop




def parse_args():
    arg_parser = argparse.ArgumentParser(description='Starts a Proxy server')
    arg_parser.add_argument('--server1',
                            help='Primary Server',
                            required=True)
    arg_parser.add_argument('--port1',
                            help='Primary Port',
                            type=int,
                            required=True)
    arg_parser.add_argument('--server2',
                            help='Secnodary Server',
                            required=True)
    arg_parser.add_argument('--port2',
                            help='Secnodary Port',
                            type=int,
                            required=True)
    arg_parser.add_argument('--listen-port',
                            help='Listen Port',
                            type=int,
                            required=True)
    arg_parser.add_argument('--listen-address',
                            help='Listen Address/IP')
    arg_parser.add_argument('-v', '--verbose',
                            action='store_true',
                            default=False)
    opts = arg_parser.parse_args()
    return opts

def setup_logging(debug=False):
    log_format = '%(asctime)s %(message)s'
    log_level = logging.INFO
    print(debug)
    if debug:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level, format=log_format)
    logging.debug('Logging Set to level DEBUG')

def main():
    opts = parse_args()
    setup_logging(opts.verbose)

    server = maproxy.proxyserver.ProxyServer(opts.server1,
                                             opts.port1,
                                             opts.server2,
                                             opts.port2)
    server.listen(opts.listen_port, opts.listen_address)
    print("%s:%s -> %s->%s" % (opts.listen_address, opts.listen_port, opts.server1, opts.port1))
    print("%s:%s -> %s->%s" % (opts.listen_address, opts.listen_port, opts.server2, opts.port2))
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    sys.exit(main())
