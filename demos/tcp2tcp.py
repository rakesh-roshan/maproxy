#!/usr/bin/env python
import argparse
import collections
import logging
import sys

import maproxy.proxyserver
import tornado.ioloop

AddressPort = collections.namedtuple('AddressPort', ['address', 'port'])

def parse_args():
    arg_parser = argparse.ArgumentParser(description='Starts a Proxy server')
    arg_parser.add_argument('--primary',
                            help='Primary Server',
                            type=address_port,
                            required=True)
    arg_parser.add_argument('--secondary',
                            help='Secondary Server',
                            type=address_port,
                            default=[],
                            action='append')
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
    if debug:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level, format=log_format)
    logging.debug('Logging Set to level DEBUG')


def address_port(s):
    address, port = s.split(':')
    if not address or not port:
        raise argparse.TypeError()
    return AddressPort(address=address, port=int(port))


def main():
    opts = parse_args()
    setup_logging(opts.verbose)

    server = maproxy.proxyserver.ProxyServer([opts.primary] + opts.secondary)
    server.listen(opts.listen_port, opts.listen_address)
    print("Primary: %s:%s -> %s->%s" % (opts.listen_address, opts.listen_port,
                                        opts.primary.address, opts.primary.port))
    for target in opts.secondary:
        print("%s:%s -> %s->%s" % (opts.listen_address, opts.listen_port,
                                   target.address, target.port))

    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    sys.exit(main())
