#!/usr/bin/env python

import threading
import logging
import socket

import tornado
import maproxy.proxyserver

from functools import partial

import tornado
import maproxy.proxyserver


class Session(object):
    """
    The Session class if the heart of the system.
    - We create the session when  a client connects to the server (proxy). this connection is "c2p"
    - We create a connection to the server (p2s)
    - Each connection (c2p,p2s) has a state (Session.State) , can be CLOSED,CONNECTING,CONNECTED
    - Initially, when c2p is created we :
        - create the p-s connection
        - start read from c2p
    - Completion Routings:
        - on_XXX_done_read:
          When we get data from one side, we initiate a "start_write" to the other side .
          Exception: if the target is not connected yet, we queue the data so we can send it later
        - on_p2s_connected:
          When p2s is connected ,we start read from the server .
          if queued data is available (data that was sent from the c2p) we initiate a "start_write" immediately
        - on_XXX_done_write:
          When we're done "sending" data , we check if there's more data to send in the queue.
          if there is - we initiate another "start_write" with the queued data
        - on_XXX_close:
          When one side closes the connection, we either initiate a "start_close" on the other side, or (if already closed) - remove the session
    - I/O routings:
        - XXX_start_read: simply start read from the socket (we assume and validate that only one read goes at a time)
        - XXX_start_write: if currently writing , add data to queue. if not writing - perform io_write...




    """
    class LoggerOptions:
        """
        Logging options - which messages/notifications we would like to log...
        The logging is for development&maintenance. In production set all to False
        """
        # Log charactaristics
        LOG_SESSION_ID=True         # for each log, add the session-id
        # Log different operations
        LOG_NEW_SESSION_OP=True
        LOG_READ_OP=True
        LOG_WRITE_OP=True
        LOG_CLOSE_OP=True
        LOG_CONNECT_OP=True
        LOG_REMOVE_SESSION=True

    class State:
        """
        Each socket has a state.
        We will use the state to identify whether the connection is open or closed
        """
        CLOSED,CONNECTING,CONNECTED=range(3)

    def __init__(self):
        pass
    #def new_connection(self,stream : tornado.iostream.IOStream ,address,proxy):
    def new_connection(self,stream ,address,proxy):
            # First,validation
            assert isinstance(proxy,maproxy.proxyserver.ProxyServer)
            assert isinstance(stream,tornado.iostream.IOStream)

            # Logging
            self.logger_nesting_level=0         # logger_nesting_level is the current "nesting level"
            if Session.LoggerOptions.LOG_NEW_SESSION_OP:
                self.log("New Session")


            # Remember our "parent" ProxyServer
            self.proxy=proxy

            # R/W flags for each socket
            # Using the flags, we can tell if we're waiting for I/O completion
            # NOTE: the "c2p" and "p2s" prefixes are NOT the direction of the IO,
            #       they represent the SOCKETS :
            #       c2p means the socket from the client to the proxy
            #       p2s means the socket from the proxy to the server
            self.c2p_reading=False  # whether we're reading from the client
            self.c2p_writing=[]  # whether we're writing to the client

            self.p2s_writing=[]  # whether we're writing to the server
            self.p2s_reading=[]  # whether we're reading from the server
            self.p2s_state=[]

            for target_num in range(self.proxy.num_targets):
                self.c2p_writing.append(False)
                self.p2s_writing.append(False)
                self.p2s_reading.append(False)
                self.p2s_state.append(Session.State.CLOSED)

            self.p2s_secondary_writing=False  # whether we're writing to the secondary server
            self.p2s_secondary_reading=False  # whether we're reading from the secondary server
            self.session_removed=False

            self.c2p_lock = threading.Lock()
            self.p2s_lock = threading.Lock()
            # Init the Client->Proxy stream
            self.c2p_stream=stream
            self.c2p_address=address
            # Client->Proxy  is connected
            self.c2p_state=Session.State.CONNECTED

            # Here we will put incoming data while we're still waiting for the target-server's connection
            self.c2s_queued_data=[] # Data that was read from the Client, and needs to be sent to the  Server
            self.s2c_queued_data=[] # Data that was read from the Server , and needs to be sent to the  client
            for target_num in range(self.proxy.num_targets):
                # Data that was read from the Client, and needs to be sent to the  Server
                self.c2s_queued_data.append([])

                # Data that was read from the Server , and needs to be sent to the  client
                self.s2c_queued_data.append([])

            # send data immediately to the client ... (Disable Nagle TCP algorithm)
            self.c2p_stream.set_nodelay(True)
            # Let us now when the client disconnects (callback on_c2p_close)
            self.c2p_stream.set_close_callback( self.on_c2p_close)

            # Create the Proxy->Server socket and stream
            server_connections = []
            for target_num in range(self.proxy.num_targets):
                server_connections.append(socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0))

            self.p2s_stream = []

            if self.proxy.server_ssl_options is not None:
                # if the "server_ssl_options" where specified, it means that when we connect, we need to wrap with SSL
                # so we need to use the SSLIOStream stream
                for target_num in range(self.proxy.num_targets):
                    self.p2s_stream.append(tornado.iostream.SSLIOStream(server_connections[target_num],
                                                                        ssl_options=self.proxy.server_ssl_options))
            else:
                # use the standard IOStream stream
                for target_num in range(self.proxy.num_targets):
                    self.p2s_stream.append(tornado.iostream.IOStream(server_connections[target_num]))

            for target_num in range(self.proxy.num_targets):
                # send data immediately to the server... (Disable Nagle TCP algorithm)
                self.p2s_stream[target_num].set_nodelay(True)

                on_p2s_close = partial(self.on_p2s_close_n, target_num=target_num)
                self.p2s_stream[target_num].set_close_callback( on_p2s_close )

                # P->S state is "connecting"
                self.p2s_state[target_num]=Session.State.CONNECTING
                on_p2s_done_connect = partial(self.on_p2s_done_connect_n, target_num=target_num)
                target = self.proxy.targets[target_num]

                self.p2s_stream[target_num].connect((target.address, target.port),
                                                    on_p2s_done_connect )

            # We can actually start reading immediatelly from the C->P socket
            self.c2p_start_read()

    # Each member-function can call this method to log data (currently to screen)
    def log(self,msg):
        prefix=str(id(self))+":" if Session.LoggerOptions.LOG_SESSION_ID else ""
        prefix+=self.logger_nesting_level*" "*4
        logging.debug(prefix + msg)



    #  Logging decorator (enter/exit)
    def logger(enabled=True):
        """
        We use this decorator to wrap functions and log the input/ouput of each function
        Since this decorator accepts a parameter, it must return an "inner" decorator....(Python stuff)
        """
        def inner_decorator(func):


            def log_wrapper(self,*args,**kwargs):

                msg="%s (%s,%s)" % (func.__name__,args,kwargs)


                self.log(msg)
                self.logger_nesting_level+=1
                r=func(self,*args,**kwargs)
                self.logger_nesting_level-=1
                self.log("%s -> %s" % (msg,str(r)) )
                return r
            return log_wrapper if enabled else func


        return inner_decorator



    ################
    ## Start Read ##
    ################
    @logger(LoggerOptions.LOG_READ_OP)
    def c2p_start_read(self):
        """
        Start read from client
        """
        assert( not self.c2p_reading)
        self.c2p_reading=True
        try:
            self.c2p_stream.read_until_close(lambda x: None,self.on_c2p_done_read)
        except tornado.iostream.StreamClosedError:
            self.c2p_reading=False

    @logger(LoggerOptions.LOG_READ_OP)
    def p2s_start_read_n(self, target_num):
        """
        Start read from server
        """
        assert( not self.p2s_reading[target_num])
        self.p2s_reading[target_num] = True
        try:
            on_p2s_done_read = partial(self.on_p2s_done_read_n, target_num=target_num)
            self.p2s_stream[target_num].read_until_close(lambda x:None, on_p2s_done_read)
        except tornado.iostream.StreamClosedError:
            self.p2s_reading[target_num] = False


    ##############################
    ## Read Completion Routines ##
    ##############################
    @logger(LoggerOptions.LOG_READ_OP)
    def on_c2p_done_read(self,data):
        # # We got data from the client (C->P ) . Send data to the server
        assert(self.c2p_reading)
        assert(data)
        for target_num in range(self.proxy.num_targets):
            self.p2s_start_write_n(target_num=target_num, data=data)

    @logger(LoggerOptions.LOG_READ_OP)
    def on_p2s_done_read_n(self, data, target_num):
        # got data from Server to Proxy . if the client is still connected - send the data to the client
        assert( self.p2s_reading[target_num])
        assert(data)
        if target_num == 0:
            self.c2p_start_write_n(target_num=target_num, data=data)


    #####################
    ## Write to stream ##
    #####################
    @logger(LoggerOptions.LOG_WRITE_OP)
    def _c2p_io_write_n(self, data, target_num):
        with self.c2p_lock:
            if data is None:
                # None means (gracefully) close-socket  (a "close request" that was queued...)
                self.c2p_state=Session.State.CLOSED
                try:
                    self.c2p_stream.close()
                except tornado.iostream.StreamClosedError:
                    self.c2p_writing[target_num] = False
            else:
                self.c2p_writing[target_num] = True
                try:
                    # on_c2p_done_write = partial(self.on_c2p_done_write_n, target_num=target_num)
                    self.c2p_stream.write(data, callback=self.on_c2p_done_write)
                except tornado.iostream.StreamClosedError:
                    # Cancel the write, we will get on_close instead...
                    self.c2p_writing[target_num] = False

    @logger(LoggerOptions.LOG_WRITE_OP)
    def _p2s_io_write_n(self, data, target_num):
        if data is None:
            # None means (gracefully) close-socket  (a "close request" that was queued...)
            with self.p2s_lock:
                self.p2s_state[target_num] = Session.State.CLOSED
                try:
                    self.p2s_stream[target_num].close()
                except tornado.iostream.StreamClosedError:
                    # Cancel the write. we will get on_close instead
                    self.p2s_writing[target_num] = False
        else:
            self.p2s_writing[target_num] = True
            try:
                on_p2s_done_write = partial(self.on_p2s_done_write_n, target_num=target_num)
                self.p2s_stream[target_num].write(data, callback=on_p2s_done_write)
            except tornado.iostream.StreamClosedError:
                # Cancel the write. we will get on_close instead
                self.p2s_writing[target_num] = False


    #################
    ## Start Write ##
    #################
    @logger(LoggerOptions.LOG_WRITE_OP)
    def c2p_start_write_n(self, data, target_num):
        """
        Write to client.if there's a pending write-operation, add it to the S->C (s2c) queue
        """
        # If not connected - do nothing...
        if self.c2p_state != Session.State.CONNECTED: return

        if not any(self.c2p_writing):
            # If we're not currently writing
            assert( not self.s2c_queued_data[target_num] ) # we expect the  queue to be empty

            # Start the "real" write I/O operation
            self._c2p_io_write_n(target_num=target_num, data=data)
        else:
            # Just add to the queue
            self.s2c_queued_data[target_num].append(data)

    @logger(LoggerOptions.LOG_WRITE_OP)
    def p2s_start_write_n(self, data, target_num):
        """
        Write to the server.
        If not connected yet - queue the data
        If there's a pending write-operation , add it to the C->S (c2s) queue
        """

        # If still connecting to the server - queue the data...
        if self.p2s_state[target_num] == Session.State.CONNECTING:
            self.c2s_queued_data[target_num].append(data)   # TODO: is it better here to append (to list) or concatenate data (to buffer) ?
            return
        # If not connected - do nothing
        if self.p2s_state[target_num] == Session.State.CLOSED:
            return
        assert(self.p2s_state[target_num] == Session.State.CONNECTED)

        if not self.p2s_writing[target_num]:
            # Start the "real" write I/O operation
            self._p2s_io_write_n(target_num=target_num, data=data)
        else:
            # Just add to the queue
            self.c2s_queued_data[target_num].append(data)


    ##############################
    ## Write Competion Routines ##
    ##############################
    @logger(LoggerOptions.LOG_WRITE_OP)
    def on_c2p_done_write(self):
        """
        A start_write C->P  (write to client) is done .
        if there is queued-data to send - send it
        """
        assert(any([writing for writing in self.c2p_writing]))

        for target_num in range(self.proxy.num_targets):
            queued_data = self.s2c_queued_data[target_num]
            if not queued_data:
                self.c2p_writing[target_num] = False
                continue
            self._c2p_io_write_n(target_num=target_num, data=queued_data.pop(0))
            return


    @logger(LoggerOptions.LOG_WRITE_OP)
    def on_p2s_done_write_n(self, target_num):
        """
        A start_write P->S  (write to server) is done .
        if there is queued-data to send - send it
        """
        assert(self.p2s_writing[target_num])
        queued_data = self.c2s_queued_data[target_num]
        if queued_data:
            # more data in the queue, write next item as well..
            self._p2s_io_write_n( target_num=target_num, data=queued_data.pop(0))
            return
        self.p2s_writing[target_num]=False



    ######################
    ## Close Connection ##
    ######################
    @logger(LoggerOptions.LOG_CLOSE_OP)
    def c2p_start_close(self,gracefully=True):
        """
        Close c->p connection
        if gracefully is True then we simply add None to the queue, and start a write-operation
        if gracefully is False then this is a "brutal" close:
            - mark the stream is closed
            - we "reset" (empty) the queued-data
            - if the other side (p->s) already closed, remove the session

        """
        if self.c2p_state == Session.State.CLOSED:
            return
        if gracefully:
            self.c2p_start_write_n(target_num=0, data=None) # Primary is always present
            return

        self.c2p_state = Session.State.CLOSED
        for target_num in range(self.proxy.num_targets):
            self.s2c_queued_data[target_num]=[]
        self.s2c_secondary_queued_data=[]
        self.c2p_stream.close()
        self.remove_session()


    @logger(LoggerOptions.LOG_CLOSE_OP)
    def p2s_start_close_n(self, target_num, gracefully=True):
        """
        Close p->s connection
        if gracefully is True then we simply add None to the queue, and start a write-operation
        if gracefully is False then this is a "brutal" close:
            - mark the stream is closed
            - we "reset" (empty) the queued-data
            - if the other side (p->s) already closed, remove the session

        """
        if self.p2s_state[target_num] == Session.State.CLOSED:
            return
        if gracefully:
            self.p2s_start_write_n(target_num=target_num, data=None)
            return

        self.p2s_state[target_num] = Session.State.CLOSED
        self.c2s_queued_data[target_num]=[]

        self.p2s_stream[target_num].close()
        if self.c2p_state == Session.State.CLOSED:
            self.remove_session()


    @logger(LoggerOptions.LOG_CLOSE_OP)
    def on_c2p_close(self):
        """
        Client closed the connection.
        we need to:
        1. update the c2p-state
        2. if there's no more data to the server (c2s_queued_data is empty) - we can close the p2s connection
        3. if p2s already closed - we can remove the session
        """
        self.c2p_state=Session.State.CLOSED
        if all([state == Session.State.CLOSED for state in self.p2s_state]):
            self.remove_session()
        else:
            for target_num in range(self.proxy.num_targets):
                self.p2s_start_close_n(target_num, gracefully=True)


    @logger(LoggerOptions.LOG_CLOSE_OP)
    def on_p2s_close_n(self, target_num):
        """
        Server closed the connection.
        We need to update the satte, and if the client closed as well - delete the session
        """
        with self.p2s_lock:
            self.p2s_state[target_num] = Session.State.CLOSED
            if self.c2p_state == Session.State.CLOSED:
                self.remove_session()
            elif all([state == Session.State.CLOSED for state in self.p2s_state]):
                self.c2p_start_close(gracefully=True)
            elif target_num == 0: # Primary Target
                self.c2p_start_close(gracefully=True)



    ########################
    ## Connect-Completion ##
    ########################
    @logger(LoggerOptions.LOG_CONNECT_OP)
    def on_p2s_done_connect_n(self, target_num):
        with self.p2s_lock:
            assert(self.p2s_state[target_num]==Session.State.CONNECTING)
            self.p2s_state[target_num]=Session.State.CONNECTED
            # Start reading from the socket
            self.p2s_start_read_n(target_num)
            assert(not self.p2s_writing[target_num])    # As expect no current write-operation ...

            # If we have pending-data to write, start writing...
            if self.c2s_queued_data[target_num]:
                # TRICKY: get thte frst item , and write it...
                # this is tricky since the "start-write" will
                # write this item even if there are queued-items... (since self.p2s_writing=False)
                self.p2s_start_write_n( target_num=target_num, data=self.c2s_queued_data[target_num].pop(0)  )


    ###########
    ## UTILS ##
    ###########
    @logger(LoggerOptions.LOG_REMOVE_SESSION)
    def remove_session(self):
        lock_acquired = self.c2p_lock.acquire(timeout=600)

        try:
            self.log('Lock acquired: %s Removing session' % lock_acquired)
            if (all([state == Session.State.CLOSED for state in self.p2s_state]) and
                self.c2p_state == Session.State.CLOSED and
                not self.session_removed):

                self.proxy.remove_session(self)
                self.session_removed = True
        finally:
            if lock_acquired:
                self.c2p_lock.release()

class SessionFactory(object):
    """
    This is  the default session-factory. it simply returns a "Session" object
    """
    def __init__(self):
        pass

    def new(self,*args,**kwargs):
        """
        The caller needs a Session objet (constructed with *args,**kwargs).
        In this implementation we're simply creating a new object. you can enhance and create a pool or add logs..
        """
        return Session(*args,**kwargs)
    def delete(self,session):
        """
        Delete a session object
        """
        assert( isinstance(session,Session))
        del session
