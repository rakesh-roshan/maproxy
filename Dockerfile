FROM python:3.4
ENV MAPROXYSRC /usr/src/maproxy/
RUN mkdir -p $MAPROXYSRC
ADD ./ $MAPROXYSRC
WORKDIR $MAPROXYSRC
RUN python $MAPROXYSRC/setup.py install --force
EXPOSE 10001
ENTRYPOINT ["python", "./demos/tcp2tcp.py", "--listen-port", "10001"]
