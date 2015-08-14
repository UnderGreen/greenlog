#!/usr/bin/python

import gevent
import gevent.queue
import logging

_greenlets = []


def startall():
    for greenlet in _greenlets:
        greenlet.start()


def joinall():
    gevent.joinall(_greenlets)


class BaseGreenlet(gevent.Greenlet):
    def __init__(self):
        super(BaseGreenlet, self).__init__()
        self.logger = logging.getLogger(__name__)
        self.out_queue = gevent.queue.Queue()
        _greenlets.append(self)

    def to(self, reciever):
        self.logger.debug('{!r} setted out to {!r}'.format(self, reciever))
        reciever.set_in_queue(self.out_queue)

    def set_in_queue(self, queue):
        self.in_queue = queue

    def get_events(self):
        return self.in_queue.get()

    def put_events(self, events):
        self.out_queue.put(events)

    def _run(self):
        raise NotImplemented()
