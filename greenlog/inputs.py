#!/usr/bin/python

import redis

from .base import BaseGreenlet


class InputRedis(BaseGreenlet):
    # TODO: multiple keys support
    # TODO: pubsub support
    def __init__(self, key, redis_config):
        super(InputRedis, self).__init__()
        self.key = key
        self.conn = redis.Redis(**redis_config)
        self.pipe = self.conn.pipeline(transaction=True)

    def _run(self):
        while True:
            try:
                first_raw_event = self.conn.blpop(self.key, timeout=1000)
                if first_raw_event is None:
                    continue
                self.pipe.lrange(self.key, 0, 1000)
                #               self.pipe.ltrim(self.key, 1000 + 1, -1)
                events = self.pipe.execute()[0]
                self.pipe.reset()
                events.append(first_raw_event[1])
                self.put_events(events)
            except redis.RedisError, e:
                self.logger.error(
                    'while fetching data from {!r} -- {!r}'.format(self.conn,
                                                                   e))
