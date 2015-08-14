#!/usr/bin/python

import time
import elasticsearch

from .base import BaseGreenlet


class OutputElastic(BaseGreenlet):
    def __init__(self, index, es_config):
        super(OutputElastic, self).__init__()
        self.conn = elasticsearch.Elasticsearch(es_config)
        self.index = index

    def send_events(self, events):
        actions = []
        for event in events:
            # TODO: actually index must be set from event timestamp
            action = {
                '_index': time.strftime(self.index),
                '_type': event['type'],
                '_source': event
            }
            actions.append(action)
        try:
            # elasticsearch.helpers.bulk(self.conn, actions)
            print(actions)
        except elasticsearch.ElasticsearchException, e:
            self.logger.error('Error bulk indexing: {}'.format(e))

    def _run(self):
        while True:
            events = self.get_events()
            self.send_events(events)
