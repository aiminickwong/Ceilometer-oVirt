#
# Copyright 2013 IBM Corp
#
# Author: Tong Li <litong01@us.ibm.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
from oslo.utils import timeutils
from oslo.config import cfg

from ceilometer import dispatcher
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from ceilometer.publisher import utils as publisher_utils
from ceilometer.utils import SetJSONEncoder

import json
import redis

LOG = log.getLogger(__name__)

STORAGE_OPTS = [
    cfg.IntOpt('redis_port',
               default=6379,
               help="The port used by redis, default 6379 "),
    cfg.IntOpt('redis_db',
               default=0,
               help="The db used by redis, default 0"),
    cfg.StrOpt('redis_host',
               default=None,
               help='The host of redis database'),
    cfg.StrOpt('redis_password',
               default=None,
               help='The password redis use'),
]

cfg.CONF.register_opts(STORAGE_OPTS, group='redis_database')

class RedisDispatcher(dispatcher.Base):
    """Dispatcher class for recording metering data into database.

    The dispatcher class which records each meter into a database configured
    in ceilometer configuration file.

    To enable this dispatcher, the following section needs to be present in
    ceilometer.conf file

    dispatchers = database
    """
    def __init__(self, conf):
        super(RedisDispatcher, self).__init__(conf)
        self.instance_map = {}
        self.host_map = {}
        print redis.__file__
        #self.redis_conn = redis.Redis(host='192.168.39.16',
        #                              port=6379, db=0, password='foobared')
        self.redis_conn = redis.Redis(host=conf.redis_database.redis_host,
                                      port=conf.redis_database.redis_port,
                                      db=conf.redis_database.redis_db,
                                      password=conf.redis_database.redis_password)

    def record_metering_data(self, data):
        # We may have receive only one counter on the wire
        if not isinstance(data, list):
            data = [data]

        for meter in data:
            LOG.debug(_(
                'metering data %(counter_name)s '
                'for %(resource_id)s @ %(timestamp)s: %(counter_volume)s')
                % ({'counter_name': meter['counter_name'],
                    'resource_id': meter['resource_id'],
                    'timestamp': meter.get('timestamp', 'NO TIMESTAMP'),
                    'counter_volume': meter['counter_volume']}))
            try:
                # Convert the timestamp to a datetime instance.
                # Storage engines are responsible for converting
                # that value to something they can store.
                if meter.get('timestamp'):
                    ts = timeutils.parse_isotime(meter['timestamp'])
                    #meter['timestamp'] = timeutils.normalize_time(ts)

                if meter.get('resource_metadata'):
                    resource_metadata = meter['resource_metadata']
                    if resource_metadata.get('instance_id'):
                        instance_uuid = resource_metadata['instance_id']
                        meter_name = meter['counter_name']
                        resource_id = meter['resource_id']

                        meter_map = self.instance_map.get(instance_uuid)
                        if meter_map is None:
                            meter_map = {}
                            self.instance_map[instance_uuid] = meter_map

                        resource_set = meter_map.get(meter_name)
                        if resource_set is None:
                            resource_set = set()
                            meter_map[meter_name] = resource_set
                        resource_set.add(resource_id)

                        meter_map_json = json.dumps(meter_map, cls=SetJSONEncoder)
                        print meter_map_json
                        self.redis_conn.set("metermeta-"+instance_uuid, meter_map_json)
                        counter_map = {}
                        counter_map['counter_name'] = meter['counter_name']
                        counter_map['counter_unit'] = meter['counter_unit']
                        counter_map['counter_type'] = meter['counter_type']
                        counter_map['counter_volume'] = meter['counter_volume']
                        counter_map['timestamp'] = meter['timestamp']

                        counter_map_json = json.dumps(counter_map)
                        print counter_map_json
                        self.redis_conn.set(meter_name+'-'+resource_id, counter_map_json)

                        print "%s instance: %s, meter_name: %s, volume: %s" %\
                              (meter['timestamp'], resource_metadata['instance_id'],
                               meter['counter_name'], meter['counter_volume'])

                    elif resource_metadata.get('hostname'):
                        hostname = resource_metadata['hostname']
                        meter_name = meter['counter_name']
                        resource_id = meter['resource_id']

                        meter_map = self.host_map.get(hostname)
                        if meter_map is None:
                            meter_map = {}
                            self.host_map[hostname] = meter_map

                        resource_set = meter_map.get(meter_name)
                        if resource_set is None:
                            resource_set = set()
                            meter_map[meter_name] = resource_set
                        resource_set.add(resource_id)

                        meter_map_json = json.dumps(meter_map, cls=SetJSONEncoder)
                        print meter_map_json
                        self.redis_conn.set("metermeta-"+hostname, meter_map_json)

                        counter_map = {}
                        counter_map['counter_name'] = meter['counter_name']
                        counter_map['counter_unit'] = meter['counter_unit']
                        counter_map['counter_type'] = meter['counter_type']
                        counter_map['counter_volume'] = meter['counter_volume']
                        counter_map['timestamp'] = meter['timestamp']

                        counter_map_json = json.dumps(counter_map)
                        print counter_map_json
                        self.redis_conn.set(meter_name+'-'+resource_id, counter_map_json)

                        print "%s hostname: %s, meter_name: %s, volume: %s" % \
                              (meter['timestamp'], resource_metadata['hostname'],
                               meter['counter_name'], meter['counter_volume'])

            except Exception as err:
                LOG.exception(_('Failed to record metering data: %s'),
                              err)


    def record_events(self, events):
        if not isinstance(events, list):
            events = [events]

        return self.storage_conn.record_events(events)
