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

from ceilometer import dispatcher
from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from ceilometer.publisher import utils as publisher_utils
from ceilometer import storage
from ceilometer.utils import SetJSONEncoder

import json
import redis

LOG = log.getLogger(__name__)


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
        self.storage_conn = storage.get_connection_from_config(conf)
        self.instance_map = {}
        self.host_map = {}
        print redis.__file__
        self.redis_conn = redis.Redis(host='192.168.39.16',
                                      port=6379, db=0, password='foobared')

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
            if publisher_utils.verify_signature(
                    meter,
                    self.conf.publisher.metering_secret):
                try:
                    # Convert the timestamp to a datetime instance.
                    # Storage engines are responsible for converting
                    # that value to something they can store.
                    if meter.get('timestamp'):
                        ts = timeutils.parse_isotime(meter['timestamp'])
                        #meter['timestamp'] = timeutils.normalize_time(ts)

                    if meter.get('resource_metadata'):
                        resource_metadata = meter['resource_metadata']
                        if resource_metadata.get('instance_uuid'):
                            instance_uuid = resource_metadata['instance_uuid']
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
                                  (meter['timestamp'], resource_metadata['instance_uuid'],
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
            else:
                LOG.warning(_(
                    'message signature invalid, discarding message: %r'),
                    meter)

    def record_events(self, events):
        if not isinstance(events, list):
            events = [events]

        return self.storage_conn.record_events(events)
