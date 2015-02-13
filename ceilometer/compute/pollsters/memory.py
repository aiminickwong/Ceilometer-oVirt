# Copyright (c) 2014 VMware, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from ceilometer.compute import plugin
from ceilometer.compute.pollsters import util
from ceilometer.compute.virt import inspector as virt_inspector
from ceilometer.openstack.common.gettextutils import _  # noqa
from ceilometer.openstack.common import log
from ceilometer import sample

LOG = log.getLogger(__name__)


class MemoryUsagePollster(plugin.ComputePollster):

    def get_samples(self, manager, cache, resources):
        self._inspection_duration = self._record_poll_time()
        for instance in resources:
            LOG.debug(_('Checking memory usage for instance %s'), instance.id)
            try:
                memory_info = manager.inspector.inspect_memory_usage(
                    instance, self._inspection_duration)
                LOG.debug(_("MEMORY USAGE: %(instance)s %(usage)f"),
                          ({'instance': instance.__dict__,
                            'usage': memory_info.usage}))
                yield util.make_sample_from_instance(
                    instance,
                    name='memory.usage',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_info.usage,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Obtaining Memory Usage is not implemented for %s'
                            ), manager.inspector.__class__.__name__)
            except Exception as err:
                LOG.error(_('Could not get Memory Usage for %(id)s: %(e)s'), (
                          {'id': instance.id, 'e': err}))

class MemoryTotalPollster(plugin.ComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory total and system info for instance %s'), instance.id)
            try:
                instance_name = util.instance_name(instance)
                memory_total_kb = int(manager.oga_inspector.inspect_mem_total(
                    instance_name))
                LOG.debug(_("MEMORY TOTAL: %(instance)s %(total)f"),
                          ({'instance': instance.__dict__,
                            'total': memory_total_kb}))

                if memory_total_kb == -1:
                    # Note(luogangyi): we do not report empty info
                    continue
                memory_total = memory_total_kb/1024

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.total',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_total,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Obtaining Memory Total is not implemented for %s'
                            ), manager.inspector.__class__.__name__)
            except Exception as err:
                LOG.exception(_('Could not get Memory Total for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemoryUnusedPollster(plugin.ComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory unused for instance %s'), instance.id)
            try:
                instance_name = util.instance_name(instance)
                memory_unused_kb = int(manager.oga_inspector.inspect_mem_unused(
                    instance_name))
                LOG.debug(_("MEMORY UNUSED: %(instance)s %(unused)f"),
                          ({'instance': instance.__dict__,
                            'unused': memory_unused_kb}))

                if memory_unused_kb == -1:
                    # Note(luogangyi): we do not report empty info
                    continue
                memory_unused = memory_unused_kb/1024

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.unused',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_unused,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Obtaining Memory Unused is not implemented for %s'
                            ), manager.inspector.__class__.__name__)
            except Exception as err:
                LOG.exception(_('Could not get Memory Unused for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemorySwapPollster(plugin.ComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory swap for instance %s'), instance.id)
            try:
                instance_name = util.instance_name(instance)
                memory_swap_kb = int(manager.oga_inspector.inspect_mem_swap(
                    instance_name))
                LOG.debug(_("MEMORY SWAP: %(instance)s %(swap)f"),
                          ({'instance': instance.__dict__,
                            'swap': memory_swap_kb}))

                if memory_swap_kb == -1:
                    # Note(luogangyi): we do not report empty info
                    continue
                memory_swap = memory_swap_kb/1024

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.swap',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_swap,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Obtaining Memory Swap is not implemented for %s'
                            ), manager.inspector.__class__.__name__)
            except Exception as err:
                LOG.exception(_('Could not get Memory Swap for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemoryBufferPollster(plugin.ComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory buffer for instance %s'), instance.id)
            try:
                instance_name = util.instance_name(instance)
                memory_buffer_kb = int(manager.oga_inspector.inspect_mem_buffer(
                    instance_name))
                LOG.debug(_("MEMORY Buffer: %(instance)s %(buffer)f"),
                          ({'instance': instance.__dict__,
                            'buffer': memory_buffer_kb}))

                if memory_buffer_kb == -1:
                    # Note(luogangyi): we do not report empty info
                    continue

                memory_buffer = memory_buffer_kb/1024

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.buffer',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_buffer,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Obtaining Memory Buffer is not implemented for %s'
                            ), manager.inspector.__class__.__name__)
            except Exception as err:
                LOG.exception(_('Could not get Memory Buffer for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})


class MemoryCachedPollster(plugin.ComputePollster):

    def get_samples(self, manager, cache, resources):
        for instance in resources:
            LOG.debug(_('Checking memory cached for instance %s'), instance.id)
            try:
                instance_name = util.instance_name(instance)
                memory_cached_kb = int(manager.oga_inspector.inspect_mem_cached(
                    instance_name))
                LOG.debug(_("MEMORY CACHED: %(instance)s %(cached)f"),
                          ({'instance': instance.__dict__,
                            'cached': memory_cached_kb}))

                if memory_cached_kb == -1:
                    # Note(luogangyi): we do not report empty info
                    continue
                memory_cached = memory_cached_kb/1024

                yield util.make_sample_from_instance(
                    instance,
                    name='memory.cached',
                    type=sample.TYPE_GAUGE,
                    unit='MB',
                    volume=memory_cached,
                )
            except virt_inspector.InstanceNotFoundException as err:
                # Instance was deleted while getting samples. Ignore it.
                LOG.debug(_('Exception while getting samples %s'), err)
            except NotImplementedError:
                # Selected inspector does not implement this pollster.
                LOG.debug(_('Obtaining Memory Cached is not implemented for %s'
                            ), manager.inspector.__class__.__name__)
            except Exception as err:
                LOG.exception(_('Could not get Memory Cached for '
                                '%(id)s: %(e)s'), {'id': instance.id,
                                                   'e': err})