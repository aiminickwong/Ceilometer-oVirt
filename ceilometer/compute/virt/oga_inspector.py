#
# Copyright 2014  China Mobile Limited
#
# Author: Gangyi Luo <luogangyi@chinamobile.com>
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
import collections
from stevedore import driver

from ceilometer.openstack.common.gettextutils import _
from ceilometer.openstack.common import log
from ceilometer import utils
import time
import os

from time import sleep
from ovirtga.guestagent import GuestAgent
from ovirtga.vmchannels import Listener
#from ovirtga.vmgreenchannels import Listener
LOG = log.getLogger(__name__)

_VMCHANNEL_DEVICE_NAME = 'com.redhat.rhevm.vdsm'
# This device name is used as default both in the qemu-guest-agent
# service/daemon and in libvirtd (to be used with the quiesce flag).
_QEMU_GA_DEVICE_NAME = 'org.qemu.guest_agent.0'
_QEMU_GA_DEVICE_DIR = '/var/lib/libvirt/qemu/'

# Named tuple representing disk usage.
#
# mount_point: mount point of a disk device
# usage: disk space usage of the mount point
#
DiskUsage = collections.namedtuple('DiskUsage',
                                   ['mount_point', 'usage'])


class OGAInspector(object):

    INSPECTOR_TIMEOUT = 120

    def __init__(self):
        self.oga_dict = {}
        self.channelListener = Listener()
        self.channelListener.settimeout(30)
        self.channelListener.start()
        #self._prepare_socket_dir()

    def _get_agent(self, instance_name):

        if instance_name in self.oga_dict:
            return self.oga_dict[instance_name]

        guestSocketFile = self._make_channel_path(_VMCHANNEL_DEVICE_NAME,
                                                  instance_name)
        if os.path.exists(guestSocketFile):
            guest_agent = GuestAgent(guestSocketFile, self.channelListener)
            self.oga_dict[instance_name] = guest_agent
            guest_agent.connect()
            return guest_agent
        else:
            LOG.error("Instance %s socket file %s does not exist!" %
                      (instance_name, guestSocketFile))
            return None

    def clear_outdated_agent(self):

        del_keys = []
        for instance_name in self.oga_dict:
            guest_agent = self.oga_dict[instance_name]
            if (time.time() - guest_agent.update_time()
                    > self.INSPECTOR_TIMEOUT):
                guest_agent.stop()
                del_keys.append(instance_name)

        for key in del_keys:
            del self.oga_dict[key]

    def _prepare_socket_dir(self):
        chmod_dir_cmd = ['chmod', '-R', 'o+x', _QEMU_GA_DEVICE_DIR]
        utils.execute(*chmod_dir_cmd, run_as_root=True)

    def _make_channel_path(self, deviceName, instance_name):
        return "/var/lib/libvirt/qemu/%s.%s.sock" % (deviceName,
                                                     instance_name)

    def inspect_mem(self, instance_name):
        """Inspect the CPU statistics for an instance.
        :param instance_name: the name of the target instance
        :return: the number of CPUs and cumulative CPU time
        """
        agt = self._get_agent(instance_name)
        while(True):
            print agt.getGuestInfo()
            if(agt.getGuestInfo().get("memoryStats") is not None and
                agt.getGuestInfo().get("memoryStats").get("mem_total")):
                print agt.getGuestInfo()["memoryStats"]["mem_total"]
            self.inspect_sys(instance_name)
            sleep(1)

    def inspect_sys(self, instance_name):
        """Inspect the system information for an instance.
        :param instance_name: the name of the target instance
        :return: the dict of system information
        """
        agt = self._get_agent(instance_name)
        if agt is None:
            return None
        sys_dict = {}
        for attr in ["netIfaces", "guestFQDN", "lastLogin",
                     "guestOs", "guestIPs"]:
            val = agt.getGuestInfo().get(attr)
            if val is not None and val != '':
                sys_dict[attr] = val
                print attr, val
        return sys_dict

    def inspect_mem_total(self, instance_name):
        """Inspect the Total Memory for an instance.
        :param instance_name: the name of the target instance
        :return: the size of the total memory or -1 if none data retrieved
        """
        agt = self._get_agent(instance_name)
        if agt is None:
            return -1
        guestInfo = agt.getGuestInfo()
        if (guestInfo is not None and
            guestInfo.get("memoryStats") is not None and
            guestInfo.get("memoryStats").get("mem_total") is not None):
            return guestInfo.get("memoryStats").get("mem_total")
        else:
            return -1

    def inspect_mem_total(self, instance_name):
        """Inspect the Total Memory for an instance.
        :param instance_name: the name of the target instance
        :return: the size of the total memory or -1 if none data retrieved
        """
        agt = self._get_agent(instance_name)
        if agt is None:
            return -1
        guestInfo = agt.getGuestInfo()
        if (guestInfo is not None and
            guestInfo.get("memoryStats") is not None and
            guestInfo.get("memoryStats").get("mem_total") is not None):
            return guestInfo.get("memoryStats").get("mem_total")
        else:
            return -1

    def inspect_mem_unused(self, instance_name):
        """Inspect the unused Memory for an instance.
        :param instance_name: the name of the target instance
        :return: the size of the unused memory or -1 if none data retrieved
        """
        agt = self._get_agent(instance_name)
        if agt is None:
            return -1
        guestInfo = agt.getGuestInfo()
        if (guestInfo is not None and
            guestInfo.get("memoryStats") is not None and
            guestInfo.get("memoryStats").get("mem_unused") is not None):
            return guestInfo.get("memoryStats").get("mem_unused")
        else:
            return -1

    def inspect_mem_cached(self, instance_name):
        """Inspect the cached Memory for an instance.
        :param instance_name: the name of the target instance
        :return: the size of the cached memory or -1 if none data retrieved
        """
        agt = self._get_agent(instance_name)
        if agt is None:
            return -1
        guestInfo = agt.getGuestInfo()
        if (guestInfo is not None and
            guestInfo.get("memoryStats") is not None and
            guestInfo.get("memoryStats").get("mem_cached") is not None):
            return guestInfo.get("memoryStats").get("mem_cached")
        else:
            return -1

    def inspect_mem_swap(self, instance_name):
        """Inspect the swap Memory for an instance.
        :param instance_name: the name of the target instance
        :return: the size of the swap memory or -1 if none data retrieved
        """
        agt = self._get_agent(instance_name)
        if agt is None:
            return -1
        guestInfo = agt.getGuestInfo()
        if (guestInfo is not None and
            guestInfo.get("memoryStats") is not None and
            guestInfo.get("memoryStats").get("swap_total") is not None):
            return guestInfo.get("memoryStats").get("swap_total")
        else:
            return -1

    def inspect_mem_buffer(self, instance_name):
        """Inspect the buffer Memory for an instance.
        :param instance_name: the name of the target instance
        :return: the size of the buffer memory or -1 if none data retrieved
        """
        agt = self._get_agent(instance_name)
        if agt is None:
            return -1
        guestInfo = agt.getGuestInfo()
        if (guestInfo is not None and
            guestInfo.get("memoryStats") is not None and
            guestInfo.get("memoryStats").get("mem_buffers") is not None):
            return guestInfo.get("memoryStats").get("mem_buffers")
        else:
            return -1

    def inspect_disk_usage(self, instance_name):
        """Inspect the disk_usage for an instance.
        :param instance_name: the name of the target instance
        :return: the list of disk usage or none if no data retrieved
        """
        agt = self._get_agent(instance_name)
        if agt is None:
            return None
        guestInfo = agt.getGuestInfo()
        if (guestInfo is not None and
            guestInfo.get("disksUsage") is not None):

            usage_list = []

            for per_disk in guestInfo["disksUsage"]:
                used = per_disk["used"]
                total = per_disk["total"]
                path = per_disk["path"]
                usage = float(used)/float(total)
                usage = round(usage, 3)
                usage = usage * 100
                disk_usage = DiskUsage(mount_point=path,
                                       usage=usage)
                usage_list.append(disk_usage)

            return usage_list

        else:
            return None


def get_oga_inspector():
    try:
        namespace = 'ceilometer.compute.virt'
        mgr = driver.DriverManager(namespace,
                                   "oga_inspector",
                                   invoke_on_load=True)
        return mgr.driver
    except ImportError as e:
        LOG.error(_("Unable to load the "
                    "Ovirt Geuest Agent inspector: %s") % e)
        return None


if __name__ == '__main__':
    inspector = OGAInspector()
    inspector.inspect_mem("instance-00000005")
