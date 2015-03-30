#!/usr/bin/python
# PBR Generated from u'console_scripts'

import sys

#from ceilometer.cmd.agent_compute import main
#from ceilometer.cmd.agent_notification import main
#from ceilometer.cmd.collector import main
#from ceilometer.cmd.agent_central import main
from ceilometer.cmd.alarm import notifier

if __name__ == "__main__":
    #sys.exit(main())
    sys.exit(notifier())