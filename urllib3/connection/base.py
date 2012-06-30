# urllib3/connection/base.py
# Copyright 2008-2012 Andrey Petrov and contributors (see CONTRIBUTORS.txt)
#
# This module is part of urllib3 and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from collections import namedtuple


class Connection(object):
    "Generic Connection base class."

    def __init__(self, host, port):
        self.host = host
        self.port = port



Cert = namedtuple('Cert', ['key_file', 'cert_file', 'cert_reqs', 'ca_certs'])
