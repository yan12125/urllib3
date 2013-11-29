import unittest

import sys
sys.path.append('../')

from urllib3 import filepost
from StringIO import StringIO

class TestFilePost(unittest.TestCase):
    def test_generator(self):
        fields = {
            'foo': 'bar',
            'somefile': ('name.txt', StringIO('trolololol')),
        }

        stream = filepost.MultipartEncoderGenerator(fields, boundary="boundary")
        body = ''.join(chunk for chunk in filepost.IterStreamer(stream))
        self.assertEqual(body, u'--boundary\r\nContent-Disposition: form-data; name="somefile"; filename="name.txt"\r\nContent-Type: text/plain\r\n\r\ntrolololol\r\n--boundary\r\nContent-Disposition: form-data; name="foo"\r\nContent-Type: text/plain\r\n\r\nbar\r\n--boundary--\r\n')

    def test_len(self):
        fields = {
            'foo': 'bar',
            'somefile': ('name.txt', StringIO('trolololol')),
        }

        iterdata = filepost.MultipartEncoderGenerator(fields, boundary="boundary")
        predicted_size = len(iterdata)

        body = ''.join(chunk for chunk in filepost.IterStreamer(iterdata))

        self.assertEqual(len(body), predicted_size)
