import mimetools, mimetypes

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO


def get_content_type(filename):
    return mimetypes.guess_type(filename)[0] or 'application/octet-stream'


def encode(str):
    return str.encode('utf-8')

def file_size(fp):
    pos = fp.tell()
    fp.seek(0, 2)
    size = fp.tell()
    fp.seek(pos)
    return size-pos

class IterStreamer(object):
    """
    File-like streaming iterator.
    """
    def __init__(self, generator):
        self.generator = generator
        self.iterator = iter(generator)
        self.leftover = ''

    def __len__(self):
        return self.generator.__len__()

    def __iter__(self):
        return self.iterator

    def next(self):
        return self.iterator.next()

    def read(self, size):
        data = self.leftover
        count = len(self.leftover)
        try:
            while count < size:
                chunk = self.next()
                data += chunk
                count += len(chunk)
        except StopIteration, e:
            pass

        if count > size:
            self.leftover = data[size:]

        return data[:size]


class MultipartEncoderGenerator(object):
    """
    Generator yielding chunk-by-chunk streaming data from fields, with proper
    headers and boundary separators along the way. This is useful for streaming
    large files as iterators without loading the entire data body into memory.

    ``fields`` is a dictionary where the parameter name is the key and the value
    is either a (filename, data) tuple or just data.

    The data can be a unicode string, an iterator producing strings, or a file-like
    object. File-like objects are read ``chunk_size`` bytes at a time.

    If no ``boundary`` is specified then a random one is used.
    """
    def __init__(self, fields, boundary=None, chunk_size=8192):
        self.fields = fields
        self.chunk_size = chunk_size
        self.boundary = boundary or mimetools.choose_boundary()

    def get_content_type(self):
        return 'multipart/form-data; boundary=%s' % self.boundary

    def __len__(self):
        """
        Figure out the expected body size by iterating over the fields as if they
        contained empty files, while accumulating the value file sizes as 
        efficiently as we can.
        """
        empty_fields = {}
        size = 0
        for fieldname, value in self.fields.iteritems():
            if isinstance(value, tuple):
                filename, data = value
                empty_fields[fieldname] = (filename, '')
            else:
                data = value
                empty_fields[fieldname] = ''

            if hasattr(data, '__len__'):
                size += len(data)
            elif isinstance(data, int):
                size += len(str(data))
            elif hasattr(data, 'seek'):
                size += file_size(data)
            elif hasattr(data, 'read'):
                size += len(data.read()) # This is undesired
            elif hasattr(data, '__iter__'):
                size += sum(len(chunk) for chunk in data) # This is also undesired
            else:
                size += len(unicode(data)) # Hope for the best

        return size + sum(len(chunk) for chunk in iter(MultipartEncoderGenerator(empty_fields, boundary=self.boundary)))

    def __iter__(self):

        for fieldname, value in self.fields.iteritems():
            yield encode(u'--%s\r\n' % (self.boundary))

            if isinstance(value, tuple):
                filename, data = value
                yield encode(u'Content-Disposition: form-data; name="%s"; filename="%s"\r\n' % (fieldname, filename))
                yield encode(u'Content-Type: %s\r\n\r\n' % (get_content_type(filename)))
            else:
                data = value
                yield encode(u'Content-Disposition: form-data; name="%s"\r\n' % fieldname)
                yield encode(u'Content-Type: text/plain\r\n\r\n')

            if isinstance(data, unicode):
                yield encode(data)

            elif isinstance(data, int):
                # Handle integers for backwards compatibility
                yield str(data)

            elif hasattr(data, 'read'):
                # Stream from a file-like object
                while True:
                    chunk = data.read(self.chunk_size)
                    if not chunk:
                        break
                    yield encode(chunk)

            elif hasattr(data, '__iter__'):
                # Stream from an iterator
                for chunk in data:
                    yield encode(chunk)

            else:
                # Hope for the best
                yield unicode(data)

            yield encode(u'\r\n')

        yield encode(u'--%s--\r\n' % (self.boundary))


def encode_multipart_formdata(fields, boundary=None, chunk_size=8192):
    """
    ``fields`` is a dictionary where the parameter name is the key and the value
    is either a (filename, data) tuple or just data. Data can be a string, file-like
    object, or iterator.

    Example:
        fields = {
            'foo': 'bar',
            'upload_file': ('file.txt', 'data'),
            'huge_huge': ('video.mpg', fp),
            'hihihi_42_times': ('hi' for i in xrange(42)),
        }

    File-like objects are read ``chunk_size`` bytes at a time.

    If no ``boundary`` is given, a random one is chosen.

    See MultipartEncoderGenerator for more details.
    """
    stream = MultipartEncoderGenerator(fields, boundary=boundary, chunk_size=chunk_size)
    return IterStreamer(stream), stream.get_content_type()
