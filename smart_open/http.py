# -*- coding: utf-8 -*-
#
# Copyright (C) 2019 Radim Rehurek <me@radimrehurek.com>
#
# This code is distributed under the terms and conditions
# from the MIT License (MIT).
#
"""Implements file-like objects for reading from http."""

import io
import logging
import os.path
import urllib.parse
import time
import urllib3

import diskcache as dc
import redis

from os import environ

try:
    import requests
except ImportError:
    MISSING_DEPS = True

from smart_open import bytebuffer, constants
import smart_open.utils

DEFAULT_BUFFER_SIZE = 128 * 1024
SCHEMES = ("http", "https")

logger = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1 << 17

_HEADERS = {"Accept-Encoding": "identity"}
"""The headers we send to the server with every HTTP request.

For now, we ask the server to send us the files as they are.
Sometimes, servers compress the file for more efficient transfer, in which case
the client (us) has to decompress them with the appropriate algorithm.
"""

DEFAULT_DISKCACHE_SIZE = 1 << 30


def parse_uri(uri_as_string):
    split_uri = urllib.parse.urlsplit(uri_as_string)
    assert split_uri.scheme in SCHEMES

    uri_path = split_uri.netloc + split_uri.path
    uri_path = "/" + uri_path.lstrip("/")
    return dict(scheme=split_uri.scheme, uri_path=uri_path)


def open_uri(uri, mode, transport_params):
    kwargs = smart_open.utils.check_kwargs(open, transport_params)
    return open(uri, mode, **kwargs)


def open(
    uri,
    mode,
    kerberos=False,
    user=None,
    password=None,
    cert=None,
    headers=None,
    timeout=None,
    buffer_size=DEFAULT_BUFFER_SIZE,
    diskcache_dir=None,
    diskcache_size=None,
    redis_host=None,
    redis_port=None,
):
    """Implement streamed reader from a web site.

    Supports Kerberos and Basic HTTP authentication.

    Parameters
    ----------
    url: str
        The URL to open.
    mode: str
        The mode to open using.
    kerberos: boolean, optional
        If True, will attempt to use the local Kerberos credentials
    user: str, optional
        The username for authenticating over HTTP
    password: str, optional
        The password for authenticating over HTTP
    cert: str/tuple, optional
        if String, path to ssl client cert file (.pem). If Tuple, (‘cert’, ‘key’)
    headers: dict, optional
        Any headers to send in the request. If ``None``, the default headers are sent:
        ``{'Accept-Encoding': 'identity'}``. To use no headers at all,
        set this variable to an empty dict, ``{}``.
    buffer_size: int, optional
        The buffer size to use when performing I/O.

    Note
    ----
    If neither kerberos or (user, password) are set, will connect
    unauthenticated, unless set separately in headers.

    """
    if mode == constants.READ_BINARY:
        fobj = SeekableBufferedInputBase(
            uri,
            mode,
            buffer_size=buffer_size,
            kerberos=kerberos,
            user=user,
            password=password,
            cert=cert,
            headers=headers,
            timeout=timeout,
            diskcache_dir=None,
            diskcache_size=None,
            redis_host=None,
        )
        fobj.name = os.path.basename(urllib.parse.urlparse(uri).path)
        return fobj
    else:
        raise NotImplementedError("http support for mode %r not implemented" % mode)


def _get(url, range, auth, cert, headers, timeout):
    headers.update({"range": range})

    response = requests.get(
        url,
        auth=auth,
        stream=True,
        cert=cert,
        headers=headers,
        timeout=timeout,
    )

    return response.content


class BufferedInputBase(io.BufferedIOBase):
    def __init__(
        self,
        url,
        mode="r",
        buffer_size=DEFAULT_BUFFER_SIZE,
        kerberos=False,
        user=None,
        password=None,
        cert=None,
        headers=None,
        timeout=None,
    ):
        if kerberos:
            import requests_kerberos

            auth = requests_kerberos.HTTPKerberosAuth()
        elif user is not None and password is not None:
            auth = (user, password)
        else:
            auth = None

        self.buffer_size = buffer_size
        self.mode = mode

        if headers is None:
            self.headers = _HEADERS.copy()
        else:
            self.headers = headers

        self.timeout = timeout

        self.response = requests.get(
            url,
            auth=auth,
            cert=cert,
            stream=True,
            headers=self.headers,
            timeout=self.timeout,
        )

        if not self.response.ok:
            self.response.raise_for_status()

        self._read_iter = self.response.iter_content(self.buffer_size)
        self._read_buffer = bytebuffer.ByteBuffer(buffer_size)
        self._position = 0

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

    #
    # Override some methods from io.IOBase.
    #
    def close(self):
        """Flush and close this stream."""
        logger.debug("close: called")
        self.response = None
        self._read_iter = None

    def readable(self):
        """Return True if the stream can be read from."""
        return True

    def seekable(self):
        return False

    #
    # io.BufferedIOBase methods.
    #
    def detach(self):
        """Unsupported."""
        raise io.UnsupportedOperation

    def _read_chunk(self, chunk_pos):
        """Read a chunk from the specified file handle.

        Check if the chunk is in the cache first.
        """
        # make sure we have data in the cache
        if chunk_pos in self._reads:
            # print("cache hit", chunk_pos)
            return self._reads[chunk_pos]
        else:
            t1 = time.time()
            # check if it's in the disk cache
            cache_key = f"{self.url}.{self._chunk_size}.{chunk_pos}"
            if self._redis and cache_key in self._redis:
                self._cache_hits += 1
                self._reads[chunk_pos] = data = self._redis.get(cache_key)
                t2 = time.time()
                # print(f"redis hit {chunk_pos} {t2 - t1:.4f}")
                return data
            elif self._diskcache and cache_key in self._diskcache:
                # print("diskcache hit", chunk_pos)
                self._cache_hits += 1
                self._reads[chunk_pos] = data = self._diskcache[cache_key]
                return self._diskcache[cache_key]
            else:
                data = _get(
                    url=self.url,
                    range=smart_open.utils.make_range_string(
                        chunk_pos * self._chunk_size,
                        (chunk_pos + 1) * self._chunk_size - 1,
                    ),
                    auth=self.auth,
                    cert=self.cert,
                    headers=self.headers,
                    timeout=self.timeout,
                )
                # print("chunk_size", self._chunk_size)
                # print(f"Read: {len(data)}")
                self._reads[chunk_pos] = data

                if self._diskcache is not None:
                    self._cache_misses += 1
                    self._diskcache[cache_key] = data
                if self._redis is not None:
                    self._cache_misses += 1
                    self._redis.set(cache_key, data)

                # Close the stream so that we don't try to read this chunk again
                # and end up with some data from the wrong position
                t2 = time.time()
                # print(f"cache miss {chunk_pos} {t2 - t1:.4f}")

        return data

    def _chunk_pos(self, position):
        """Return the chunk number for the given position."""
        return position // self._chunk_size

    def _chunked_read(self, position, size=None):
        """Read from the specified file handle in chunks.

        Check the local cache for a chunk before reading from the remote
        """
        logger.debug(f"chunked_read {position} {size}")
        remaining_size = self._content_length - position

        if not size or size > remaining_size:
            # The requested chunk boes beyond the end of the file?
            size = remaining_size
            # pass

        chunk_pos = self._chunk_pos(position)
        data = self._read_chunk(chunk_pos)
        # Get the part of the chunk that we need to return
        index = position - (chunk_pos * self._chunk_size)
        to_return = data[index : index + size]

        # Move on to the next chunk in preparation over iterating over
        # the remaining chunks necessary to fetch the full chunk
        chunk_pos += 1
        position = chunk_pos * self._chunk_size

        while position < self._content_length:
            remaining_size = size - len(to_return)
            if remaining_size <= 0:
                return to_return

            data = self._read_chunk(chunk_pos)
            index = position - (chunk_pos * self._chunk_size)
            to_return += data[index : index + remaining_size]
            chunk_pos += 1
            position = chunk_pos * self._chunk_size

        if size:
            if len(to_return) != size:
                raise AssertionError(
                    f"Length of data to be returned ({len(to_return)}) "
                    f"does not match requested length ({size}). "
                    f"Request at position: {position}. "
                    f"Content length: {self._content_length}"
                )
        return to_return

    def read(self, size=-1):
        """Read from the continuous connection with the remote peer."""
        if self._position >= self._content_length:
            return b""

        #
        # Boto3 has built-in error handling and retry mechanisms:
        #
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
        #
        # Unfortunately, it isn't always enough. There is still a non-zero
        # possibility that an exception will slip past these mechanisms and
        # terminate the read prematurely.  Luckily, at this stage, it's very
        # simple to recover from the problem: wait a little bit, reopen the
        # HTTP connection and try again.  Usually, a single retry attempt is
        # enough to recover, but we try multiple times "just in case".
        for attempt, seconds in enumerate([1, 2, 4, 8, 16], 1):
            try:
                if size == -1:
                    binary = self._chunked_read(self._position)
                else:
                    binary = self._chunked_read(self._position, size)
            except (urllib3.exceptions.HTTPError,) as err:
                logger.warning(
                    "%s: caught %r while reading %d bytes, sleeping %ds before retry",
                    self,
                    err,
                    size,
                    seconds,
                )
                time.sleep(seconds)
            else:
                self._position += len(binary)
                return binary

        raise IOError(
            "%s: failed to read %d bytes after %d attempts" % (self, size, attempt)
        )

    # def read(self, size=-1):
    #     """
    #     Mimics the read call to a filehandle object.
    #     """
    #     # logger.debug("reading with size: %d", size)
    #     print(f"reading with size: {size}")
    #     print(f"Current position: {self._position}")
    #     print("content length", self._content_length)
    #     if self.response is None:
    #         return b""

    #     if size == 0:
    #         return b""
    #     elif size < 0 and len(self._read_buffer) == 0:
    #         retval = self.response.raw.read()
    #     elif size < 0:
    #         retval = self._read_buffer.read() + self.response.raw.read()
    #     else:
    #         while len(self._read_buffer) < size:
    #             logger.debug(
    #                 "http reading more content at current_pos: %d with size: %d",
    #                 self._position,
    #                 size,
    #             )
    #             bytes_read = self._read_buffer.fill(self._read_iter)
    #             if bytes_read == 0:
    #                 # Oops, ran out of data early.
    #                 retval = self._read_buffer.read()
    #                 self._position += len(retval)

    #                 return retval

    #         # If we got here, it means we have enough data in the buffer
    #         # to return to the caller.
    #         retval = self._read_buffer.read(size)

    #     self._position += len(retval)
    #     return retval

    def read1(self, size=-1):
        """This is the same as read()."""
        return self.read(size=size)

    def readinto(self, b):
        """Read up to len(b) bytes into b, and return the number of bytes
        read."""
        data = self.read(len(b))
        if not data:
            return 0
        b[: len(data)] = data
        return len(data)


class SeekableBufferedInputBase(BufferedInputBase):
    """
    Implement seekable streamed reader from a web site.
    Supports Kerberos, client certificate and Basic HTTP authentication.
    """

    def __init__(
        self,
        url,
        mode="r",
        buffer_size=DEFAULT_BUFFER_SIZE,
        kerberos=False,
        user=None,
        password=None,
        cert=None,
        headers=None,
        timeout=None,
        chunk_size=DEFAULT_CHUNK_SIZE,
        diskcache_dir=None,
        diskcache_size=None,
        redis_host=None,
        redis_port=None,
    ):
        """
        If Kerberos is True, will attempt to use the local Kerberos credentials.
        If cert is set, will try to use a client certificate
        Otherwise, will try to use "basic" HTTP authentication via username/password.

        If none of those are set, will connect unauthenticated.
        """
        self.url = url

        if kerberos:
            import requests_kerberos

            self.auth = requests_kerberos.HTTPKerberosAuth()
        elif user is not None and password is not None:
            self.auth = (user, password)
        else:
            self.auth = None

        if headers is None:
            self.headers = _HEADERS.copy()
        else:
            self.headers = headers

        self.cert = cert
        self.timeout = timeout
        self._chunk_size = chunk_size

        self.buffer_size = buffer_size
        self.mode = mode
        self.response = self._partial_request()

        if not self.response.ok:
            self.response.raise_for_status()

        logger.debug("self.response: %r, raw: %r", self.response, self.response.raw)

        self._content_length = int(self.response.headers.get("Content-Length", -1))
        #
        # We assume the HTTP stream is seekable unless the server explicitly
        # tells us it isn't.  It's better to err on the side of "seekable"
        # because we don't want to prevent users from seeking a stream that
        # does not appear to be seekable but really is.
        #
        self._seekable = (
            self.response.headers.get("Accept-Ranges", "").lower() != "none"
        )

        self._read_iter = self.response.iter_content(self.buffer_size)
        self._read_buffer = bytebuffer.ByteBuffer(buffer_size)
        self._position = 0
        self._reads = {}

        #
        # This member is part of the io.BufferedIOBase interface.
        #
        self.raw = None

        self._chunk_size = chunk_size or int(
            environ.get("SMART_OPEN_CHUNK_SIZE", DEFAULT_CHUNK_SIZE)
        )

        logger.info("chunk_size: %d", chunk_size)

        if diskcache_size and not diskcache_dir:
            raise ValueError("diskcache_size requires diskcache_dir")
        if redis_host and diskcache_dir:
            raise ValueError(
                "Please specify one but not both of redis_host and diskcache_dir"
            )

        self._diskcache = None
        self._redis = None

        logger.info("diskcache_dir: %s", diskcache_dir)
        logger.info("diskcache_size: %s", diskcache_size)

        redis_host = redis_host or environ.get("SMART_OPEN_REDIS_HOST")
        redis_port = redis_port or environ.get("SMART_OPEN_REDIS_PORT")

        logger.info("redis_host: %s", redis_host)
        logger.info("redis_port: %s", redis_port)

        if diskcache_dir:
            if not diskcache_size:
                logger.info("diskcache_size not specified, using default of 1GB")
                diskcache_size = DEFAULT_DISKCACHE_SIZE
            self._diskcache = dc.Cache(diskcache_dir, size_limit=diskcache_size)

        if redis_host:
            if not redis_port:
                redis_port = 6379

            self._redis = redis.Redis(host=redis_host, port=redis_port)

        self._cache_hits = 0
        self._cache_misses = 0

    def seek(self, offset, whence=constants.WHENCE_START):
        """Seek to the specified position.

        :param int offset: The offset in bytes.
        :param int whence: Where the offset is from.

        :returns: the position after seeking.
        :rtype: int
        """
        # print("seek", offset)
        if whence not in constants.WHENCE_CHOICES:
            raise ValueError(
                "invalid whence, expected one of %r" % constants.WHENCE_CHOICES
            )

        if whence == constants.WHENCE_START:
            self._position = max(0, offset)
        elif whence == constants.WHENCE_CURRENT:
            self._position += offset
        else:
            self._position = max(0, self._content_length + offset)

        if self._position > self._content_length:
            self._position = self._content_length

        return self._position

    def tell(self):
        return self._position

    def seekable(self, *args, **kwargs):
        return self._seekable

    def truncate(self, size=None):
        """Unsupported."""
        raise io.UnsupportedOperation

    def _partial_request(self, start_pos=None):
        if start_pos is not None:
            range_str = smart_open.utils.make_range_string(start_pos)
            self.headers.update({"range": range_str})

        response = requests.get(
            self.url,
            auth=self.auth,
            stream=True,
            cert=self.cert,
            headers=self.headers,
            timeout=self.timeout,
        )
        return response
