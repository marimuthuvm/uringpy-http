# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: freethreading_compatible=True
"""
uringpy_http._core: an io_uring HTTP/1.1 server whose transport and HTTP
parser run in C with the GIL released.

One Engine is one worker: one io_uring ring, one listening socket, and a
connection table indexed by file descriptor. Nothing in it is shared with
other workers, so the data path needs no locks.

Per connection the reactor:

  recv   -> into a buffer owned by the connection (grows as needed)
  parse  -> picohttpparser, headers lowercased, the target percent-decoded,
            Content-Length and chunked bodies collected, all without the GIL
  run    -> the GIL is taken once per request, or once per pass over the
            completion queue for every request found in it (GIL batching),
            and the ASGI app is started as an eager asyncio Task; a request
            that never waits on I/O completes inside that one acquisition
  send   -> the response bytes the app produced, sent without the GIL

A Task that does wait on I/O is finished by the worker's own asyncio loop,
which the reactor runs one step at a time whenever it has work: the ring
polls the loop's selector and wakes up for its timers.

Requests on one connection are handled one at a time; pipelined requests
are parsed from the buffer as soon as the previous response has been sent.
Responses are assembled in full before they are sent.
"""

import asyncio
import contextvars
import sys
import traceback
from asyncio import events as _aio_events
from http import HTTPStatus

from libc.stdlib cimport malloc, free, realloc
from libc.string cimport memcpy, memmove, memset, memcmp, memchr
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t, int32_t, int64_t
from libc.errno cimport EINTR, ETIME
from cpython.ref cimport PyObject
from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsStringAndSize, PyBytes_GET_SIZE
from cpython.unicode cimport PyUnicode_DecodeUTF8, PyUnicode_DecodeLatin1
from cpython.exc cimport PyErr_CheckSignals
from posix.time cimport clock_gettime, timespec, CLOCK_MONOTONIC

cdef extern from "<time.h>" nogil:
    ctypedef long time_t
    time_t time(time_t *t)

cdef extern from "<sys/socket.h>" nogil:
    ctypedef unsigned int socklen_t
    struct sockaddr:
        unsigned short sa_family
    struct sockaddr_storage:
        unsigned short ss_family
    enum: AF_INET
    enum: AF_INET6

cdef extern from "<netinet/in.h>" nogil:
    struct in_addr:
        pass
    struct in6_addr:
        pass
    struct sockaddr_in:
        unsigned short sin_family
        unsigned short sin_port
        in_addr sin_addr
    struct sockaddr_in6:
        unsigned short sin6_family
        unsigned short sin6_port
        in6_addr sin6_addr
    unsigned short ntohs(unsigned short)

cdef extern from "<arpa/inet.h>" nogil:
    const char *inet_ntop(int af, const void *src, char *dst, socklen_t size)

cdef extern from "<sys/eventfd.h>" nogil:
    int eventfd(unsigned int initval, int flags)
    enum: EFD_CLOEXEC

cdef extern from "<sys/socket.h>" nogil:
    int shutdown(int fd, int how)
    enum: SHUT_WR

cdef extern from "<unistd.h>" nogil:
    int close(int fd)
    ssize_t write(int fd, const void *buf, size_t n)

cdef extern from "<poll.h>" nogil:
    enum: POLLIN

cdef extern from "liburing.h" nogil:
    struct io_uring_sqe:
        uint64_t user_data
    struct io_uring_cqe:
        uint64_t user_data
        int32_t res
        uint32_t flags
    struct io_uring:
        pass
    struct __kernel_timespec:
        int64_t tv_sec
        long long tv_nsec
    int io_uring_queue_init(unsigned entries, io_uring *ring, unsigned flags)
    void io_uring_queue_exit(io_uring *ring)
    io_uring_sqe *io_uring_get_sqe(io_uring *ring)
    int io_uring_submit(io_uring *ring)
    int io_uring_submit_and_wait(io_uring *ring, unsigned wait_nr)
    int io_uring_submit_and_wait_timeout(io_uring *ring, io_uring_cqe **cqe_ptr,
                                         unsigned wait_nr, __kernel_timespec *ts,
                                         void *sigmask)
    int io_uring_peek_cqe(io_uring *ring, io_uring_cqe **cqe_ptr)
    void io_uring_cqe_seen(io_uring *ring, io_uring_cqe *cqe)
    void io_uring_prep_accept(io_uring_sqe *sqe, int fd, sockaddr *addr,
                              socklen_t *addrlen, int flags)
    void io_uring_prep_recv(io_uring_sqe *sqe, int fd, void *buf, size_t n, int flags)
    void io_uring_prep_send(io_uring_sqe *sqe, int fd, const void *buf, size_t n, int flags)
    void io_uring_prep_read(io_uring_sqe *sqe, int fd, void *buf, unsigned n, uint64_t off)
    void io_uring_prep_poll_add(io_uring_sqe *sqe, int fd, unsigned poll_mask)

cdef extern from "picohttpparser.h" nogil:
    struct phr_header:
        const char *name
        size_t name_len
        const char *value
        size_t value_len
    struct phr_chunked_decoder:
        size_t bytes_left_in_chunk
        char consume_trailer
    int phr_parse_request(const char *buf, size_t len, const char **method,
                          size_t *method_len, const char **path, size_t *path_len,
                          int *minor_version, phr_header *headers,
                          size_t *num_headers, size_t last_len)
    ssize_t phr_decode_chunked(phr_chunked_decoder *decoder, char *buf, size_t *bufsz)


# --- constants -----------------------------------------------------------------

cdef enum:
    OP_ACCEPT = 1
    OP_RECV = 2
    OP_SEND = 3
    OP_CONTINUE = 4   # "100 Continue" interim response
    OP_POLL = 5       # the worker loop's selector became readable
    OP_WAKE = 6       # eventfd written by stop()
    MAX_HEADERS = 100
    MAX_HEADER_BYTES = 65536
    RECV_CHUNK = 16384
    PEND_CAP = 4096

# Connection states.
cdef enum:
    ST_FREE = 0
    ST_READ = 1       # reading a request
    ST_APP = 2        # request handed to the app, response not complete
    ST_SEND = 3       # response being sent
    ST_DEAD = 4       # closed while the app still runs (generation bumped)

cdef const char *CONTINUE_RESP = b"HTTP/1.1 100 Continue\r\n\r\n"
cdef size_t CONTINUE_LEN = 25


ctypedef struct conn_t:
    int state
    unsigned gen               # bumped on every close; stale app callbacks see it
    unsigned reqno             # bumped for every request dispatched to the app
    char *rbuf
    size_t rcap
    size_t rlen
    char *wbuf
    size_t wcap
    size_t wlen
    # the request being read / served
    int hdr_parsed
    size_t hdr_len
    size_t need                # total bytes required before dispatch (Content-Length)
    size_t body_len
    size_t req_total           # bytes of this request in rbuf (headers + decoded body)
    int chunked                # 0 none, 1 decoding
    size_t chunk_raw
    phr_chunked_decoder dec
    int keep_alive
    int http10
    int is_head
    int expect_continue
    # parsed request, valid from dispatch until the response is sent
    const char *method
    size_t method_len
    const char *target
    size_t target_len
    size_t path_len            # target_len up to '?'
    phr_header headers[MAX_HEADERS]
    size_t nheaders
    char *dpath                # percent-decoded path
    size_t dpath_cap
    size_t dpath_len
    # response state
    int resp_started
    int resp_hdr_done
    int resp_done
    int resp_chunked
    int resp_has_cl
    int resp_close
    int static_send            # sending the engine's static response, not wbuf
    int awaiting               # the app is suspended in the worker loop
    size_t send_off            # bytes of the response sent so far
    int linger                 # error response: drain the socket before closing
    size_t drained
    # peer address, formatted at accept
    char peer_host[48]
    int peer_port
    int peer_ok


cdef inline unsigned long long _now_ns() noexcept nogil:
    cdef timespec ts
    clock_gettime(CLOCK_MONOTONIC, &ts)
    return <unsigned long long>ts.tv_sec * 1000000000ULL + <unsigned long long>ts.tv_nsec


cdef inline unsigned char _lower(unsigned char ch) noexcept nogil:
    if ch >= 65 and ch <= 90:
        return ch + 32
    return ch


cdef inline int _ieq(const char *a, size_t alen, const char *lit, size_t llen) noexcept nogil:
    # case-insensitive compare of a against a lowercase literal
    cdef size_t i
    if alen != llen:
        return 0
    for i in range(alen):
        if _lower(<unsigned char>a[i]) != <unsigned char>lit[i]:
            return 0
    return 1


cdef inline int _icontains(const char *a, size_t alen, const char *lit, size_t llen) noexcept nogil:
    # case-insensitive substring test (token lists such as "keep-alive, Upgrade")
    cdef size_t i, j
    if llen > alen:
        return 0
    for i in range(alen - llen + 1):
        j = 0
        while j < llen and _lower(<unsigned char>a[i + j]) == <unsigned char>lit[j]:
            j += 1
        if j == llen:
            return 1
    return 0


cdef inline int _hexval(char ch) noexcept nogil:
    if ch >= 48 and ch <= 57:
        return ch - 48
    if ch >= 97 and ch <= 102:
        return ch - 87
    if ch >= 65 and ch <= 70:
        return ch - 55
    return -1


cdef inline int _grow(char **buf, size_t *cap, size_t need) noexcept nogil:
    cdef size_t ncap
    cdef char *nb
    if need <= cap[0]:
        return 0
    ncap = cap[0] * 2 if cap[0] else 4096
    while ncap < need:
        ncap *= 2
    nb = <char*>realloc(buf[0], ncap)
    if nb == NULL:
        return -1
    buf[0] = nb
    cap[0] = ncap
    return 0


cdef inline int _put(conn_t *c, const char *s, size_t n) noexcept nogil:
    if _grow(&c.wbuf, &c.wcap, c.wlen + n) < 0:
        return -1
    memcpy(c.wbuf + c.wlen, s, n)
    c.wlen += n
    return 0


cdef inline int _put_uint(conn_t *c, unsigned long long v, int base) noexcept nogil:
    cdef char tmp[24]
    cdef int i = 24
    cdef const char *digits = b"0123456789abcdef"
    if v == 0:
        return _put(c, b"0", 1)
    while v > 0:
        i -= 1
        tmp[i] = digits[v % base]
        v //= base
    return _put(c, tmp + i, 24 - i)


cdef const char *_WDAY[7]
cdef const char *_MON[12]
_WDAY[:] = [b"Sun", b"Mon", b"Tue", b"Wed", b"Thu", b"Fri", b"Sat"]
_MON[:] = [b"Jan", b"Feb", b"Mar", b"Apr", b"May", b"Jun", b"Jul", b"Aug",
           b"Sep", b"Oct", b"Nov", b"Dec"]


cdef void _format_date(time_t t, char *out) noexcept nogil:
    # "date: Wed, 14 Oct 2026 17:52:00 GMT\r\n" (37 bytes), from epoch seconds,
    # without locale or libc time zone state.
    cdef long long days = t // 86400
    cdef long long secs = t - days * 86400
    cdef int wday = <int>((days + 4) % 7)
    cdef long long z = days + 719468
    cdef long long era = (z if z >= 0 else z - 146096) // 146097
    cdef long long doe = z - era * 146097
    cdef long long yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    cdef long long y = yoe + era * 400
    cdef long long doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    cdef long long mp = (5 * doy + 2) // 153
    cdef int d = <int>(doy - (153 * mp + 2) // 5 + 1)
    cdef int m = <int>(mp + 3 if mp < 10 else mp - 9)
    cdef int hh = <int>(secs // 3600), mi = <int>((secs % 3600) // 60), ss = <int>(secs % 60)
    if m <= 2:
        y += 1
    memcpy(out, b"date: ", 6)
    memcpy(out + 6, _WDAY[wday], 3)
    out[9] = 44; out[10] = 32
    out[11] = 48 + d // 10; out[12] = 48 + d % 10; out[13] = 32
    memcpy(out + 14, _MON[m - 1], 3)
    out[17] = 32
    out[18] = 48 + <int>(y // 1000) % 10; out[19] = 48 + <int>(y // 100) % 10
    out[20] = 48 + <int>(y // 10) % 10; out[21] = 48 + <int>(y % 10); out[22] = 32
    out[23] = 48 + hh // 10; out[24] = 48 + hh % 10; out[25] = 58
    out[26] = 48 + mi // 10; out[27] = 48 + mi % 10; out[28] = 58
    out[29] = 48 + ss // 10; out[30] = 48 + ss % 10
    memcpy(out + 31, b" GMT\r\n", 6)


# Status lines, built once.
cdef list _STATUS = [None] * 600
for _code in range(100, 600):
    try:
        _phrase = HTTPStatus(_code).phrase
    except ValueError:
        _phrase = ""
    _STATUS[_code] = ("HTTP/1.1 %d %s\r\n" % (_code, _phrase)).encode("latin-1")
del _code, _phrase

cdef bytes _ERR_400 = b"HTTP/1.1 400 Bad Request\r\ncontent-type: text/plain\r\ncontent-length: 11\r\nconnection: close\r\n\r\nBad Request"
cdef bytes _ERR_413 = b"HTTP/1.1 413 Content Too Large\r\ncontent-type: text/plain\r\ncontent-length: 17\r\nconnection: close\r\n\r\nContent Too Large"
cdef bytes _ERR_431 = b"HTTP/1.1 431 Request Header Fields Too Large\r\ncontent-type: text/plain\r\ncontent-length: 31\r\nconnection: close\r\n\r\nRequest Header Fields Too Large"
cdef bytes _ERR_500 = b"HTTP/1.1 500 Internal Server Error\r\ncontent-type: text/plain\r\ncontent-length: 21\r\nconnection: close\r\n\r\nInternal Server Error"
cdef bytes _ERR_501 = b"HTTP/1.1 501 Not Implemented\r\ncontent-type: text/plain\r\ncontent-length: 15\r\nconnection: close\r\n\r\nNot Implemented"
cdef const char *ERR_400 = _ERR_400
cdef const char *ERR_413 = _ERR_413
cdef const char *ERR_431 = _ERR_431
cdef const char *ERR_500 = _ERR_500
cdef const char *ERR_501 = _ERR_501
cdef size_t ERR_400_LEN = len(_ERR_400)
cdef size_t ERR_413_LEN = len(_ERR_413)
cdef size_t ERR_431_LEN = len(_ERR_431)
cdef size_t ERR_500_LEN = len(_ERR_500)
cdef size_t ERR_501_LEN = len(_ERR_501)


# --- awaitables used on the fast path --------------------------------------------

cdef class _Ready:
    """An awaitable that is already complete: awaiting it never suspends."""
    cdef object value

    def __await__(self):
        return self

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration(self.value)

    def send(self, v):
        raise StopIteration(self.value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            raise typ
        raise val

    def close(self):
        pass


cdef inline _Ready _ready(object v):
    cdef _Ready r = _Ready.__new__(_Ready)
    r.value = v
    return r


cdef _Ready _DONE = _ready(None)
cdef dict _DISCONNECT_TEMPLATE = {"type": "http.disconnect"}


class ClientDisconnected(OSError):
    """send() was called after the client had gone (ASGI spec 2.4)."""


class WorkerLoop(asyncio.SelectorEventLoop):
    """The worker's own loop. While the reactor starts requests it claims to be
    running, so that eager tasks start synchronously and asyncio APIs find it."""
    _uh_stepping = False

    def is_running(self):
        return self._uh_stepping or asyncio.SelectorEventLoop.is_running(self)


def _noop():
    pass


# --- per-connection Python state ---------------------------------------------------

cdef class _PyConn:
    """receive() and send() for one connection, reused across its requests."""
    cdef Engine engine
    cdef int fd
    cdef unsigned gen
    cdef public object client
    cdef bint body_given
    cdef object disc_fut
    cdef public object recv_cb
    cdef public object send_cb

    def __cinit__(self, Engine engine, int fd, unsigned gen, object client):
        self.engine = engine
        self.fd = fd
        self.gen = gen
        self.client = client
        self.body_given = False
        self.disc_fut = None
        self.recv_cb = self.receive
        self.send_cb = self.send

    cdef inline void new_request(self):
        self.body_given = False
        self.disc_fut = None

    cdef void disconnected(self):
        cdef object f = self.disc_fut
        if f is not None and not f.done():
            f.set_result({"type": "http.disconnect"})
        self.disc_fut = None

    async def _receive_slow(self):
        return await self.disc_fut

    def receive(self):
        cdef conn_t *c = &self.engine.conns[self.fd]
        if not self.body_given and c.gen == self.gen:
            self.body_given = True
            return _ready({"type": "http.request",
                           "body": PyBytes_FromStringAndSize(c.rbuf + c.hdr_len, c.body_len),
                           "more_body": False})
        if c.gen != self.gen or c.resp_done:
            return _ready(dict(_DISCONNECT_TEMPLATE))
        if self.disc_fut is None:
            self.disc_fut = self.engine._loop.create_future()
        return self.disc_fut

    def send(self, message):
        cdef conn_t *c = &self.engine.conns[self.fd]
        cdef str t
        if c.gen != self.gen:
            raise ClientDisconnected("client disconnected")
        t = message["type"]
        if t == "http.response.body":
            if not c.resp_started:
                raise RuntimeError("http.response.body sent before http.response.start")
            if c.resp_done:
                raise RuntimeError("response already completed")
            self.engine._resp_body(c, message.get("body", b""), message.get("more_body", False))
            if c.resp_done:
                self.disconnected_later()
                if c.awaiting:
                    c.awaiting = 0
                    self.engine._push_ready(self.fd)
        elif t == "http.response.start":
            if c.resp_started:
                raise RuntimeError("http.response.start sent twice")
            self.engine._resp_start(c, message["status"], message.get("headers", ()))
        else:
            raise RuntimeError("unsupported ASGI message type %r" % (t,))
        return _DONE

    cdef inline void disconnected_later(self):
        # Once the response is complete, a pending receive() resolves to
        # http.disconnect, as the spec allows.
        if self.disc_fut is not None:
            self.disconnected()


# --- the engine --------------------------------------------------------------------

cdef class Engine:
    cdef io_uring ring
    cdef bint ring_ok
    cdef conn_t *conns
    cdef int nconn
    cdef list pyconns
    cdef object app
    cdef public object _loop
    cdef object _set_running
    cdef object _task_cls
    cdef object _Context
    cdef object server_addr
    cdef str root_path
    cdef object state
    cdef bint has_state
    cdef size_t max_body
    cdef int wake_fd
    cdef uint64_t wake_val
    cdef int listen_fd
    cdef sockaddr_storage acc_addr
    cdef socklen_t acc_len
    cdef char date_line[40]
    cdef time_t date_sec
    # mode
    cdef bint gil_batch
    cdef int pend_cap
    cdef int *pend
    cdef int npend
    cdef int *ready
    cdef int nready
    cdef int ready_cap
    cdef bint static_mode
    cdef char *static_resp
    cdef size_t static_len
    cdef bint gil_timing
    cdef int loop_fd
    cdef bint poll_armed
    cdef bint loop_wake
    cdef int suspended
    cdef long long loop_timeout_ns
    cdef int _stop
    cdef int serving
    cdef object pending_exc
    # counters
    cdef public unsigned long long n_requests
    cdef public unsigned long long n_enters
    cdef public unsigned long long n_completions
    cdef public unsigned long long n_gil_acquires
    cdef public unsigned long long n_suspended
    cdef public unsigned long long n_app_errors
    cdef public unsigned long long n_bad_requests
    cdef public unsigned long long n_closes
    cdef public unsigned long long gil_hold_ns
    cdef public unsigned long long gil_wait_ns

    def __cinit__(self, unsigned entries=2048):
        cdef int ret = io_uring_queue_init(entries, &self.ring, 0)
        if ret < 0:
            raise OSError(-ret, "io_uring_queue_init failed")
        self.ring_ok = True
        self.conns = NULL
        self.nconn = 0
        self.pend = NULL
        self.ready = NULL
        self.static_resp = NULL
        self.wake_fd = eventfd(0, EFD_CLOEXEC)
        if self.wake_fd < 0:
            raise OSError("eventfd failed")
        self.pend_cap = PEND_CAP
        self.loop_fd = -1
        self.date_sec = 0

    def __init__(self, unsigned entries=2048):
        self.pyconns = []
        self.max_body = 16 * 1024 * 1024
        self.root_path = ""
        self.state = None
        self.has_state = False
        self.server_addr = None
        self.pending_exc = None
        self._set_running = _aio_events._set_running_loop
        self._task_cls = asyncio.Task
        self._Context = contextvars.Context

    def __dealloc__(self):
        cdef int i
        if self.conns != NULL:
            for i in range(self.nconn):
                free(self.conns[i].rbuf)
                free(self.conns[i].wbuf)
                free(self.conns[i].dpath)
            free(self.conns)
        free(self.pend)
        free(self.ready)
        free(self.static_resp)
        if self.wake_fd >= 0:
            close(self.wake_fd)
        if self.ring_ok:
            io_uring_queue_exit(&self.ring)

    # -- configuration --

    def configure(self, *, gil_batch=True, max_requests=0, max_body=16 * 1024 * 1024,
                  root_path="", gil_timing=False, server=None, state=None):
        """gil_batch: take the GIL once per pass for all requests found in it
        (default) instead of once per request. max_requests caps the requests
        started under one acquisition (0 = no cap)."""
        if self.serving:
            raise RuntimeError("cannot configure while serving")
        self.gil_batch = bool(gil_batch)
        self.pend_cap = PEND_CAP if not max_requests or max_requests > PEND_CAP else int(max_requests)
        self.max_body = max_body
        self.root_path = root_path
        self.gil_timing = bool(gil_timing)
        self.server_addr = server
        self.state = state
        self.has_state = state is not None

    def set_static_response(self, bytes response):
        """Answer every request with these bytes (a complete HTTP response),
        without entering Python: the transport and the parser alone."""
        cdef Py_ssize_t n = len(response)
        if n <= 0:
            raise ValueError("empty response")
        free(self.static_resp)
        self.static_resp = <char*>malloc(n)
        if self.static_resp == NULL:
            raise MemoryError()
        memcpy(self.static_resp, <const char*>response, n)
        self.static_len = n
        self.static_mode = True

    def stop(self):
        """Stop serving; safe to call from any thread."""
        cdef uint64_t one = 1
        self._stop = 1
        with nogil:
            write(self.wake_fd, &one, 8)

    def stats(self):
        return {"requests": self.n_requests, "enters": self.n_enters,
                "completions": self.n_completions, "gil_acquires": self.n_gil_acquires,
                "suspended": self.n_suspended, "app_errors": self.n_app_errors,
                "bad_requests": self.n_bad_requests, "closes": self.n_closes,
                "gil_hold_ns": self.gil_hold_ns, "gil_wait_ns": self.gil_wait_ns}

    # -- connection table --

    cdef int _ensure(self, int fd) noexcept nogil:
        cdef int n, i
        cdef conn_t *nc
        if fd < self.nconn:
            return 0
        n = self.nconn * 2 if self.nconn else 1024
        while n <= fd:
            n *= 2
        nc = <conn_t*>realloc(self.conns, n * sizeof(conn_t))
        if nc == NULL:
            return -1
        memset(nc + self.nconn, 0, (n - self.nconn) * sizeof(conn_t))
        self.conns = nc
        self.nconn = n
        return 0

    cdef inline io_uring_sqe *_sqe(self) noexcept nogil:
        cdef io_uring_sqe *sqe = io_uring_get_sqe(&self.ring)
        if sqe == NULL:
            io_uring_submit(&self.ring)
            self.n_enters += 1
            sqe = io_uring_get_sqe(&self.ring)
        return sqe

    cdef void _arm_accept(self) noexcept nogil:
        cdef io_uring_sqe *sqe = self._sqe()
        if sqe == NULL:
            return
        self.acc_len = sizeof(sockaddr_storage)
        io_uring_prep_accept(sqe, self.listen_fd, <sockaddr*>&self.acc_addr, &self.acc_len, 0)
        sqe.user_data = <uint64_t>OP_ACCEPT << 56

    cdef void _arm_wake(self) noexcept nogil:
        cdef io_uring_sqe *sqe = self._sqe()
        if sqe == NULL:
            return
        io_uring_prep_read(sqe, self.wake_fd, &self.wake_val, 8, 0)
        sqe.user_data = <uint64_t>OP_WAKE << 56

    cdef void _arm_poll(self) noexcept nogil:
        cdef io_uring_sqe *sqe
        if self.loop_fd < 0 or self.poll_armed:
            return
        sqe = self._sqe()
        if sqe == NULL:
            return
        io_uring_prep_poll_add(sqe, self.loop_fd, POLLIN)
        sqe.user_data = <uint64_t>OP_POLL << 56
        self.poll_armed = True

    cdef void _close(self, int fd) noexcept nogil:
        cdef conn_t *c = &self.conns[fd]
        close(fd)
        self.n_closes += 1
        c.gen += 1
        if c.state == ST_APP:
            # The app still holds this request; its next send() raises.
            c.state = ST_DEAD
        else:
            c.state = ST_FREE
        c.rlen = 0
        c.wlen = 0
        c.hdr_parsed = 0
        c.chunked = 0
        c.awaiting = 0
        c.linger = 0

    cdef void _arm_recv(self, int fd) noexcept nogil:
        cdef conn_t *c = &self.conns[fd]
        cdef io_uring_sqe *sqe
        if c.rcap - c.rlen < 2048:
            if _grow(&c.rbuf, &c.rcap, c.rlen + RECV_CHUNK) < 0:
                self._close(fd)
                return
        sqe = self._sqe()
        if sqe == NULL:
            self._close(fd)
            return
        io_uring_prep_recv(sqe, fd, c.rbuf + c.rlen, c.rcap - c.rlen, 0)
        sqe.user_data = (<uint64_t>OP_RECV << 56) | (<uint64_t>(<uint32_t>fd) << 24)

    cdef void _arm_send(self, int fd, size_t off) noexcept nogil:
        cdef conn_t *c = &self.conns[fd]
        cdef io_uring_sqe *sqe = self._sqe()
        cdef const char *base
        cdef size_t total
        if sqe == NULL:
            self._close(fd)
            return
        if c.static_send:
            base = self.static_resp
            total = self.static_len
        else:
            base = c.wbuf
            total = c.wlen
        io_uring_prep_send(sqe, fd, base + off, total - off, 0)
        sqe.user_data = (<uint64_t>OP_SEND << 56) | (<uint64_t>(<uint32_t>fd) << 24)
        c.send_off = off
        c.state = ST_SEND

    cdef void _send_error(self, int fd, const char *resp, size_t n) noexcept nogil:
        cdef conn_t *c = &self.conns[fd]
        self.n_bad_requests += 1
        c.linger = 1
        c.wlen = 0
        c.static_send = 0
        c.resp_close = 1
        c.keep_alive = 0
        if _put(c, resp, n) < 0:
            self._close(fd)
            return
        self._arm_send(fd, 0)

    cdef void _arm_continue(self, int fd) noexcept nogil:
        cdef io_uring_sqe *sqe = self._sqe()
        if sqe == NULL:
            return
        io_uring_prep_send(sqe, fd, CONTINUE_RESP, CONTINUE_LEN, 0)
        sqe.user_data = (<uint64_t>OP_CONTINUE << 56) | (<uint64_t>(<uint32_t>fd) << 24)

    # -- HTTP parsing, no GIL --

    cdef int _scan_headers(self, conn_t *c, phr_header *h, size_t nh) noexcept nogil:
        # Lowercase header names in place and pick out the framing headers.
        # Returns 0, or 400/501 for a request we refuse.
        cdef size_t i, k
        cdef char *name
        cdef unsigned long long cl
        cdef int have_cl = 0
        cdef int conn_close = 0, conn_keep = 0
        c.need = 0
        c.body_len = 0
        c.chunked = 0
        c.expect_continue = 0
        for i in range(nh):
            name = <char*>h[i].name
            if name == NULL:
                return 400   # obsolete line folding
            for k in range(h[i].name_len):
                name[k] = <char>_lower(<unsigned char>name[k])
            if h[i].name_len == 14 and memcmp(name, b"content-length", 14) == 0:
                if have_cl or h[i].value_len == 0 or h[i].value_len > 18:
                    return 400
                cl = 0
                for k in range(h[i].value_len):
                    if h[i].value[k] < 48 or h[i].value[k] > 57:
                        return 400
                    cl = cl * 10 + <unsigned long long>(h[i].value[k] - 48)
                have_cl = 1
                c.body_len = <size_t>cl
            elif h[i].name_len == 17 and memcmp(name, b"transfer-encoding", 17) == 0:
                if _ieq(h[i].value, h[i].value_len, b"chunked", 7):
                    c.chunked = 1
                else:
                    return 501
            elif h[i].name_len == 10 and memcmp(name, b"connection", 10) == 0:
                if _icontains(h[i].value, h[i].value_len, b"close", 5):
                    conn_close = 1
                if _icontains(h[i].value, h[i].value_len, b"keep-alive", 10):
                    conn_keep = 1
            elif h[i].name_len == 6 and memcmp(name, b"expect", 6) == 0:
                if _ieq(h[i].value, h[i].value_len, b"100-continue", 12):
                    c.expect_continue = 1
        if c.chunked and have_cl:
            return 400
        if c.http10:
            c.keep_alive = conn_keep and not conn_close
        else:
            c.keep_alive = not conn_close
        return 0

    cdef int _parse(self, int fd) noexcept nogil:
        # Try to complete the request at the start of rbuf. Returns 1 when it
        # is complete (and parsed into the conn), 0 when more bytes are
        # needed, and -1 when an error response has been queued.
        cdef conn_t *c = &self.conns[fd]
        cdef int r, minor = 0, err
        cdef size_t nh = MAX_HEADERS
        cdef ssize_t left
        cdef size_t sz, old_end
        if not c.hdr_parsed:
            r = phr_parse_request(c.rbuf, c.rlen, &c.method, &c.method_len,
                                  &c.target, &c.target_len, &minor,
                                  c.headers, &nh, 0)
            if r == -2:
                if c.rlen >= MAX_HEADER_BYTES:
                    self._send_error(fd, ERR_431, ERR_431_LEN)
                    return -1
                return 0
            if r < 0:
                self._send_error(fd, ERR_400, ERR_400_LEN)
                return -1
            c.hdr_len = <size_t>r
            c.http10 = minor == 0
            err = self._scan_headers(c, c.headers, nh)
            if err == 400:
                self._send_error(fd, ERR_400, ERR_400_LEN)
                return -1
            if err == 501:
                self._send_error(fd, ERR_501, ERR_501_LEN)
                return -1
            c.hdr_parsed = 1
            if c.chunked:
                memset(&c.dec, 0, sizeof(phr_chunked_decoder))
                c.dec.consume_trailer = 1
                c.chunk_raw = c.hdr_len
                c.body_len = 0
            else:
                if c.body_len > self.max_body:
                    self._send_error(fd, ERR_413, ERR_413_LEN)
                    return -1
                c.need = c.hdr_len + c.body_len
            if c.expect_continue and (c.chunked or c.rlen < c.need):
                self._arm_continue(fd)
        if c.chunked == 1:
            old_end = c.rlen
            sz = c.rlen - c.chunk_raw
            left = phr_decode_chunked(&c.dec, c.rbuf + c.chunk_raw, &sz)
            c.body_len += sz
            if left == -1:
                self._send_error(fd, ERR_400, ERR_400_LEN)
                return -1
            if c.body_len > self.max_body:
                self._send_error(fd, ERR_413, ERR_413_LEN)
                return -1
            if left == -2:
                c.rlen = c.hdr_len + c.body_len
                c.chunk_raw = c.rlen
                return 0
            # complete: picohttpparser has already moved the bytes after the
            # body (a pipelined request) to just behind the decoded body
            c.rlen = c.hdr_len + c.body_len + <size_t>left
            c.chunked = 2
            c.req_total = c.hdr_len + c.body_len
        else:
            if c.rlen < c.need:
                if _grow(&c.rbuf, &c.rcap, c.need + 1) < 0:
                    self._close(fd)
                    return -1
                return 0
            c.req_total = c.need
        # Complete. Re-parse the header block: rbuf may have moved while the
        # body arrived, and this refreshes every pointer into it.
        nh = MAX_HEADERS
        phr_parse_request(c.rbuf, c.hdr_len, &c.method, &c.method_len,
                          &c.target, &c.target_len, &minor, c.headers, &nh, 0)
        c.nheaders = nh   # names were lowercased in place on the first parse
        c.is_head = c.method_len == 4 and memcmp(c.method, b"HEAD", 4) == 0
        if self._decode_path(c) < 0:
            self._send_error(fd, ERR_400, ERR_400_LEN)
            return -1
        return 1

    cdef int _decode_path(self, conn_t *c) noexcept nogil:
        cdef const char *q = <const char*>memchr(c.target, 63, c.target_len)  # '?'
        cdef size_t plen = <size_t>(q - c.target) if q != NULL else c.target_len
        cdef size_t i = 0, o = 0
        cdef int hi, lo
        c.path_len = plen
        if _grow(&c.dpath, &c.dpath_cap, plen + 1) < 0:
            return -1
        while i < plen:
            if c.target[i] == 37 and i + 2 < plen:  # '%'
                hi = _hexval(c.target[i + 1])
                lo = _hexval(c.target[i + 2])
                if hi >= 0 and lo >= 0:
                    c.dpath[o] = <char>(hi * 16 + lo)
                    o += 1
                    i += 3
                    continue
            c.dpath[o] = c.target[i]
            o += 1
            i += 1
        c.dpath_len = o
        return 0

    cdef void _reset_response(self, conn_t *c) noexcept nogil:
        c.wlen = 0
        c.resp_started = 0
        c.resp_hdr_done = 0
        c.resp_done = 0
        c.resp_chunked = 0
        c.resp_has_cl = 0
        c.resp_close = 0
        c.static_send = 0
        c.awaiting = 0

    cdef void _push_pend(self, int fd) noexcept nogil:
        self.pend[self.npend] = fd
        self.npend += 1

    cdef void _push_ready(self, int fd) noexcept nogil:
        cdef int *nr
        if self.nready >= self.ready_cap:
            nr = <int*>realloc(self.ready, (self.ready_cap * 2 + 64) * sizeof(int))
            if nr == NULL:
                return
            self.ready = nr
            self.ready_cap = self.ready_cap * 2 + 64
        self.ready[self.nready] = fd
        self.nready += 1

    cdef void _on_request_data(self, int fd, unsigned long long *t_wait) noexcept nogil:
        # New bytes are in rbuf (or a response just went out and bytes were
        # left over). Dispatch the request if it is complete.
        cdef conn_t *c = &self.conns[fd]
        cdef int r = self._parse(fd)
        if r == 0:
            self._arm_recv(fd)
            return
        if r < 0:
            return
        self._reset_response(c)
        if self.static_mode:
            c.static_send = 1
            c.resp_close = not c.keep_alive
            self._arm_send(fd, 0)
            return
        c.state = ST_APP
        c.reqno += 1
        if self.gil_batch:
            self._push_pend(fd)
            if self.npend >= self.pend_cap:
                self._flush_pending(t_wait)
        else:
            self._push_pend(fd)
            self._flush_pending(t_wait)

    cdef void _finish_request(self, int fd) noexcept nogil:
        # The response has been sent in full.
        cdef conn_t *c = &self.conns[fd]
        if c.linger:
            # Unread request bytes would turn a close into a reset that can
            # destroy the error response in flight: stop writing, read until
            # the client closes (bounded), then close.
            shutdown(fd, SHUT_WR)
            c.rlen = 0
            c.drained = 0
            c.state = ST_READ
            self._arm_recv(fd)
            return
        self.n_requests += 1
        if c.resp_close or not c.keep_alive:
            self._close(fd)
            return
        # Drop this request from rbuf; keep any pipelined bytes behind it.
        if c.rlen > c.req_total:
            memmove(c.rbuf, c.rbuf + c.req_total, c.rlen - c.req_total)
            c.rlen -= c.req_total
        else:
            c.rlen = 0
        c.hdr_parsed = 0
        c.chunked = 0
        c.state = ST_READ
        c.wlen = 0
        c.static_send = 0
        if c.rlen > 0:
            self._on_request_data(fd, NULL)
        else:
            self._arm_recv(fd)

    # -- the GIL-held part --

    cdef void _flush_pending(self, unsigned long long *unused) noexcept nogil:
        # Start every pending request under one GIL acquisition, then queue
        # the responses that are complete.
        cdef int i, fd, n
        cdef unsigned long long t_req = 0
        if self.npend == 0 and self.nready == 0 and not self.loop_wake:
            return
        if self.gil_timing:
            t_req = _now_ns()
        self._under_gil(t_req)
        n = self.npend
        for i in range(n):
            fd = self.pend[i]
            if self.conns[fd].state == ST_APP and self.conns[fd].resp_done:
                self._arm_send(fd, 0)
        self.npend = 0
        self._drain_ready()

    cdef void _drain_ready(self) noexcept nogil:
        cdef int i, fd
        for i in range(self.nready):
            fd = self.ready[i]
            if self.conns[fd].state == ST_APP and self.conns[fd].resp_done:
                self._arm_send(fd, 0)
            elif self.conns[fd].state == ST_DEAD:
                self.conns[fd].state = ST_FREE
        self.nready = 0

    cdef int _under_gil(self, unsigned long long t_req) noexcept with gil:
        cdef unsigned long long t_in = 0
        cdef int i, fd
        self.n_gil_acquires += 1
        if t_req:
            t_in = _now_ns()
            self.gil_wait_ns += t_in - t_req
        try:
            if self.npend:
                self._loop._uh_stepping = True
                self._set_running(self._loop)
                try:
                    for i in range(self.npend):
                        self._start_app(self.pend[i])
                finally:
                    self._set_running(None)
                    self._loop._uh_stepping = False
            if self.suspended > 0 or self.loop_wake:
                self._loop_step()
        except BaseException as exc:
            if self.pending_exc is None:
                self.pending_exc = exc
            self._stop = 1
        if t_req:
            self.gil_hold_ns += _now_ns() - t_in
        return 0

    cdef _client_tuple(self, conn_t *c):
        if not c.peer_ok:
            return None
        return (PyUnicode_DecodeLatin1(c.peer_host, len(c.peer_host), NULL), c.peer_port)

    cdef object _build_scope(self, conn_t *c, _PyConn pc):
        cdef size_t i
        cdef list headers = []
        cdef object method
        cdef const char *m = c.method
        cdef size_t ml = c.method_len
        if ml == 3 and memcmp(m, b"GET", 3) == 0:
            method = "GET"
        elif ml == 4 and memcmp(m, b"POST", 4) == 0:
            method = "POST"
        else:
            method = PyUnicode_DecodeLatin1(m, ml, NULL)
        for i in range(c.nheaders):
            headers.append((PyBytes_FromStringAndSize(c.headers[i].name, c.headers[i].name_len),
                            PyBytes_FromStringAndSize(c.headers[i].value, c.headers[i].value_len)))
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.0" if c.http10 else "1.1",
            "server": self.server_addr,
            "client": pc.client,
            "scheme": "http",
            "method": method,
            "root_path": self.root_path,
            "path": PyUnicode_DecodeUTF8(c.dpath, c.dpath_len, "replace"),
            "raw_path": PyBytes_FromStringAndSize(c.target, c.path_len),
            "query_string": PyBytes_FromStringAndSize(c.target + c.path_len + 1,
                                                      c.target_len - c.path_len - 1)
                            if c.path_len < c.target_len else b"",
            "headers": headers,
        }
        if self.has_state:
            scope["state"] = self.state.copy()
        return scope

    cdef void _start_app(self, int fd):
        cdef conn_t *c = &self.conns[fd]
        cdef _PyConn pc
        while len(self.pyconns) <= fd:
            self.pyconns.append(None)
        pc = <_PyConn>self.pyconns[fd] if self.pyconns[fd] is not None else None
        if pc is None or pc.gen != c.gen:
            pc = _PyConn(self, fd, c.gen, self._client_tuple(c))
            self.pyconns[fd] = pc
        pc.new_request()
        try:
            scope = self._build_scope(c, pc)
            coro = self.app(scope, pc.recv_cb, pc.send_cb)
            task = self._task_cls(coro, loop=self._loop, eager_start=True,
                                  context=self._Context())
        except BaseException as exc:
            self._app_failed(c, exc)
            return
        if task.done():
            if task.cancelled():
                self._app_failed(c, asyncio.CancelledError())
            elif task.exception() is not None:
                self._app_failed(c, task.exception())
            else:
                self._app_returned(c)
        else:
            # Suspended on real I/O: the worker loop finishes it.
            self.suspended += 1
            self.n_suspended += 1
            if not c.resp_done:
                c.awaiting = 1
            task.add_done_callback(_TaskDone(self, fd, c.gen, c.reqno))

    cdef void _app_returned(self, conn_t *c):
        # The app is finished. A response it did not complete is replaced or
        # cut short and the connection closed.
        if c.resp_done:
            return
        if not c.resp_started:
            self.n_app_errors += 1
            c.wlen = 0
            _put(c, ERR_500, ERR_500_LEN)
        c.resp_close = 1
        c.resp_done = 1

    cdef void _report(self, exc):
        self.n_app_errors += 1
        if self.n_app_errors == 1:
            traceback.print_exception(exc, file=sys.stderr)

    cdef void _app_failed(self, conn_t *c, exc):
        self._report(exc)
        if not c.resp_started:
            c.wlen = 0
            _put(c, ERR_500, ERR_500_LEN)
        c.resp_close = 1
        c.resp_done = 1

    cdef void _task_done(self, int fd, unsigned gen, unsigned reqno, task):
        cdef conn_t *c = &self.conns[fd]
        self.suspended -= 1
        if c.gen == gen and c.reqno != reqno:
            # This request's response went out long ago and the connection
            # has moved on; the app was still running (background work).
            if not task.cancelled() and task.exception() is not None:
                self._report(task.exception())
            return
        if c.gen != gen:
            # the connection went away while the app ran
            if c.state == ST_DEAD:
                c.state = ST_FREE
            return
        if c.state != ST_APP:
            return    # response already sent; this was a background task
        if task.cancelled():
            self._app_failed(c, asyncio.CancelledError())
        elif task.exception() is not None:
            self._app_failed(c, task.exception())
        else:
            self._app_returned(c)
        if c.awaiting:
            c.awaiting = 0
            self._push_ready(fd)

    cdef void _loop_step(self):
        # Run one iteration of the worker loop without blocking.
        cdef object loop = self._loop
        cdef double delay
        self.loop_wake = False
        loop.call_soon(_noop)
        self._set_running(loop)
        try:
            loop._run_once()
        finally:
            self._set_running(None)
        # How long the reactor may sleep before the loop needs another step.
        try:
            if loop._ready:
                self.loop_timeout_ns = 0
            elif loop._scheduled:
                delay = loop._scheduled[0]._when - loop.time()
                self.loop_timeout_ns = 0 if delay <= 0 else <long long>(delay * 1e9) + 1
            else:
                self.loop_timeout_ns = -1
        except Exception:
            self.loop_timeout_ns = 1000000

    # -- response assembly (called from send(), GIL held) --

    cdef void _resp_start(self, conn_t *c, int status, headers) except *:
        cdef bytes line
        cdef const char *name
        cdef const char *value
        cdef Py_ssize_t nlen, vlen
        if status < 100 or status > 599:
            raise ValueError("invalid status %r" % (status,))
        line = <bytes>_STATUS[status]
        c.wlen = 0
        _put(c, line, len(line))
        for h in headers:
            hn, hv = h
            if not isinstance(hn, bytes) or not isinstance(hv, bytes):
                raise TypeError("header names and values must be bytes")
            PyBytes_AsStringAndSize(hn, <char**>&name, &nlen)
            PyBytes_AsStringAndSize(hv, <char**>&value, &vlen)
            if memchr(name, 13, nlen) or memchr(name, 10, nlen) or \
                    memchr(value, 13, vlen) or memchr(value, 10, vlen):
                raise ValueError("invalid header")
            if _ieq(name, nlen, b"content-length", 14):
                c.resp_has_cl = 1
            elif _ieq(name, nlen, b"transfer-encoding", 17):
                c.resp_chunked = 1
            elif _ieq(name, nlen, b"connection", 10) and \
                    _icontains(value, vlen, b"close", 5):
                c.resp_close = 1
            _put(c, name, nlen)
            _put(c, b": ", 2)
            _put(c, value, vlen)
            _put(c, b"\r\n", 2)
        c.resp_started = 1

    cdef void _resp_body(self, conn_t *c, body, more) except *:
        cdef const char *b
        cdef Py_ssize_t blen
        cdef bint more_body = bool(more)
        cdef time_t now
        if not isinstance(body, bytes):
            body = bytes(body)
        PyBytes_AsStringAndSize(body, <char**>&b, &blen)
        if not c.resp_hdr_done:
            if not c.resp_has_cl and not c.resp_chunked:
                if more_body:
                    _put(c, b"transfer-encoding: chunked\r\n", 28)
                    c.resp_chunked = 1
                else:
                    _put(c, b"content-length: ", 16)
                    _put_uint(c, <unsigned long long>blen, 10)
                    _put(c, b"\r\n", 2)
            now = time(NULL)
            if now != self.date_sec:
                _format_date(now, self.date_line)
                self.date_sec = now
            _put(c, self.date_line, 37)
            if c.resp_close or not c.keep_alive:
                c.resp_close = 1
                _put(c, b"connection: close\r\n", 19)
            elif c.http10:
                _put(c, b"connection: keep-alive\r\n", 24)
            _put(c, b"\r\n", 2)
            c.resp_hdr_done = 1
        if not c.is_head:
            if c.resp_chunked:
                if blen > 0:
                    _put_uint(c, <unsigned long long>blen, 16)
                    _put(c, b"\r\n", 2)
                    _put(c, b, blen)
                    _put(c, b"\r\n", 2)
                if not more_body:
                    _put(c, b"0\r\n\r\n", 5)
            elif blen > 0:
                _put(c, b, blen)
        if not more_body:
            c.resp_done = 1

    # -- the reactor --

    cdef int _check_signals(self) noexcept with gil:
        try:
            PyErr_CheckSignals()
            return 0
        except BaseException as exc:
            self.pending_exc = exc
            return -1

    cdef int _serve(self) noexcept nogil:
        cdef io_uring_cqe *cqe
        cdef int ret, op, fd, res
        cdef uint64_t ud
        cdef size_t off, total
        cdef conn_t *c
        cdef __kernel_timespec ts
        cdef sockaddr_in *a4
        cdef sockaddr_in6 *a6
        self._arm_accept()
        self._arm_wake()
        while self._stop == 0:
            if self.suspended > 0:
                self._arm_poll()
            if self.suspended > 0 and self.loop_timeout_ns >= 0:
                ts.tv_sec = self.loop_timeout_ns // 1000000000
                ts.tv_nsec = self.loop_timeout_ns % 1000000000
                ret = io_uring_submit_and_wait_timeout(&self.ring, &cqe, 1, &ts, NULL)
                if ret == -ETIME:
                    ret = 0
                    self.loop_wake = True
            else:
                ret = io_uring_submit_and_wait(&self.ring, 1)
            self.n_enters += 1
            if ret < 0:
                if ret == -EINTR:
                    if self._check_signals() < 0:
                        return 0
                    continue
                return ret
            while self._stop == 0:
                if io_uring_peek_cqe(&self.ring, &cqe) != 0 or cqe == NULL:
                    break
                ud = cqe.user_data
                res = cqe.res
                io_uring_cqe_seen(&self.ring, cqe)
                self.n_completions += 1
                op = <int>(ud >> 56)
                fd = <int>((ud >> 24) & <uint64_t>0xFFFFFFFF)
                if op == OP_RECV:
                    c = &self.conns[fd]
                    if res <= 0:
                        self._close(fd)
                        continue
                    if c.linger:
                        c.drained += <size_t>res
                        if c.drained > 4 * 1024 * 1024:
                            self._close(fd)
                        else:
                            self._arm_recv(fd)
                        continue
                    c.rlen += <size_t>res
                    self._on_request_data(fd, NULL)
                elif op == OP_SEND:
                    c = &self.conns[fd]
                    if res <= 0:
                        self._close(fd)
                        continue
                    off = c.send_off + <size_t>res
                    total = self.static_len if c.static_send else c.wlen
                    if off < total:
                        self._arm_send(fd, off)
                    else:
                        self._finish_request(fd)
                elif op == OP_ACCEPT:
                    if res >= 0:
                        if self._ensure(res) < 0:
                            close(res)
                        else:
                            c = &self.conns[res]
                            c.state = ST_READ
                            c.rlen = 0
                            c.hdr_parsed = 0
                            c.chunked = 0
                            c.peer_ok = 0
                            c.linger = 0
                            if self.acc_addr.ss_family == AF_INET:
                                a4 = <sockaddr_in*>&self.acc_addr
                                if inet_ntop(AF_INET, &a4.sin_addr, c.peer_host, 48) != NULL:
                                    c.peer_port = ntohs(a4.sin_port)
                                    c.peer_ok = 1
                            elif self.acc_addr.ss_family == AF_INET6:
                                a6 = <sockaddr_in6*>&self.acc_addr
                                if inet_ntop(AF_INET6, &a6.sin6_addr, c.peer_host, 48) != NULL:
                                    c.peer_port = ntohs(a6.sin6_port)
                                    c.peer_ok = 1
                            self._arm_recv(res)
                    self._arm_accept()
                elif op == OP_POLL:
                    self.poll_armed = False
                    self.loop_wake = True
                elif op == OP_WAKE:
                    self._stop = 1
                # OP_CONTINUE: nothing to do
            if self.npend or self.nready or self.loop_wake:
                self._flush_pending(NULL)
        return 0

    def serve(self, listen_sock, app):
        """Serve `app` (an ASGI 3 application) on `listen_sock` until stop()."""
        cdef int ret
        if self.serving:
            raise RuntimeError("already serving")
        self.app = app
        self.listen_fd = listen_sock.fileno()
        if self.server_addr is None:
            try:
                sa = listen_sock.getsockname()
                self.server_addr = (sa[0], sa[1])
            except OSError:
                self.server_addr = None
        if self.pend == NULL:
            self.pend = <int*>malloc(PEND_CAP * sizeof(int))
            if self.pend == NULL:
                raise MemoryError()
        self._loop = WorkerLoop()
        try:
            self.loop_fd = self._loop._selector.fileno()
        except Exception:
            self.loop_fd = -1
        self.serving = 1
        try:
            with nogil:
                ret = self._serve()
        finally:
            self.serving = 0
            try:
                self._loop.close()
            except Exception:
                pass
        if self.pending_exc is not None:
            exc, self.pending_exc = self.pending_exc, None
            raise exc
        if ret < 0:
            raise OSError(-ret, "io_uring wait failed")


cdef class _TaskDone:
    cdef Engine engine
    cdef int fd
    cdef unsigned gen
    cdef unsigned reqno

    def __cinit__(self, Engine engine, int fd, unsigned gen, unsigned reqno):
        self.engine = engine
        self.fd = fd
        self.gen = gen
        self.reqno = reqno

    def __call__(self, task):
        self.engine._task_done(self.fd, self.gen, self.reqno, task)
