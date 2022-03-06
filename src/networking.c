/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "atomicvar.h"
#include "cluster.h"
#include <sys/socket.h>
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>

static void setProtocolError(const char *errstr, client *c);

int postponeClientRead(client *c);

int ProcessingEventsWhileBlocked = 0; /* See processEventsWhileBlocked(). */

/* Return the size consumed from the allocator, for the specified SDS string,
 * including internal fragmentation. This function is used in order to compute
 * the client output buffer size. */
size_t sdsZmallocSize(sds s) {
    void *sh = sdsAllocPtr(s);
    return zmalloc_size(sh);
}

/* Return the amount of memory used by the sds string at object->ptr
 * for a string object. This includes internal fragmentation. */
size_t getStringObjectSdsUsedMemory(robj *o) {
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
    switch (o->encoding) {
        case OBJ_ENCODING_RAW:
            return sdsZmallocSize(o->ptr);
        case OBJ_ENCODING_EMBSTR:
            return zmalloc_size(o) - sizeof(robj);
        default:
            return 0; /* Just integer encoding for now. */
    }
}

/* Return the length of a string object.
 * This does NOT includes internal fragmentation or sds unused space. */
size_t getStringObjectLen(robj *o) {
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
    switch (o->encoding) {
        case OBJ_ENCODING_RAW:
            return sdslen(o->ptr);
        case OBJ_ENCODING_EMBSTR:
            return sdslen(o->ptr);
        default:
            return 0; /* Just integer encoding for now. */
    }
}

/* Client.reply list dup and free methods. */
void *dupClientReplyValue(void *o) {
    clientReplyBlock *old = o;
    clientReplyBlock *buf = zmalloc(sizeof(clientReplyBlock) + old->size);
    memcpy(buf, o, sizeof(clientReplyBlock) + old->size);
    return buf;
}

void freeClientReplyValue(void *o) {
    zfree(o);
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a, b);
}

/* This function links the client to the global linked list of clients.
 * unlinkClient() does the opposite, among other things. */
void linkClient(client *c) {
    listAddNodeTail(server.clients, c);
    /* Note that we remember the linked list node where the client is stored,
     * this way removing the client in unlinkClient() will not require
     * a linear scan, but just a constant time operation. */
    c->client_list_node = listLast(server.clients);
    uint64_t id = htonu64(c->id);
    raxInsert(server.clients_index, (unsigned char *) &id, sizeof(id), c, NULL);
}

/* Initialize client authentication state.
 */
static void clientSetDefaultAuth(client *c) {
    /* If the default user does not require authentication, the user is
     * directly authenticated. */
    c->user = DefaultUser;
    c->authenticated = (c->user->flags & USER_FLAG_NOPASS) &&
                       !(c->user->flags & USER_FLAG_DISABLED);
}

/*
 * 创建一个新的客户端
 */
client *createClient(connection *conn) {
    // 分配空间
    client *c = zmalloc(sizeof(client));

    /* passing NULL as conn it is possible to create a non connected client.
     * This is useful since all the commands needs to be executed
     * in the context of a client. When commands are executed in other
     * contexts (for instance a Lua script) we need a non connected client. */
    if (conn) {
        // 非阻塞
        connNonBlock(conn);
        // 禁用 Nagle 算法
        connEnableTcpNoDelay(conn);
        // 设置 keep alive
        if (server.tcpkeepalive)
            connKeepAlive(conn, server.tcpkeepalive);

        // 为 conn 的文件描述符注册读事件
        // 为 conn 连接的文件描述符设置读事件处理器 readQueryFromClient，
        connSetReadHandler(conn, readQueryFromClient);

        connSetPrivateData(conn, c);
    }

    /*-------------- 初始化各个属性  ---------------*/

    // 默认数据库
    selectDb(c, 0);
    uint64_t client_id;
    atomicGetIncr(server.next_client_id, client_id, 1);
    c->id = client_id;
    c->resp = 2;
    c->conn = conn;

    // 名称
    c->name = NULL;

    // 回复缓冲区的偏移量
    c->bufpos = 0;
    c->qb_pos = 0;
    // 查询缓冲区
    c->querybuf = sdsempty();
    c->pending_querybuf = sdsempty();
    // 查询缓冲区峰值
    c->querybuf_peak = 0;
    // 命令请求的类型
    c->reqtype = 0;
    // 命令参数数量
    c->argc = 0;
    // 命令参数
    c->argv = NULL;
    c->argv_len_sum = 0;
    c->original_argc = 0;
    c->original_argv = NULL;
    // 当前执行的命令和最近一次执行的命令
    c->cmd = c->lastcmd = NULL;
    // 查询缓冲区中未读入的命令内容数量
    c->multibulklen = 0;
    // 读入的参数的长度
    c->bulklen = -1;
    // 已发送字节数
    c->sentlen = 0;
    // 状态 FLAG
    c->flags = 0;
    // 创建时间和最后一次交互时间
    c->ctime = c->lastinteraction = server.unixtime;
    clientSetDefaultAuth(c);
    // 复制状态
    c->replstate = REPL_STATE_NONE;
    c->repl_put_online_on_ack = 0;
    // 复制偏移量
    c->reploff = 0;
    c->read_reploff = 0;
    // 通过 ACK 命令接收到的偏移量
    c->repl_ack_off = 0;
    // 通过 AKC 命令接收到偏移量的时间
    c->repl_ack_time = 0;
    c->repl_last_partial_write = 0;
    // 客户端为从服务器时使用，记录了从服务器所使用的端口号
    c->slave_listening_port = 0;
    c->slave_addr = NULL;
    c->slave_capa = SLAVE_CAPA_NONE;
    // 回复链表
    c->reply = listCreate();
    // 回复链表的字节量
    c->reply_bytes = 0;
    // 回复缓冲区大小达到软限制的时间
    c->obuf_soft_limit_reached_time = 0;
    // 回复链表的释放和复制函数
    listSetFreeMethod(c->reply, freeClientReplyValue);
    listSetDupMethod(c->reply, dupClientReplyValue);
    // 阻塞类型
    c->btype = BLOCKED_NONE;
    // 阻塞超时
    c->bpop.timeout = 0;
    // 造成客户端阻塞的列表键
    c->bpop.keys = dictCreate(&objectKeyHeapPointerValueDictType, NULL);
    // 在解除阻塞时将元素推入到 target 指定的键中
    // BRPOPLPUSH 命令时使用
    c->bpop.target = NULL;
    c->bpop.xread_group = NULL;
    c->bpop.xread_consumer = NULL;
    c->bpop.xread_group_noack = 0;
    c->bpop.numreplicas = 0;
    c->bpop.reploffset = 0;
    c->woff = 0;

    // 进行事务时监视的键
    c->watched_keys = listCreate();

    // 订阅的频道和模式
    c->pubsub_channels = dictCreate(&objectKeyPointerValueDictType, NULL);
    c->pubsub_patterns = listCreate();
    c->peerid = NULL;
    c->sockname = NULL;
    c->client_list_node = NULL;
    c->paused_list_node = NULL;
    c->client_tracking_redirection = 0;
    c->client_tracking_prefixes = NULL;
    c->client_cron_last_memory_usage = 0;
    c->client_cron_last_memory_type = CLIENT_TYPE_NORMAL;
    c->auth_callback = NULL;
    c->auth_callback_privdata = NULL;
    c->auth_module = NULL;
    listSetFreeMethod(c->pubsub_patterns, decrRefCountVoid);
    listSetMatchMethod(c->pubsub_patterns, listMatchObjects);

    // 如果不是伪客户端，那么添加到服务器的客户端链表中
    if (conn) linkClient(c);

    // 初始化客户端的事务状态
    initClientMultiState(c);

    // 返回客户端
    return c;
}

/* This function puts the client in the queue of clients that should write
 * their output buffers to the socket. Note that it does not *yet* install
 * the write handler, to start clients are put in a queue of clients that need
 * to write, so we try to do that before returning in the event loop (see the
 * handleClientsWithPendingWrites() function).
 *
 * 将客户端 c 放入应该将其输出缓冲区写入套接字的 客户端队列中。即 将客户端 C 加入到客户端等待写链表中。
 *
 * If we fail and there is more data to write, compared to what the socket
 * buffers can hold, then we'll really install the handler. */
// 该函数用来判断能否推迟客户端写操作，判断条件如下：
// 1 客户端没有设置过 CLIENT_PENDING_WRITE 标识，即没有被推迟过执行写操作。
// 2 客户端所在实例没有进行主从复制，或者客户端所在实例是主从复制中的从节点，但全量复制的 RDB 文件已经传输完成，客户端可以接收请求
// 推迟客户端写操作要上述两个条件同时满足
void clientInstallWriteHandler(client *c) {
    /* Schedule the client to write the output buffers to the socket only
     * if not already done and, for slaves, if the slave can actually receive
     * writes at this stage. */
    if (!(c->flags & CLIENT_PENDING_WRITE) &&
        (c->replstate == REPL_STATE_NONE ||
         (c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack))) {
        /* Here instead of installing the write handler, we just flag the
         * client and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */

        // 把客户端标识设置为 CLIENT_PENDING_WRITE（待写回），表示推迟该客户端的写操作
        c->flags |= CLIENT_PENDING_WRITE;

        // 将客户端 C 加入到 server 全局等待写队列中
        listAddNodeHead(server.clients_pending_write, c);
    }
}

/* This function is called every time we are going to transmit new data
 * to the client. The behavior is the following:
 *
 * 该函数在每次向客户端发送数据时都会被调用，函数行为如下：
 *
 * If the client should receive new data (normal clients will) the function
 * returns C_OK, and make sure to install the write handler in our event
 * loop so that when the socket is writable new data gets written.
 *
 * If the client should not receive new data, because it is a fake client
 * (used to load AOF in memory), a master or because the setup of the write
 * handler failed, the function returns C_ERR.
 *
 * The function may return C_OK without actually installing the write
 * event handler in the following cases:
 *
 * 在以下情况下，函数可能返回C_OK而不实际安装write事件处理程序:
 *
 * 1) The event handler should already be installed since the output buffer
 *    already contains something.
 *    事件处理程序应该已经安装，因为输出缓冲区已经包含了一些内容。
 *
 * 2) The client is a slave but not yet online, so we want to just accumulate
 *    writes in the buffer but not actually sending them yet.
 *    客户端是slave，但还没有上线，所以我们只是想在缓冲区中积累写操作，但还没有真正发送它们。
 *
 * Typically gets called every time a reply is built, before adding more
 * data to the clients output buffers. If the function returns C_ERR no
 * data should be appended to the output buffers. */
// 该函数会根据客户端设置的标识进行一系列判断。
// 其中，该函数会调用 clientHasPendingReplies 函数，判断当前客户端是否还有留存在输出缓冲区中的数据等待写回。
int prepareClientToWrite(client *c) {
    /* If it's the Lua client we always return ok without installing any
     * handler since there is no socket at all. */
    if (c->flags & (CLIENT_LUA | CLIENT_MODULE)) return C_OK;

    /* If CLIENT_CLOSE_ASAP flag is set, we need not write anything. */
    if (c->flags & CLIENT_CLOSE_ASAP) return C_ERR;

    /* CLIENT REPLY OFF / SKIP handling: don't send replies. */
    if (c->flags & (CLIENT_REPLY_OFF | CLIENT_REPLY_SKIP)) return C_ERR;

    /* Masters don't receive replies, unless CLIENT_MASTER_FORCE_REPLY flag
     * is set. */
    if ((c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_MASTER_FORCE_REPLY))
        return C_ERR;

    if (!c->conn) return C_ERR; /* Fake client for AOF loading. */

    /* Schedule the client to write the output buffers to the socket, unless
     * it should already be setup to do so (it has already pending data).
     *
     * If CLIENT_PENDING_READ is set, we're in an IO thread and should
     * not install a write handler. Instead, it will be done by
     * handleClientsWithPendingReadsUsingThreads() upon return.
     */
    // 如果当前客户端没有待写回数据，调用 clientInstallWriteHandler 函数再进一步判断能否推迟该客户端写操作。
    if (!clientHasPendingReplies(c) && !(c->flags & CLIENT_PENDING_READ))
        clientInstallWriteHandler(c);

    /* Authorize the caller to queue in the output buffer of this client. */
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

/* Attempts to add the reply to the static buffer in the client struct.
 * Returns C_ERR if the buffer is full, or the reply list is not empty,
 * in which case the reply must be added to the reply list.
 *
 *  尝试将回复添加到输出缓冲区
 */
int _addReplyToBuffer(client *c, const char *s, size_t len) {
    size_t available = sizeof(c->buf) - c->bufpos;

    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return C_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (listLength(c->reply) > 0) return C_ERR;

    /* Check that the buffer has enough space available for this string. */
    if (len > available) return C_ERR;

    memcpy(c->buf + c->bufpos, s, len);
    c->bufpos += len;
    return C_OK;
}

/* Adds the reply to the reply linked list.
 * Note: some edits to this function need to be relayed to AddReplyFromClient. */
void _addReplyProtoToList(client *c, const char *s, size_t len) {
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    listNode *ln = listLast(c->reply);
    clientReplyBlock *tail = ln ? listNodeValue(ln) : NULL;

    /* Note that 'tail' may be NULL even if we have a tail node, because when
     * addReplyDeferredLen() is used, it sets a dummy node to NULL just
     * fo fill it later, when the size of the bulk length is set. */

    /* Append to tail string when possible. */
    if (tail) {
        /* Copy the part we can fit into the tail, and leave the rest for a
         * new node */
        size_t avail = tail->size - tail->used;
        size_t copy = avail >= len ? len : avail;
        memcpy(tail->buf + tail->used, s, copy);
        tail->used += copy;
        s += copy;
        len -= copy;
    }
    if (len) {
        /* Create a new node, make sure it is allocated to at
         * least PROTO_REPLY_CHUNK_BYTES */
        size_t size = len < PROTO_REPLY_CHUNK_BYTES ? PROTO_REPLY_CHUNK_BYTES : len;
        tail = zmalloc(size + sizeof(clientReplyBlock));
        /* take over the allocation's internal fragmentation */
        tail->size = zmalloc_usable_size(tail) - sizeof(clientReplyBlock);
        tail->used = len;
        memcpy(tail->buf, s, len);
        listAddNodeTail(c->reply, tail);
        c->reply_bytes += tail->size;

        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

/* Add the object 'obj' string representation to the client output buffer.
 *
 * 将对象 obj 字符串形式添加到客户端输出缓冲区
 */
// Redis 在执行了客户端命令，要给客户端返回结果时，会调用 addReply 函数将待返回结果写入客户端输出缓冲区。
void addReply(client *c, robj *obj) {

    // 调用 prepareClientToWrite 判断是否推迟执行客户端写操作，即将待写回客户端加入到全局变量 server 的 clients_pending_write 列表中
    // 后续等 IO 线程执行写入缓冲区（也可能分配到主线程执行）
    if (prepareClientToWrite(c) != C_OK) return;

    // 如果不需要推迟执行，那么就由主线程处理，继续往下执行

    if (sdsEncodedObject(obj)) {
        // 将要返回的结果添加到客户端的输出缓冲区中
        if (_addReplyToBuffer(c, obj->ptr, sdslen(obj->ptr)) != C_OK)
            _addReplyProtoToList(c, obj->ptr, sdslen(obj->ptr));

    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* For integer encoded strings we just convert it into a string
         * using our optimized function, and attach the resulting string
         * to the output buffer. */
        char buf[32];
        size_t len = ll2string(buf, sizeof(buf), (long) obj->ptr);

        // 将要返回的结果添加到客户端的输出缓冲区中
        if (_addReplyToBuffer(c, buf, len) != C_OK)
            _addReplyProtoToList(c, buf, len);

    } else {
        serverPanic("Wrong obj->encoding in addReply()");
    }
}

/* Add the SDS 's' string to the client output buffer, as a side effect
 * the SDS string is freed. */
void addReplySds(client *c, sds s) {
    // 判断当前客户端是否还有留存在输出缓冲区中的数据等待写回。
    if (prepareClientToWrite(c) != C_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }

    // 将回复添加到 c 的输出缓冲区
    if (_addReplyToBuffer(c, s, sdslen(s)) != C_OK)
        _addReplyProtoToList(c, s, sdslen(s));

    sdsfree(s);
}

/* This low level function just adds whatever protocol you send it to the
 * client buffer, trying the static buffer initially, and using the string
 * of objects if not possible.
 *
 * It is efficient because does not create an SDS object nor an Redis object
 * if not needed. The object will only be created by calling
 * _addReplyProtoToList() if we fail to extend the existing tail object
 * in the list of objects. */
void addReplyProto(client *c, const char *s, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;

    // 尝试将回复添加到输出缓冲区
    if (_addReplyToBuffer(c, s, len) != C_OK)
        _addReplyProtoToList(c, s, len);
}

/* Low level function called by the addReplyError...() functions.
 * It emits the protocol for a Redis error, in the form:
 *
 * -ERRORCODE Error Message<CR><LF>
 *
 * If the error code is already passed in the string 's', the error
 * code provided is used, otherwise the string "-ERR " for the generic
 * error code is automatically added.
 * Note that 's' must NOT end with \r\n. */
void addReplyErrorLength(client *c, const char *s, size_t len) {
    /* If the string already starts with "-..." then the error code
     * is provided by the caller. Otherwise we use "-ERR". */
    if (!len || s[0] != '-') addReplyProto(c, "-ERR ", 5);
    addReplyProto(c, s, len);
    addReplyProto(c, "\r\n", 2);
}

/* Do some actions after an error reply was sent (Log if needed, updates stats, etc.) */
void afterErrorReply(client *c, const char *s, size_t len) {
    /* Increment the global error counter */
    server.stat_total_error_replies++;
    /* Increment the error stats
     * If the string already starts with "-..." then the error prefix
     * is provided by the caller ( we limit the search to 32 chars). Otherwise we use "-ERR". */
    if (s[0] != '-') {
        incrementErrorCount("ERR", 3);
    } else {
        char *spaceloc = memchr(s, ' ', len < 32 ? len : 32);
        if (spaceloc) {
            const size_t errEndPos = (size_t) (spaceloc - s);
            incrementErrorCount(s + 1, errEndPos - 1);
        } else {
            /* Fallback to ERR if we can't retrieve the error prefix */
            incrementErrorCount("ERR", 3);
        }
    }

    /* Sometimes it could be normal that a slave replies to a master with
     * an error and this function gets called. Actually the error will never
     * be sent because addReply*() against master clients has no effect...
     * A notable example is:
     *
     *    EVAL 'redis.call("incr",KEYS[1]); redis.call("nonexisting")' 1 x
     *
     * Where the master must propagate the first change even if the second
     * will produce an error. However it is useful to log such events since
     * they are rare and may hint at errors in a script or a bug in Redis. */
    int ctype = getClientType(c);
    if (ctype == CLIENT_TYPE_MASTER || ctype == CLIENT_TYPE_SLAVE || c->id == CLIENT_ID_AOF) {
        char *to, *from;

        if (c->id == CLIENT_ID_AOF) {
            to = "AOF-loading-client";
            from = "server";
        } else if (ctype == CLIENT_TYPE_MASTER) {
            to = "master";
            from = "replica";
        } else {
            to = "replica";
            from = "master";
        }

        if (len > 4096) len = 4096;
        char *cmdname = c->lastcmd ? c->lastcmd->name : "<unknown>";
        serverLog(LL_WARNING, "== CRITICAL == This %s is sending an error "
                              "to its %s: '%.*s' after processing the command "
                              "'%s'", from, to, (int) len, s, cmdname);
        if (ctype == CLIENT_TYPE_MASTER && server.repl_backlog &&
            server.repl_backlog_histlen > 0) {
            showLatestBacklog();
        }
        server.stat_unexpected_error_replies++;
    }
}

/* The 'err' object is expected to start with -ERRORCODE and end with \r\n.
 * Unlike addReplyErrorSds and others alike which rely on addReplyErrorLength. */
void addReplyErrorObject(client *c, robj *err) {
    addReply(c, err);
    afterErrorReply(c, err->ptr, sdslen(err->ptr) - 2); /* Ignore trailing \r\n */
}

/* See addReplyErrorLength for expectations from the input string. */
void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c, err, strlen(err));
    afterErrorReply(c, err, strlen(err));
}

/* See addReplyErrorLength for expectations from the input string. */
/* As a side effect the SDS string is freed. */
void addReplyErrorSds(client *c, sds err) {
    addReplyErrorLength(c, err, sdslen(err));
    afterErrorReply(c, err, sdslen(err));
    sdsfree(err);
}

/* See addReplyErrorLength for expectations from the formatted string.
 * The formatted string is safe to contain \r and \n anywhere. */
void addReplyErrorFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    sds s = sdscatvprintf(sdsempty(), fmt, ap);
    va_end(ap);
    /* Trim any newlines at the end (ones will be added by addReplyErrorLength) */
    s = sdstrim(s, "\r\n");
    /* Make sure there are no newlines in the middle of the string, otherwise
     * invalid protocol is emitted. */
    s = sdsmapchars(s, "\r\n", "  ", 2);
    addReplyErrorLength(c, s, sdslen(s));
    afterErrorReply(c, s, sdslen(s));
    sdsfree(s);
}

void addReplyStatusLength(client *c, const char *s, size_t len) {
    addReplyProto(c, "+", 1);
    addReplyProto(c, s, len);
    addReplyProto(c, "\r\n", 2);
}

void addReplyStatus(client *c, const char *status) {
    addReplyStatusLength(c, status, strlen(status));
}

void addReplyStatusFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    sds s = sdscatvprintf(sdsempty(), fmt, ap);
    va_end(ap);
    addReplyStatusLength(c, s, sdslen(s));
    sdsfree(s);
}

/* Sometimes we are forced to create a new reply node, and we can't append to
 * the previous one, when that happens, we wanna try to trim the unused space
 * at the end of the last reply node which we won't use anymore. */
void trimReplyUnusedTailSpace(client *c) {
    listNode *ln = listLast(c->reply);
    clientReplyBlock *tail = ln ? listNodeValue(ln) : NULL;

    /* Note that 'tail' may be NULL even if we have a tail node, because when
     * addReplyDeferredLen() is used */
    if (!tail) return;

    /* We only try to trim the space is relatively high (more than a 1/4 of the
     * allocation), otherwise there's a high chance realloc will NOP.
     * Also, to avoid large memmove which happens as part of realloc, we only do
     * that if the used part is small.  */
    if (tail->size - tail->used > tail->size / 4 &&
        tail->used < PROTO_REPLY_CHUNK_BYTES) {
        size_t old_size = tail->size;
        tail = zrealloc(tail, tail->used + sizeof(clientReplyBlock));
        /* take over the allocation's internal fragmentation (at least for
         * memory usage tracking) */
        tail->size = zmalloc_usable_size(tail) - sizeof(clientReplyBlock);
        c->reply_bytes = c->reply_bytes + tail->size - old_size;
        listNodeValue(ln) = tail;
    }
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *addReplyDeferredLen(client *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredAggregateLen() will be called. */
    if (prepareClientToWrite(c) != C_OK) return NULL;
    trimReplyUnusedTailSpace(c);
    listAddNodeTail(c->reply, NULL); /* NULL is our placeholder. */
    return listLast(c->reply);
}

void setDeferredReply(client *c, void *node, const char *s, size_t length) {
    listNode *ln = (listNode *) node;
    clientReplyBlock *next, *prev;

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addReplyDeferredLen() */
    if (node == NULL) return;
    serverAssert(!listNodeValue(ln));

    /* Normally we fill this dummy NULL node, added by addReplyDeferredLen(),
     * with a new buffer structure containing the protocol needed to specify
     * the length of the array following. However sometimes there might be room
     * in the previous/next node so we can instead remove this NULL node, and
     * suffix/prefix our data in the node immediately before/after it, in order
     * to save a write(2) syscall later. Conditions needed to do it:
     *
     * - The prev node is non-NULL and has space in it or
     * - The next node is non-NULL,
     * - It has enough room already allocated
     * - And not too large (avoid large memmove) */
    if (ln->prev != NULL && (prev = listNodeValue(ln->prev)) &&
        prev->size - prev->used > 0) {
        size_t len_to_copy = prev->size - prev->used;
        if (len_to_copy > length)
            len_to_copy = length;
        memcpy(prev->buf + prev->used, s, len_to_copy);
        prev->used += len_to_copy;
        length -= len_to_copy;
        if (length == 0) {
            listDelNode(c->reply, ln);
            return;
        }
        s += len_to_copy;
    }

    if (ln->next != NULL && (next = listNodeValue(ln->next)) &&
        next->size - next->used >= length &&
        next->used < PROTO_REPLY_CHUNK_BYTES * 4) {
        memmove(next->buf + length, next->buf, next->used);
        memcpy(next->buf, s, length);
        next->used += length;
        listDelNode(c->reply, ln);
    } else {
        /* Create a new node */
        clientReplyBlock *buf = zmalloc(length + sizeof(clientReplyBlock));
        /* Take over the allocation's internal fragmentation */
        buf->size = zmalloc_usable_size(buf) - sizeof(clientReplyBlock);
        buf->used = length;
        memcpy(buf->buf, s, length);
        listNodeValue(ln) = buf;
        c->reply_bytes += buf->size;

        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* Populate the length object and try gluing it to the next chunk. */
void setDeferredAggregateLen(client *c, void *node, long length, char prefix) {
    serverAssert(length >= 0);

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addReplyDeferredLen() */
    if (node == NULL) return;

    char lenstr[128];
    size_t lenstr_len = sprintf(lenstr, "%c%ld\r\n", prefix, length);
    setDeferredReply(c, node, lenstr, lenstr_len);
}

void setDeferredArrayLen(client *c, void *node, long length) {
    setDeferredAggregateLen(c, node, length, '*');
}

void setDeferredMapLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    setDeferredAggregateLen(c, node, length, prefix);
}

void setDeferredSetLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    setDeferredAggregateLen(c, node, length, prefix);
}

void setDeferredAttributeLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '|';
    if (c->resp == 2) length *= 2;
    setDeferredAggregateLen(c, node, length, prefix);
}

void setDeferredPushLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '>';
    setDeferredAggregateLen(c, node, length, prefix);
}

/* Add a double as a bulk reply */
void addReplyDouble(client *c, double d) {
    if (isinf(d)) {
        /* Libc in odd systems (Hi Solaris!) will format infinite in a
         * different way, so better to handle it in an explicit way. */
        if (c->resp == 2) {
            addReplyBulkCString(c, d > 0 ? "inf" : "-inf");
        } else {
            addReplyProto(c, d > 0 ? ",inf\r\n" : ",-inf\r\n",
                          d > 0 ? 6 : 7);
        }
    } else {
        char dbuf[MAX_LONG_DOUBLE_CHARS + 3],
                sbuf[MAX_LONG_DOUBLE_CHARS + 32];
        int dlen, slen;
        if (c->resp == 2) {
            dlen = snprintf(dbuf, sizeof(dbuf), "%.17g", d);
            slen = snprintf(sbuf, sizeof(sbuf), "$%d\r\n%s\r\n", dlen, dbuf);
            addReplyProto(c, sbuf, slen);
        } else {
            dlen = snprintf(dbuf, sizeof(dbuf), ",%.17g\r\n", d);
            addReplyProto(c, dbuf, dlen);
        }
    }
}

/* Add a long double as a bulk reply, but uses a human readable formatting
 * of the double instead of exposing the crude behavior of doubles to the
 * dear user. */
void addReplyHumanLongDouble(client *c, long double d) {
    if (c->resp == 2) {
        robj *o = createStringObjectFromLongDouble(d, 1);
        addReplyBulk(c, o);
        decrRefCount(o);
    } else {
        char buf[MAX_LONG_DOUBLE_CHARS];
        int len = ld2string(buf, sizeof(buf), d, LD_STR_HUMAN);
        addReplyProto(c, ",", 1);
        addReplyProto(c, buf, len);
        addReplyProto(c, "\r\n", 2);
    }
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    if (prefix == '*' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c, shared.mbulkhdr[ll]);
        return;
    } else if (prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c, shared.bulkhdr[ll]);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf + 1, sizeof(buf) - 1, ll);
    buf[len + 1] = '\r';
    buf[len + 2] = '\n';
    addReplyProto(c, buf, len + 3);
}

void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c, shared.czero);
    else if (ll == 1)
        addReply(c, shared.cone);
    else
        addReplyLongLongWithPrefix(c, ll, ':');
}

void addReplyAggregateLen(client *c, long length, int prefix) {
    serverAssert(length >= 0);
    addReplyLongLongWithPrefix(c, length, prefix);
}

void addReplyArrayLen(client *c, long length) {
    addReplyAggregateLen(c, length, '*');
}

void addReplyMapLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    addReplyAggregateLen(c, length, prefix);
}

void addReplySetLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    addReplyAggregateLen(c, length, prefix);
}

void addReplyAttributeLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '|';
    if (c->resp == 2) length *= 2;
    addReplyAggregateLen(c, length, prefix);
}

void addReplyPushLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '>';
    addReplyAggregateLen(c, length, prefix);
}

void addReplyNull(client *c) {
    if (c->resp == 2) {
        addReplyProto(c, "$-1\r\n", 5);
    } else {
        addReplyProto(c, "_\r\n", 3);
    }
}

void addReplyBool(client *c, int b) {
    if (c->resp == 2) {
        addReply(c, b ? shared.cone : shared.czero);
    } else {
        addReplyProto(c, b ? "#t\r\n" : "#f\r\n", 4);
    }
}

/* A null array is a concept that no longer exists in RESP3. However
 * RESP2 had it, so API-wise we have this call, that will emit the correct
 * RESP2 protocol, however for RESP3 the reply will always be just the
 * Null type "_\r\n". */
void addReplyNullArray(client *c) {
    if (c->resp == 2) {
        addReplyProto(c, "*-1\r\n", 5);
    } else {
        addReplyProto(c, "_\r\n", 3);
    }
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(client *c, robj *obj) {
    size_t len = stringObjectLen(obj);

    addReplyLongLongWithPrefix(c, len, '$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(client *c, robj *obj) {
    addReplyBulkLen(c, obj);
    addReply(c, obj);
    addReply(c, shared.crlf);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(client *c, const void *p, size_t len) {
    addReplyLongLongWithPrefix(c, len, '$');
    addReplyProto(c, p, len);
    addReply(c, shared.crlf);
}

/* Add sds to reply (takes ownership of sds and frees it) */
void addReplyBulkSds(client *c, sds s) {
    addReplyLongLongWithPrefix(c, sdslen(s), '$');
    addReplySds(c, s);
    addReply(c, shared.crlf);
}

/* Set sds to a deferred reply (for symmetry with addReplyBulkSds it also frees the sds) */
void setDeferredReplyBulkSds(client *c, void *node, sds s) {
    sds reply = sdscatprintf(sdsempty(), "$%d\r\n%s\r\n", (unsigned) sdslen(s), s);
    setDeferredReply(c, node, reply, sdslen(reply));
    sdsfree(reply);
    sdsfree(s);
}

/* Add a C null term string as bulk reply */
void addReplyBulkCString(client *c, const char *s) {
    if (s == NULL) {
        addReplyNull(c);
    } else {
        addReplyBulkCBuffer(c, s, strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(client *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf, 64, ll);
    addReplyBulkCBuffer(c, buf, len);
}

/* Reply with a verbatim type having the specified extension.
 *
 * The 'ext' is the "extension" of the file, actually just a three
 * character type that describes the format of the verbatim string.
 * For instance "txt" means it should be interpreted as a text only
 * file by the receiver, "md " as markdown, and so forth. Only the
 * three first characters of the extension are used, and if the
 * provided one is shorter than that, the remaining is filled with
 * spaces. */
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext) {
    if (c->resp == 2) {
        addReplyBulkCBuffer(c, s, len);
    } else {
        char buf[32];
        size_t preflen = snprintf(buf, sizeof(buf), "=%zu\r\nxxx:", len + 4);
        char *p = buf + preflen - 4;
        for (int i = 0; i < 3; i++) {
            if (*ext == '\0') {
                p[i] = ' ';
            } else {
                p[i] = *ext++;
            }
        }
        addReplyProto(c, buf, preflen);
        addReplyProto(c, s, len);
        addReplyProto(c, "\r\n", 2);
    }
}

/* Add an array of C strings as status replies with a heading.
 * This function is typically invoked by from commands that support
 * subcommands in response to the 'help' subcommand. The help array
 * is terminated by NULL sentinel. */
void addReplyHelp(client *c, const char **help) {
    sds cmd = sdsnew((char *) c->argv[0]->ptr);
    void *blenp = addReplyDeferredLen(c);
    int blen = 0;

    sdstoupper(cmd);
    addReplyStatusFormat(c,
                         "%s <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", cmd);
    sdsfree(cmd);

    while (help[blen]) addReplyStatus(c, help[blen++]);

    addReplyStatus(c, "HELP");
    addReplyStatus(c, "    Prints this help.");

    blen += 1;  /* Account for the header. */
    blen += 2;  /* Account for the footer. */
    setDeferredArrayLen(c, blenp, blen);
}

/* Add a suggestive error reply.
 * This function is typically invoked by from commands that support
 * subcommands in response to an unknown subcommand or argument error. */
void addReplySubcommandSyntaxError(client *c) {
    sds cmd = sdsnew((char *) c->argv[0]->ptr);
    sdstoupper(cmd);
    addReplyErrorFormat(c,
                        "Unknown subcommand or wrong number of arguments for '%s'. Try %s HELP.",
                        (char *) c->argv[1]->ptr, cmd);
    sdsfree(cmd);
}

/* Append 'src' client output buffers into 'dst' client output buffers.
 * This function clears the output buffers of 'src' */
void AddReplyFromClient(client *dst, client *src) {
    /* If the source client contains a partial response due to client output
     * buffer limits, propagate that to the dest rather than copy a partial
     * reply. We don't wanna run the risk of copying partial response in case
     * for some reason the output limits don't reach the same decision (maybe
     * they changed) */
    if (src->flags & CLIENT_CLOSE_ASAP) {
        sds client = catClientInfoString(sdsempty(), dst);
        freeClientAsync(dst);
        serverLog(LL_WARNING, "Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
        return;
    }

    /* First add the static buffer (either into the static buffer or reply list) */
    addReplyProto(dst, src->buf, src->bufpos);

    /* We need to check with prepareClientToWrite again (after addReplyProto)
     * since addReplyProto may have changed something (like CLIENT_CLOSE_ASAP) */
    if (prepareClientToWrite(dst) != C_OK)
        return;

    /* We're bypassing _addReplyProtoToList, so we need to add the pre/post
     * checks in it. */
    if (dst->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    /* Concatenate the reply list into the dest */
    if (listLength(src->reply))
        listJoin(dst->reply, src->reply);
    dst->reply_bytes += src->reply_bytes;
    src->reply_bytes = 0;
    src->bufpos = 0;

    /* Check output buffer limits */
    closeClientOnOutputBufferLimitReached(dst, 1);
}

/* Copy 'src' client output buffers into 'dst' client output buffers.
 * The function takes care of freeing the old output buffers of the
 * destination client. */
void copyClientOutputBuffer(client *dst, client *src) {
    listRelease(dst->reply);
    dst->sentlen = 0;
    dst->reply = listDup(src->reply);
    memcpy(dst->buf, src->buf, src->bufpos);
    dst->bufpos = src->bufpos;
    dst->reply_bytes = src->reply_bytes;
}

/* 如果指定的客户端有等待写入套接字的应答缓冲区，则返回true。*/
int clientHasPendingReplies(client *c) {
    return c->bufpos || listLength(c->reply);
}

void clientAcceptHandler(connection *conn) {
    client *c = connGetPrivateData(conn);

    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,
                  "Error accepting a client connection: %s",
                  connGetLastError(conn));
        freeClientAsync(c);
        return;
    }

    /* If the server is running in protected mode (the default) and there
     * is no password set, nor a specific interface is bound, we don't accept
     * requests from non loopback interfaces. Instead we try to explain the
     * user what to do to fix it if needed. */
    if (server.protected_mode &&
        server.bindaddr_count == 0 &&
        DefaultUser->flags & USER_FLAG_NOPASS &&
        !(c->flags & CLIENT_UNIX_SOCKET)) {
        char cip[NET_IP_STR_LEN + 1] = {0};
        connPeerToString(conn, cip, sizeof(cip) - 1, NULL);

        if (strcmp(cip, "127.0.0.1") && strcmp(cip, "::1")) {
            char *err =
                    "-DENIED Redis is running in protected mode because protected "
                    "mode is enabled, no bind address was specified, no "
                    "authentication password is requested to clients. In this mode "
                    "connections are only accepted from the loopback interface. "
                    "If you want to connect from external computers to Redis you "
                    "may adopt one of the following solutions: "
                    "1) Just disable protected mode sending the command "
                    "'CONFIG SET protected-mode no' from the loopback interface "
                    "by connecting to Redis from the same host the server is "
                    "running, however MAKE SURE Redis is not publicly accessible "
                    "from internet if you do so. Use CONFIG REWRITE to make this "
                    "change permanent. "
                    "2) Alternatively you can just disable the protected mode by "
                    "editing the Redis configuration file, and setting the protected "
                    "mode option to 'no', and then restarting the server. "
                    "3) If you started the server manually just for testing, restart "
                    "it with the '--protected-mode no' option. "
                    "4) Setup a bind address or an authentication password. "
                    "NOTE: You only need to do one of the above things in order for "
                    "the server to start accepting connections from the outside.\r\n";
            if (connWrite(c->conn, err, strlen(err)) == -1) {
                /* Nothing to do, Just to avoid the warning... */
            }
            server.stat_rejected_conn++;
            freeClientAsync(c);
            return;
        }
    }

    server.stat_numconnections++;
    moduleFireServerEvent(REDISMODULE_EVENT_CLIENT_CHANGE,
                          REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED,
                          c);
}

#define MAX_ACCEPTS_PER_CALL 1000

/**
 *  TCP 连接 accept 处理器，通过调用 createClient 函数创建客户端。
 *  特殊说明：
 *  1 该函数处理 conn 的 文件描述符的事件注册，该过程在调用 createClient 函数时触发，逻辑封装在了 ConnectionType 封装的函数中
 *  2 createClient 接着会初始化 Client 对象
 * @param conn
 * @param flags
 * @param ip
 */
static void acceptCommonHandler(connection *conn, int flags, char *ip) {
    client *c;
    char conninfo[100];
    UNUSED(ip);

    if (connGetState(conn) != CONN_STATE_ACCEPTING) {
        serverLog(LL_VERBOSE,
                  "Accepted client connection in error state: %s (conn: %s)",
                  connGetLastError(conn),
                  connGetInfo(conn, conninfo, sizeof(conninfo)));
        connClose(conn);
        return;
    }

    /* Limit the number of connections we take at the same time.
     *
     * Admission control will happen before a client is created and connAccept()
     * called, because we don't want to even start transport-level negotiation
     * if rejected. */
    // 如果新添加的客户端令服务器的最大客户端数量达到了，那么拒绝创建客户端
    if (listLength(server.clients) + getClusterConnectionsCount()
        >= server.maxclients) {
        char *err;
        if (server.cluster_enabled)
            err = "-ERR max number of clients + cluster "
                  "connections reached\r\n";
        else
            err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors.
         * Note that for TLS connections, no handshake was done yet so nothing
         * is written and the connection will just drop. */
        if (connWrite(conn, err, strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        server.stat_rejected_conn++;
        connClose(conn);
        return;
    }

    /* Create connection and client */
    // 注册 conn.cfd 的监听事件然创建客户端
    if ((c = createClient(conn)) == NULL) {
        serverLog(LL_WARNING,
                  "Error registering fd event for the new client: %s (conn: %s)",
                  connGetLastError(conn),
                  connGetInfo(conn, conninfo, sizeof(conninfo)));
        connClose(conn); /* May be already closed, just ignore errors */
        return;
    }

    /* Last chance to keep flags */
    // 设置客户端状态
    c->flags |= flags;

    /* Initiate accept.
     * 开始接受
     *
     * Note that connAccept() is free to do two things here:
     * connAccept 在这里可以做两件事
     * 1. Call clientAcceptHandler() immediately;
     * 立即调用 clientAcceptHandler 函数
     * 2. Schedule a future call to clientAcceptHandler().
     * 计划将来对 clientAcceptHandler 的调用
     *
     * Because of that, we must do nothing else afterwards.
     */
    if (connAccept(conn, clientAcceptHandler) == C_ERR) {
        char conninfo[100];
        if (connGetState(conn) == CONN_STATE_ERROR)
            serverLog(LL_WARNING,
                      "Error accepting a client connection: %s (conn: %s)",
                      connGetLastError(conn), connGetInfo(conn, conninfo, sizeof(conninfo)));
        freeClient(connGetPrivateData(conn));
        return;
    }
}

/*
 * 连接应答处理器
 *
 * 用于对连接服务器监听套接字的客户端进行应答，即 Redis server 接收到客户端的连接请求就会使用 acceptTcpHandler 函数进行处理
 *
 * 当 Redis 服务器初始化时，程序会将这个连接应答处理器和服务器监听套接字的 AE_READABLE 事件关联起来，
 * 当有客户端向服务器发起连接，那么服务器监听套接字就会产生 AE_READABLE 事件，触发连接应答处理器执行。
 * 处理器会对客户端的连接请求进行应答，然后创建客户端套接字，以及客户端状态，并将客户端套接字的 AE_READABLE 事件
 * 与命令请求处理器进行关联，使得客户端可以向主服务器发送命令请求
 *
 */
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while (max--) {
        // accept 客户端连接，并创建已连接套接字 cfd
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                          "Accepting client connection: %s", server.neterr);
            return;
        }

        anetCloexec(cfd);
        serverLog(LL_VERBOSE, "Accepted %s:%d", cip, cport);

        // connCreateAcceptedSocket：根据得到 cfd 创建一个 connection ，注意创建 connection 会初始化一个 ConnectionType 类型的属性 type，里面封装了和连接 connection 相关各种处理函数，详情见：ConnectionType CT_Socket
        // acceptCommonHandler：为客户端创建客户端状态（redisClient）
        acceptCommonHandler(connCreateAcceptedSocket(cfd), 0, cip);
    }
}

void acceptTLSHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while (max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                          "Accepting client connection: %s", server.neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE, "Accepted %s:%d", cip, cport);
        acceptCommonHandler(connCreateAcceptedTLS(cfd, server.tls_auth_clients), 0, cip);
    }
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd, max = MAX_ACCEPTS_PER_CALL;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while (max--) {
        cfd = anetUnixAccept(server.neterr, fd);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                          "Accepting client connection: %s", server.neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE, "Accepted connection to %s", server.unixsocket);
        acceptCommonHandler(connCreateAcceptedSocket(cfd), CLIENT_UNIX_SOCKET, NULL);
    }
}

void freeClientOriginalArgv(client *c) {
    /* We didn't rewrite this client */
    if (!c->original_argv) return;

    for (int j = 0; j < c->original_argc; j++)
        decrRefCount(c->original_argv[j]);
    zfree(c->original_argv);
    c->original_argv = NULL;
    c->original_argc = 0;
}

static void freeClientArgv(client *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    c->argc = 0;
    c->cmd = NULL;
    c->argv_len_sum = 0;
}

/* Close all the slaves connections. This is useful in chained replication
 * when we resync with our own master and want to force all our slaves to
 * resync with us as well. */
void disconnectSlaves(void) {
    listIter li;
    listNode *ln;
    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        freeClient((client *) ln->value);
    }
}

/* Check if there is any other slave waiting dumping RDB finished expect me.
 * This function is useful to judge current dumping RDB can be used for full
 * synchronization or not. */
int anyOtherSlaveWaitRdb(client *except_me) {
    listIter li;
    listNode *ln;

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;
        if (slave != except_me &&
            slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            return 1;
        }
    }
    return 0;
}

/* Remove the specified client from global lists where the client could
 * be referenced, not including the Pub/Sub channels.
 * This is used by freeClient() and replicationCacheMaster(). */
void unlinkClient(client *c) {
    listNode *ln;

    /* If this is marked as current client unset it. */
    if (server.current_client == c) server.current_client = NULL;

    /* Certain operations must be done only if the client has an active connection.
     * If the client was already unlinked or if it's a "fake client" the
     * conn is already set to NULL. */
    if (c->conn) {
        /* Remove from the list of active clients. */
        if (c->client_list_node) {
            uint64_t id = htonu64(c->id);
            raxRemove(server.clients_index, (unsigned char *) &id, sizeof(id), NULL);
            listDelNode(server.clients, c->client_list_node);
            c->client_list_node = NULL;
        }

        /* Check if this is a replica waiting for diskless replication (rdb pipe),
         * in which case it needs to be cleaned from that list */
        if (c->flags & CLIENT_SLAVE &&
            c->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            server.rdb_pipe_conns) {
            int i;
            for (i = 0; i < server.rdb_pipe_numconns; i++) {
                if (server.rdb_pipe_conns[i] == c->conn) {
                    rdbPipeWriteHandlerConnRemoved(c->conn);
                    server.rdb_pipe_conns[i] = NULL;
                    break;
                }
            }
        }
        connClose(c->conn);
        c->conn = NULL;
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flags & CLIENT_PENDING_WRITE) {
        ln = listSearchKey(server.clients_pending_write, c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_pending_write, ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* Remove from the list of pending reads if needed. */
    if (c->flags & CLIENT_PENDING_READ) {
        ln = listSearchKey(server.clients_pending_read, c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_pending_read, ln);
        c->flags &= ~CLIENT_PENDING_READ;
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    if (c->flags & CLIENT_UNBLOCKED) {
        ln = listSearchKey(server.unblocked_clients, c);
        serverAssert(ln != NULL);
        listDelNode(server.unblocked_clients, ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    }

    /* Clear the tracking status. */
    if (c->flags & CLIENT_TRACKING) disableTracking(c);
}

void freeClient(client *c) {
    listNode *ln;

    /* If a client is protected, yet we need to free it right now, make sure
     * to at least use asynchronous freeing. */
    if (c->flags & CLIENT_PROTECTED) {
        freeClientAsync(c);
        return;
    }

    /* For connected clients, call the disconnection event of modules hooks. */
    if (c->conn) {
        moduleFireServerEvent(REDISMODULE_EVENT_CLIENT_CHANGE,
                              REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED,
                              c);
    }

    /* Notify module system that this client auth status changed. */
    moduleNotifyUserChanged(c);

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. Note that we need to do this here, because later
     * we may call replicationCacheMaster() and the client should already
     * be removed from the list of clients to free. */
    if (c->flags & CLIENT_CLOSE_ASAP) {
        ln = listSearchKey(server.clients_to_close, c);
        serverAssert(ln != NULL);
        listDelNode(server.clients_to_close, ln);
    }

    /* If it is our master that's being disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    if (server.master && c->flags & CLIENT_MASTER) {
        serverLog(LL_WARNING, "Connection with master lost.");
        if (!(c->flags & (CLIENT_PROTOCOL_ERROR | CLIENT_BLOCKED))) {
            c->flags &= ~(CLIENT_CLOSE_ASAP | CLIENT_CLOSE_AFTER_REPLY);
            replicationCacheMaster(c);
            return;
        }
    }

    /* Log link disconnection with slave */
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        serverLog(LL_WARNING, "Connection with replica %s lost.",
                  replicationGetSlaveName(c));
    }

    /* Free the query buffer */
    sdsfree(c->querybuf);
    sdsfree(c->pending_querybuf);
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    if (c->flags & CLIENT_BLOCKED) unblockClient(c);
    dictRelease(c->bpop.keys);

    /* UNWATCH all the keys */
    unwatchAllKeys(c);
    listRelease(c->watched_keys);

    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c, 0);
    pubsubUnsubscribeAllPatterns(c, 0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);

    /* Free data structures. */
    listRelease(c->reply);
    freeClientArgv(c);
    freeClientOriginalArgv(c);

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    unlinkClient(c);

    /* Master/slave cleanup Case 1:
     * we lost the connection with a slave. */
    if (c->flags & CLIENT_SLAVE) {
        /* If there is no any other slave waiting dumping RDB finished, the
         * current child process need not continue to dump RDB, then we kill it.
         * So child process won't use more memory, and we also can fork a new
         * child process asap to dump rdb for next full synchronization or bgsave.
         * But we also need to check if users enable 'save' RDB, if enable, we
         * should not remove directly since that means RDB is important for users
         * to keep data safe and we may delay configured 'save' for full sync. */
        if (server.saveparamslen == 0 &&
            c->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            server.child_type == CHILD_TYPE_RDB &&
            server.rdb_child_type == RDB_CHILD_TYPE_DISK &&
            anyOtherSlaveWaitRdb(c) == 0) {
            killRDBChild();
        }
        if (c->replstate == SLAVE_STATE_SEND_BULK) {
            if (c->repldbfd != -1) close(c->repldbfd);
            if (c->replpreamble) sdsfree(c->replpreamble);
        }
        list *l = (c->flags & CLIENT_MONITOR) ? server.monitors : server.slaves;
        ln = listSearchKey(l, c);
        serverAssert(ln != NULL);
        listDelNode(l, ln);
        /* We need to remember the time when we started to have zero
         * attached slaves, as after some time we'll free the replication
         * backlog. */
        if (getClientType(c) == CLIENT_TYPE_SLAVE && listLength(server.slaves) == 0)
            server.repl_no_slaves_since = server.unixtime;
        refreshGoodSlavesCount();
        /* Fire the replica change modules event. */
        if (c->replstate == SLAVE_STATE_ONLINE)
            moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                                  REDISMODULE_SUBEVENT_REPLICA_CHANGE_OFFLINE,
                                  NULL);
    }

    /* Master/slave cleanup Case 2:
     * we lost the connection with the master. */
    if (c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection();

    /* Remove the contribution that this client gave to our
      * incrementally computed memory usage. */
    server.stat_clients_type_memory[c->client_cron_last_memory_type] -=
            c->client_cron_last_memory_usage;

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    if (c->name) decrRefCount(c->name);
    zfree(c->argv);
    c->argv_len_sum = 0;
    freeClientMultiState(c);
    sdsfree(c->peerid);
    sdsfree(c->sockname);
    sdsfree(c->slave_addr);
    zfree(c);
}

/* Schedule a client to free it at a safe time in the serverCron() function.
 * This function is useful when we need to terminate a client but we are in
 * a context where calling freeClient() is not possible, because the client
 * should be valid for the continuation of the flow of the program. */
void freeClientAsync(client *c) {
    /* We need to handle concurrent access to the server.clients_to_close list
     * only in the freeClientAsync() function, since it's the only function that
     * may access the list while Redis uses I/O threads. All the other accesses
     * are in the context of the main thread while the other threads are
     * idle. */
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_LUA) return;
    c->flags |= CLIENT_CLOSE_ASAP;
    if (server.io_threads_num == 1) {
        /* no need to bother with locking if there's just one thread (the main thread) */
        listAddNodeTail(server.clients_to_close, c);
        return;
    }
    static pthread_mutex_t async_free_queue_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&async_free_queue_mutex);
    listAddNodeTail(server.clients_to_close, c);
    pthread_mutex_unlock(&async_free_queue_mutex);
}

/* Free the clients marked as CLOSE_ASAP, return the number of clients
 * freed. */
int freeClientsInAsyncFreeQueue(void) {
    int freed = 0;
    listIter li;
    listNode *ln;

    listRewind(server.clients_to_close, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = listNodeValue(ln);

        if (c->flags & CLIENT_PROTECTED) continue;

        c->flags &= ~CLIENT_CLOSE_ASAP;
        freeClient(c);
        listDelNode(server.clients_to_close, ln);
        freed++;
    }
    return freed;
}

/* Return a client by ID, or NULL if the client ID is not in the set
 * of registered clients. Note that "fake clients", created with -1 as FD,
 * are not registered clients. */
client *lookupClientByID(uint64_t id) {
    id = htonu64(id);
    client *c = raxFind(server.clients_index, (unsigned char *) &id, sizeof(id));
    return (c == raxNotFound) ? NULL : c;
}

/* Write data in output buffers to client. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed because of some
 * error.  If handler_installed is set, it will attempt to clear the
 * write event.
 *
 * 将输出缓冲区中的数据写入客户端。
 *
 * This function is called by threads, but always with handler_installed
 * set to 0. So when handler_installed is set to 0 the function must be
 * thread safe. */
int writeToClient(client *c, int handler_installed) {
    /* Update total number of writes on server */
    atomicIncr(server.stat_total_writes_processed, 1);

    ssize_t nwritten = 0, totwritten = 0;
    size_t objlen;
    clientReplyBlock *o;

    // 一直循环，直到回复缓冲区为空或者指定条件满足为止
    while (clientHasPendingReplies(c)) {
        if (c->bufpos > 0) {

            // 写入内容到套接字
            // c->sentlen 是用来处理 short write 的，当出现 short write ，导致写入未能一次完成时，
            // c-buf+c->sentlen 就会偏移到正确（未写入）内容的位置上
            nwritten = connWrite(c->conn, c->buf + c->sentlen, c->bufpos - c->sentlen);

            // 出错则跳出
            if (nwritten <= 0) break;

            // 成功写入则更新写入计数器变量
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            // 如果缓冲区中的内容已经全部写入完毕
            // 那么清空客户端的两个计数器变量
            if ((int) c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {

            // 取出位于链表最前面的对象
            o = listNodeValue(listFirst(c->reply));
            objlen = o->used;

            // 略过空对象
            if (objlen == 0) {
                c->reply_bytes -= o->size;
                listDelNode(c->reply, listFirst(c->reply));
                continue;
            }

            // 写入内容到套接字
            // c->sentlen 是用来处理 short write 的
            // 当出现 short write ，导致写入未能一次完成时，
            // c->buf+c->sentlen 就会偏移到正确（未写入）内容的位置上。
            nwritten = connWrite(c->conn, o->buf + c->sentlen, objlen - c->sentlen);
            if (nwritten <= 0) break;
            c->sentlen += nwritten;
            totwritten += nwritten;

            // 如果缓冲区内容全部写入完毕，那么删除已写入完毕的节点
            /* If we fully sent the object on head go to the next one */
            if (c->sentlen == objlen) {
                c->reply_bytes -= o->size;
                listDelNode(c->reply, listFirst(c->reply));
                c->sentlen = 0;
                /* If there are no longer objects in the list, we expect
                 * the count of reply bytes to be exactly zero. */
                if (listLength(c->reply) == 0)
                    serverAssert(c->reply_bytes == 0);
            }
        }
        /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interface).
         *
         * 为了避免一个非常大的回复独占服务器，
         * 当写入的总数量大于 REDIS_MAX_WRITE_PER_EVENT ，
         * 临时中断写入，将处理时间让给其他客户端，
         * 剩余的内容等下次写入就绪再继续写入
         *
         * However if we are over the maxmemory limit we ignore that and
         * just deliver as much data as it is possible to deliver.
         *
         * Moreover, we also send as much as possible if the client is
         * a slave or a monitor (otherwise, on high-speed traffic, the
         * replication/output buffer will grow indefinitely)
         *
         * 不过，如果服务器的内存占用已经超过了限制，
         * 那么为了将回复缓冲区中的内容尽快写入给客户端，
         * 然后释放回复缓冲区的空间来回收内存，
         * 这时即使写入量超过了 REDIS_MAX_WRITE_PER_EVENT ，
         * 程序也继续进行写入
         */
        if (totwritten > NET_MAX_WRITES_PER_EVENT &&
            (server.maxmemory == 0 ||
             zmalloc_used_memory() < server.maxmemory) &&
            !(c->flags & CLIENT_SLAVE))
            // 不跳出就继续写入
            break;
    }
    atomicIncr(server.stat_net_output_bytes, totwritten);

    // 写入出错检查
    if (nwritten == -1) {
        if (connGetState(c->conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_VERBOSE,
                      "Error writing to client: %s", connGetLastError(c->conn));
            freeClientAsync(c);
            return C_ERR;
        }
    }
    if (totwritten > 0) {
        /* For clients representing masters we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        if (!(c->flags & CLIENT_MASTER)) c->lastinteraction = server.unixtime;
    }
    if (!clientHasPendingReplies(c)) {
        c->sentlen = 0;
        /* Note that writeToClient() is called in a threaded way, but
         * adDeleteFileEvent() is not thread safe: however writeToClient()
         * is always called with handler_installed set to 0 from threads
         * so we are fine. */
        if (handler_installed) connSetWriteHandler(c->conn, NULL);

        /* Close connection after entire reply has been sent. */
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
            freeClientAsync(c);
            return C_ERR;
        }
    }
    return C_OK;
}

/* Write event handler. Just send data to the client.
 *
 * 命令回复处理器，负责将服务器执行命令后得到的结果通过套接字返回给客户端。
 * 说明：
 *  而当实例需要向客户端写回数据时，实例会在事件驱动框架中注册可写事件，并使用 sendReplyToClient 作为回调函数，将缓冲区中数据写回客户端
 *
 * 当服务器有命令回复需要传送给客户端的时候，服务器会将客户端套接字的 AE_WRITEABLE 事件和命令回复处理器关联起来，
 * 当客户端准备好接收服务器传回的结果时，就会产生 AE_WRITABLE 事件，引发命令回复处理器执行，并执行相应的套接字写入操作。
 *
 * 当命令回复发送完毕后，服务器就会解除命令回复处理器与客户端套接字的 AE_WRITEABLE 事件之间的关联
 */
void sendReplyToClient(connection *conn) {
    client *c = connGetPrivateData(conn);
    writeToClient(c, 1);
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. */
int handleClientsWithPendingWrites(void) {
    listIter li;
    listNode *ln;
    int processed = listLength(server.clients_pending_write);

    // 从  全局等待写队列 中获取待写回的客户端
    listRewind(server.clients_pending_write, &li);

    // 遍历每一个待写回的客户端
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
        listDelNode(server.clients_pending_write, ln);

        /* If a client is protected, don't do anything,
         * that may trigger write error or recreate handler. */
        if (c->flags & CLIENT_PROTECTED) continue;

        /* Don't write to clients that are going to be closed anyway. */
        if (c->flags & CLIENT_CLOSE_ASAP) continue;

        /* Try to write buffers to the client socket. */
        // 调用writeToClient将当前客户端的输出缓冲区数据写回
        if (writeToClient(c, 0) == C_ERR) continue;

        /* If after the synchronous writes above we still have data to
         * output to the client, we need to install the writable handler. */
        // 如果还有待写回数据，那么需要重新关联写事件处理函数
        if (clientHasPendingReplies(c)) {
            int ae_barrier = 0;
            /* For the fsync=always policy, we want that a given FD is never
             * served for reading and writing in the same event loop iteration,
             * so that in the middle of receiving the query, and serving it
             * to the client, we'll call beforeSleep() that will do the
             * actual fsync of AOF to disk. the write barrier ensures that. */
            if (server.aof_state == AOF_ON &&
                server.aof_fsync == AOF_FSYNC_ALWAYS) {
                ae_barrier = 1;
            }

            /*
             * 相当于
             *  // 创建可写事件的监听，以及设置回调函数
             *  if (aeCreateFileEvent(server.el, c->fd, ae_flags,sendReplyToClient, c) == AE_ERR)
               {
                    freeClientAsync(c);
                }
             */
            if (connSetWriteHandlerWithBarrier(c->conn, sendReplyToClient, ae_barrier) == C_ERR) {
                freeClientAsync(c);
            }
        }
    }

    return processed;
}

/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
    redisCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL;

    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;

    /* We clear the ASKING flag as well if we are not inside a MULTI, and
     * if what we just executed is not the ASKING command itself. */
    if (!(c->flags & CLIENT_MULTI) && prevcmd != askingCommand)
        c->flags &= ~CLIENT_ASKING;

    /* We do the same for the CACHING command as well. It also affects
     * the next command or transaction executed, in a way very similar
     * to ASKING. */
    if (!(c->flags & CLIENT_MULTI) && prevcmd != clientCommand)
        c->flags &= ~CLIENT_TRACKING_CACHING;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    c->flags &= ~CLIENT_REPLY_SKIP;
    if (c->flags & CLIENT_REPLY_SKIP_NEXT) {
        c->flags |= CLIENT_REPLY_SKIP;
        c->flags &= ~CLIENT_REPLY_SKIP_NEXT;
    }
}

/* This function is used when we want to re-enter the event loop but there
 * is the risk that the client we are dealing with will be freed in some
 * way. This happens for instance in:
 *
 * * DEBUG RELOAD and similar.
 * * When a Lua script is in -BUSY state.
 *
 * So the function will protect the client by doing two things:
 *
 * 1) It removes the file events. This way it is not possible that an
 *    error is signaled on the socket, freeing the client.
 * 2) Moreover it makes sure that if the client is freed in a different code
 *    path, it is not really released, but only marked for later release. */
void protectClient(client *c) {
    c->flags |= CLIENT_PROTECTED;
    if (c->conn) {
        connSetReadHandler(c->conn, NULL);
        connSetWriteHandler(c->conn, NULL);
    }
}

/* This will undo the client protection done by protectClient() */
void unprotectClient(client *c) {
    if (c->flags & CLIENT_PROTECTED) {
        c->flags &= ~CLIENT_PROTECTED;
        if (c->conn) {
            connSetReadHandler(c->conn, readQueryFromClient);
            if (clientHasPendingReplies(c)) clientInstallWriteHandler(c);
        }
    }
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure. Returns C_OK if the command
 * is ready to be executed, or C_ERR if there is still protocol to read to
 * have a well formed command. The function also returns C_ERR when there is
 * a protocol error: in such a case the client structure is setup to reply
 * with the error and close the connection. */
int processInlineBuffer(client *c) {
    char *newline;
    int argc, j, linefeed_chars = 1;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(c->querybuf + c->qb_pos, '\n');

    /* Nothing to do without a \r\n */
    if (newline == NULL) {
        if (sdslen(c->querybuf) - c->qb_pos > PROTO_INLINE_MAX_SIZE) {
            addReplyError(c, "Protocol error: too big inline request");
            setProtocolError("too big inline request", c);
        }
        return C_ERR;
    }

    /* Handle the \r\n case. */
    if (newline != c->querybuf + c->qb_pos && *(newline - 1) == '\r')
        newline--, linefeed_chars++;

    /* Split the input buffer up to the \r\n */
    querylen = newline - (c->querybuf + c->qb_pos);
    aux = sdsnewlen(c->querybuf + c->qb_pos, querylen);
    argv = sdssplitargs(aux, &argc);
    sdsfree(aux);
    if (argv == NULL) {
        addReplyError(c, "Protocol error: unbalanced quotes in request");
        setProtocolError("unbalanced quotes in inline request", c);
        return C_ERR;
    }

    /* Newline from slaves can be used to refresh the last ACK time.
     * This is useful for a slave to ping back while loading a big
     * RDB file. */
    if (querylen == 0 && getClientType(c) == CLIENT_TYPE_SLAVE)
        c->repl_ack_time = server.unixtime;

    /* Masters should never send us inline protocol to run actual
     * commands. If this happens, it is likely due to a bug in Redis where
     * we got some desynchronization in the protocol, for example
     * beause of a PSYNC gone bad.
     *
     * However the is an exception: masters may send us just a newline
     * to keep the connection active. */
    if (querylen != 0 && c->flags & CLIENT_MASTER) {
        sdsfreesplitres(argv, argc);
        serverLog(LL_WARNING,
                  "WARNING: Receiving inline protocol from master, master stream corruption? Closing the master connection and discarding the cached master.");
        setProtocolError("Master using the inline protocol. Desync?", c);
        return C_ERR;
    }

    /* Move querybuffer position to the next query in the buffer. */
    c->qb_pos += querylen + linefeed_chars;

    /* Setup argv array on client structure */
    if (argc) {
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj *) * argc);
        c->argv_len_sum = 0;
    }

    /* Create redis objects for all arguments. */
    for (c->argc = 0, j = 0; j < argc; j++) {
        c->argv[c->argc] = createObject(OBJ_STRING, argv[j]);
        c->argc++;
        c->argv_len_sum += sdslen(argv[j]);
    }
    zfree(argv);
    return C_OK;
}

/* Helper function. Record protocol erro details in server log,
 * and set the client as CLIENT_CLOSE_AFTER_REPLY and
 * CLIENT_PROTOCOL_ERROR. */
#define PROTO_DUMP_LEN 128

static void setProtocolError(const char *errstr, client *c) {
    if (server.verbosity <= LL_VERBOSE || c->flags & CLIENT_MASTER) {
        sds client = catClientInfoString(sdsempty(), c);

        /* Sample some protocol to given an idea about what was inside. */
        char buf[256];
        if (sdslen(c->querybuf) - c->qb_pos < PROTO_DUMP_LEN) {
            snprintf(buf, sizeof(buf), "Query buffer during protocol error: '%s'", c->querybuf + c->qb_pos);
        } else {
            snprintf(buf, sizeof(buf), "Query buffer during protocol error: '%.*s' (... more %zu bytes ...) '%.*s'",
                     PROTO_DUMP_LEN / 2, c->querybuf + c->qb_pos, sdslen(c->querybuf) - c->qb_pos - PROTO_DUMP_LEN,
                     PROTO_DUMP_LEN / 2, c->querybuf + sdslen(c->querybuf) - PROTO_DUMP_LEN / 2);
        }

        /* Remove non printable chars. */
        char *p = buf;
        while (*p != '\0') {
            if (!isprint(*p)) *p = '.';
            p++;
        }

        /* Log all the client and protocol info. */
        int loglevel = (c->flags & CLIENT_MASTER) ? LL_WARNING :
                       LL_VERBOSE;
        serverLog(loglevel,
                  "Protocol error (%s) from client: %s. %s", errstr, client, buf);
        sdsfree(client);
    }
    c->flags |= (CLIENT_CLOSE_AFTER_REPLY | CLIENT_PROTOCOL_ERROR);
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution. Returns C_OK if after running the function
 * the client has a well-formed ready to be processed command, otherwise
 * C_ERR if there is still to read more buffer to get the full command.
 * The function also returns C_ERR when there is a protocol error: in such a
 * case the client structure is setup to reply with the error and close
 * the connection.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */
int processMultibulkBuffer(client *c) {
    char *newline = NULL;
    int ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        serverAssertWithInfo(c, NULL, c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf + c->qb_pos, '\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf) - c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                addReplyError(c, "Protocol error: too big mbulk count string");
                setProtocolError("too big mbulk count string", c);
            }
            return C_ERR;
        }

        /* Buffer should also contain \n */
        if (newline - (c->querybuf + c->qb_pos) > (ssize_t) (sdslen(c->querybuf) - c->qb_pos - 2))
            return C_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        serverAssertWithInfo(c, NULL, c->querybuf[c->qb_pos] == '*');
        ok = string2ll(c->querybuf + 1 + c->qb_pos, newline - (c->querybuf + 1 + c->qb_pos), &ll);
        if (!ok || ll > 1024 * 1024) {
            addReplyError(c, "Protocol error: invalid multibulk length");
            setProtocolError("invalid mbulk count", c);
            return C_ERR;
        }

        c->qb_pos = (newline - c->querybuf) + 2;

        if (ll <= 0) return C_OK;

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj *) * c->multibulklen);
        c->argv_len_sum = 0;
    }

    serverAssertWithInfo(c, NULL, c->multibulklen > 0);
    while (c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf + c->qb_pos, '\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf) - c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                    addReplyError(c,
                                  "Protocol error: too big bulk count string");
                    setProtocolError("too big bulk count string", c);
                    return C_ERR;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline - (c->querybuf + c->qb_pos) > (ssize_t) (sdslen(c->querybuf) - c->qb_pos - 2))
                break;

            if (c->querybuf[c->qb_pos] != '$') {
                addReplyErrorFormat(c,
                                    "Protocol error: expected '$', got '%c'",
                                    c->querybuf[c->qb_pos]);
                setProtocolError("expected $ but got something else", c);
                return C_ERR;
            }

            ok = string2ll(c->querybuf + c->qb_pos + 1, newline - (c->querybuf + c->qb_pos + 1), &ll);
            if (!ok || ll < 0 ||
                (!(c->flags & CLIENT_MASTER) && ll > server.proto_max_bulk_len)) {
                addReplyError(c, "Protocol error: invalid bulk length");
                setProtocolError("invalid bulk length", c);
                return C_ERR;
            }

            c->qb_pos = newline - c->querybuf + 2;
            if (ll >= PROTO_MBULK_BIG_ARG) {
                /* If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data.
                 *
                 * But only when the data we have not parsed is less than
                 * or equal to ll+2. If the data length is greater than
                 * ll+2, trimming querybuf is just a waste of time, because
                 * at this time the querybuf contains not only our bulk. */
                if (sdslen(c->querybuf) - c->qb_pos <= (size_t) ll + 2) {
                    sdsrange(c->querybuf, c->qb_pos, -1);
                    c->qb_pos = 0;
                    /* Hint the sds library about the amount of bytes this string is
                     * going to contain. */
                    c->querybuf = sdsMakeRoomFor(c->querybuf, ll + 2 - sdslen(c->querybuf));
                }
            }
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf) - c->qb_pos < (size_t) (c->bulklen + 2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Optimization: if the buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            if (c->qb_pos == 0 &&
                c->bulklen >= PROTO_MBULK_BIG_ARG &&
                sdslen(c->querybuf) == (size_t) (c->bulklen + 2)) {
                c->argv[c->argc++] = createObject(OBJ_STRING, c->querybuf);
                c->argv_len_sum += c->bulklen;
                sdsIncrLen(c->querybuf, -2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                c->querybuf = sdsnewlen(SDS_NOINIT, c->bulklen + 2);
                sdsclear(c->querybuf);
            } else {
                c->argv[c->argc++] =
                        createStringObject(c->querybuf + c->qb_pos, c->bulklen);
                c->argv_len_sum += c->bulklen;
                c->qb_pos += c->bulklen + 2;
            }
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) return C_OK;

    /* Still not ready to process the command */
    return C_ERR;
}

/* Perform necessary tasks after a command was executed:
 *
 * 1. The client is reset unless there are reasons to avoid doing it.
 * 2. In the case of master clients, the replication offset is updated.
 * 3. Propagate commands we got from our master to replicas down the line. */
void commandProcessed(client *c) {
    long long prev_offset = c->reploff;
    if (c->flags & CLIENT_MASTER && !(c->flags & CLIENT_MULTI)) {
        /* Update the applied replication offset of our master. */
        c->reploff = c->read_reploff - sdslen(c->querybuf) + c->qb_pos;
    }

    /* Don't reset the client structure for blocked clients, so that the reply
     * callback will still be able to access the client argv and argc fields.
     * The client will be reset in unblockClient(). */
    if (!(c->flags & CLIENT_BLOCKED)) {
        resetClient(c);
    }

    /* If the client is a master we need to compute the difference
     * between the applied offset before and after processing the buffer,
     * to understand how much of the replication stream was actually
     * applied to the master state: this quantity, and its corresponding
     * part of the replication stream, will be propagated to the
     * sub-replicas and to the replication backlog. */
    if (c->flags & CLIENT_MASTER) {
        long long applied = c->reploff - prev_offset;
        if (applied) {
            replicationFeedSlavesFromMasterStream(server.slaves,
                                                  c->pending_querybuf, applied);
            sdsrange(c->pending_querybuf, applied, -1);
        }
    }
}

/* This function calls processCommand(), but also performs a few sub tasks
 * for the client that are useful in that context:
 *
 * 1. It sets the current client to the client 'c'.
 * 2. calls commandProcessed() if the command was handled.
 *
 * The function returns C_ERR in case the client was freed as a side effect
 * of processing the command, otherwise C_OK is returned.
 *
 * */
int processCommandAndResetClient(client *c) {
    int deadclient = 0;
    // 获取 server 的 当前客户端
    client *old_client = server.current_client;
    // 将 server 的当前客户端重置为 c
    server.current_client = c;

    // 执行命令
    if (processCommand(c) == C_OK) {
        commandProcessed(c);
    }
    if (server.current_client == NULL) deadclient = 1;
    /*
     * Restore the old client, this is needed because when a script
     * times out, we will get into this code from processEventsWhileBlocked.
     * Which will cause to set the server.current_client. If not restored
     * we will return 1 to our caller which will falsely indicate the client
     * is dead and will stop reading from its buffer.
     */
    server.current_client = old_client;
    /* performEvictions may flush slave output buffers. This may
     * result in a slave, that may be the active client, to be
     * freed. */
    return deadclient ? C_ERR : C_OK;
}


/* This function will execute any fully parsed commands pending on
 * the client. Returns C_ERR if the client is no longer valid after executing
 * the command, and C_OK for all other cases.
 *
 * 该函数用于执行客户端上完全解析的命令。
 */
int processPendingCommandsAndResetClient(client *c) {
    // 判断客户端的标识中是否有 CLIENT_PENDING_COMMAND，如果有 CLIENT_PENDING_COMMAND 标识，表明该客户端中的命令已经被某一个 IO 线程解析过，已经可以被执行了。
    if (c->flags & CLIENT_PENDING_COMMAND) {

        // 重置客户端标识
        c->flags &= ~CLIENT_PENDING_COMMAND;

        // 执行命令
        if (processCommandAndResetClient(c) == C_ERR) {
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process.
 *
 * 解析输入缓冲区
 */
void processInputBuffer(client *c) {
    /* Keep processing while there is something in the input buffer */
    // 不断地从客户端的输入缓冲区中读取数据（客户端的输入缓冲区的数据是从连接的 Socket 中读取的）
    while (c->qb_pos < sdslen(c->querybuf)) {

        /* Immediately abort if the client is in the middle of something. */
        // 如果客户正在处理某件事，请立即中止
        if (c->flags & CLIENT_BLOCKED) break;

        /* Don't process more buffers from clients that have already pending
         * commands to execute in c->argv. */
        // 不要处理来自c->argv中已经挂起命令要执行的客户端的更多缓冲区。
        if (c->flags & CLIENT_PENDING_COMMAND) break;

        /* Don't process input from the master while there is a busy script
         * condition on the slave. We want just to accumulate the replication
         * stream (instead of replying -BUSY like we do with other clients) and
         * later resume the processing. */
        if (server.lua_timedout && c->flags & CLIENT_MASTER) break;

        /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands).
         *
         * The same applies for clients we want to terminate ASAP. */
        if (c->flags & (CLIENT_CLOSE_AFTER_REPLY | CLIENT_CLOSE_ASAP)) break;

        /* Determine request type when unknown. */
        // 请求类型未知时，根据客户端输入缓冲区的命令开头字符判断命令类型
        if (!c->reqtype) {
            // 如果命令是以 "*" 开头，表示该命令是 PROTO_REQ_MULTIBULK 类型的命令，符合 RESP 协议，
            // 那么后续会调用 processMultibulkBuffer 函数来解析读取到的命令
            if (c->querybuf[c->qb_pos] == '*') {
                c->reqtype = PROTO_REQ_MULTIBULK; // 符合 RESP 协议的命令（Redis 客户端与服务器端的标准通信协议的请求）

                // 如果命令不是以 "*" 开头，那表示该命令是 PROTO_REQ_INLINE 类型的命令，并不是 RESP 协议请求，
                // 这类命令也被称为管道命令，命令和命令之间是使用换行符“\r\n”分隔开来的。比如，我们使用 Telnet 发送给 Redis 的命令，就是属于 PROTO_REQ_INLINE 类型的命令。
                // 那么后续会调用 processInlineBuffer 函数来解析读取到的命令
            } else {
                c->reqtype = PROTO_REQ_INLINE; // 管道类型命令
            }
        }

        // 对于管道类型命令，调用processInlineBuffer函数解析
        // 管道命令，命令和命令之间是使用换行符 \r\n 分隔开来的，比如我们使用 telnet 发送给 Redis 的命令
        if (c->reqtype == PROTO_REQ_INLINE) {
            if (processInlineBuffer(c) != C_OK) break;
            /* If the Gopher mode and we got zero or one argument, process
             * the request in Gopher mode. To avoid data race, Redis won't
             * support Gopher if enable io threads to read queries. */
            if (server.gopher_enabled && !server.io_threads_do_reads &&
                ((c->argc == 1 && ((char *) (c->argv[0]->ptr))[0] == '/') ||
                 c->argc == 0)) {
                processGopherRequest(c);
                resetClient(c);
                c->flags |= CLIENT_CLOSE_AFTER_REPLY;
                break;
            }

            // 对于 RESP 协议命令，调用 processMultibulkBuffer 函数解析输入缓冲区，将命令请求解析为：命令及其参数，每部分都是字符串对象
        } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
            // 解析读取到的命令
            if (processMultibulkBuffer(c) != C_OK) break;

        } else {
            serverPanic("Unknown request type");
        }


        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            /* If we are in the context of an I/O thread, we can't really
             * execute the command here. All we can do is to flag the client
             * as one that needs to process the command.
             *
             * 如果我们在一个I/O线程的上下文中，我们不能在这里真正执行这个命令。
             * 我们所能做的就是将客户机标记为需要处理该命令的客户机 ，即 CLIENT_PENDING_COMMAND ，
             * 然后退出命令解析的循环流程，不会执行命令。
             *
             * 说明：此时 processInputBuffer 函数只是解析了第一个命令，但并不会执行命令。
             */
            if (c->flags & CLIENT_PENDING_READ) {
                // 标记客户端为待处理状态，然后，退出命令解析的循环流程。并不会执行命令。
                c->flags |= CLIENT_PENDING_COMMAND;
                break;
            }

            // 不是推迟处理的
            /* We are finally ready to execute the command. */
            //  执行客户端请求的命令
            if (processCommandAndResetClient(c) == C_ERR) {
                /* If the client is no longer valid, we avoid exiting this
                 * loop and trimming the client buffer later. So we return
                 * ASAP in that case. */
                return;
            }
        }
    }

    /* Trim to pos */
    if (c->qb_pos) {
        sdsrange(c->querybuf, c->qb_pos, -1);
        c->qb_pos = 0;
    }
}

/**
 * 从客户端连接的 socket 中，读取最大为 readlen 长度的数据,readlen 值大小是宏定义 PROTO_IOBUF_LEN ,默认值为 16KB
 *
 * Redis 命令处理器 - 读事件处理，负责从套接字中读入客户端发送的命令请求内容，然后放到 client 中，随后调用 processInputBuffer 处理实际的命令请求。
 * 特别说明：
 * 1 当 server 和客户端完成连接建立后，server 会在已连接套接字上监听可读事件，并使用 readQueryFromClient 函数处理客户端读写请求
 * 2 无论客户端发送的请求是读或写操作，对于 server 来说，都是要读取客户端的请求并解析处理。所以，server 在客户端的已连接套接字上注册的是可读事件。
 *
 *
 * 当一个客户端通过连接应答处理器成功连接到服务器之后，服务器会将客户端套接字的AE_READABLE 事件和命令请求处理器关联起来，
 * 当客户端向服务器发送命令请求的时候，套接字就会产生 AE_READABLE 事件，引发命令请求处理器执行，并执行相应的套接字读入操作。
 *
 * 在客户端连接服务器的整个过程中，服务器都会一直为客户端套接字的 AE_READABLE 事件关联命令请求处理器。
 *
 * todo readQueryFromClient 主要是和 querybuf 打交道（对应读事件），里面涉及到RESP编解码可以深度阅读一下（Redis处理粘包拆包的方式是怎样的）。
 */
void readQueryFromClient(connection *conn) {
    // 从 connection 中获取客户端 Client
    client *c = connGetPrivateData(conn);
    int nread, readlen;
    size_t qblen;

    /* Check if we want to read from the client later when exiting from
     * the event loop. This is the case if threaded I/O is enabled.
     *
     * 是否交给 IO 线程处理，即将请求放入全局读队列中
     */
    // 判断是否推迟从客户端读取数据，如果允许则将客户端添加到 server 全局等待读队列中
    if (postponeClientRead(c)) return;

    /* Update total number of reads on server */
    atomicIncr(server.stat_total_reads_processed, 1);

    // 从客户端连接的 socket 中读入数据的最大长度（默认为 16 MB）
    readlen = PROTO_IOBUF_LEN;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * Redis Object representing the argument. */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1
        && c->bulklen >= PROTO_MBULK_BIG_ARG) {
        ssize_t remaining = (size_t) (c->bulklen + 2) - sdslen(c->querybuf);

        /* Note that the 'remaining' variable may be zero in some edge case,
         * for example once we resume a blocked client after CLIENT PAUSE. */
        if (remaining > 0 && remaining < readlen) readlen = remaining;
    }

    // 获取查询缓冲区当前内容的长度
    // 如果读取出现 short read ，那么可能会有内容滞留在读取缓冲区里面
    // 这些滞留内容也许不能完整构成一个符合协议的命令，
    qblen = sdslen(c->querybuf);

    // 如果有需要，更新缓冲区内容长度的峰值（peak）
    if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;

    // 为缓冲区分配空间
    c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);

    // todo 调用 c->conn->type->read 函数从描述符为fd的客户端socket中读取数据，放到缓冲区中
    nread = connRead(c->conn, c->querybuf + qblen, readlen);

    // 处理读取异常的情况，并结束返回
    // 读入出错
    if (nread == -1) {
        if (connGetState(conn) == CONN_STATE_CONNECTED) {
            return;
        } else {
            serverLog(LL_VERBOSE, "Reading from client: %s", connGetLastError(c->conn));
            freeClientAsync(c);
            return;
        }

        // 遇到 EOF
    } else if (nread == 0) {
        serverLog(LL_VERBOSE, "Client closed connection");
        freeClientAsync(c);
        return;


        // 当前客户端是主从复制中的主节点
    } else if (c->flags & CLIENT_MASTER) {
        /* Append the query buffer to the pending (not applied) buffer
         * of the master. We'll use this buffer later in order to have a
         * copy of the string applied by the last command executed. */
        // 将命令追加到同步缓冲区，并修改同步偏移量
        c->pending_querybuf = sdscatlen(c->pending_querybuf,
                                        c->querybuf + qblen, nread);
    }

    sdsIncrLen(c->querybuf, nread);
    c->lastinteraction = server.unixtime;
    if (c->flags & CLIENT_MASTER) c->read_reploff += nread;
    atomicIncr(server.stat_net_input_bytes, nread);
    if (sdslen(c->querybuf) > server.client_max_querybuf_len) {
        sds ci = catClientInfoString(sdsempty(), c), bytes = sdsempty();

        bytes = sdscatrepr(bytes, c->querybuf, 64);
        serverLog(LL_WARNING, "Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci,
                  bytes);
        sdsfree(ci);
        sdsfree(bytes);
        freeClientAsync(c);
        return;
    }

    /* There is more data in the client input buffer, continue parsing it
     * in case to check if there is a full command to execute.
     *
     * 处理读的内容，也就命令解析
     *
     * 该方法处理实际的命令请求，执行链路：
     * processInputBuffer -> processCommandAndResetClient->processCommand(各种reject不符合条件的命令)->call。
     */
    processInputBuffer(c);
}

void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    client *c;
    listNode *ln;
    listIter li;
    unsigned long lol = 0, bib = 0;

    listRewind(server.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);

        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

/* A Redis "Address String" is a colon separated ip:port pair.
 * For IPv4 it's in the form x.y.z.k:port, example: "127.0.0.1:1234".
 * For IPv6 addresses we use [] around the IP part, like in "[::1]:1234".
 * For Unix sockets we use path:0, like in "/tmp/redis:0".
 *
 * An Address String always fits inside a buffer of NET_ADDR_STR_LEN bytes,
 * including the null term.
 *
 * On failure the function still populates 'addr' with the "?:0" string in case
 * you want to relax error checking or need to display something anyway (see
 * anetFdToString implementation for more info). */
void genClientAddrString(client *client, char *addr,
                         size_t addr_len, int fd_to_str_type) {
    if (client->flags & CLIENT_UNIX_SOCKET) {
        /* Unix socket client. */
        snprintf(addr, addr_len, "%s:0", server.unixsocket);
    } else {
        /* TCP client. */
        connFormatFdAddr(client->conn, addr, addr_len, fd_to_str_type);
    }
}

/* This function returns the client peer id, by creating and caching it
 * if client->peerid is NULL, otherwise returning the cached value.
 * The Peer ID never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientPeerId(client *c) {
    char peerid[NET_ADDR_STR_LEN];

    if (c->peerid == NULL) {
        genClientAddrString(c, peerid, sizeof(peerid), FD_TO_PEER_NAME);
        c->peerid = sdsnew(peerid);
    }
    return c->peerid;
}

/* This function returns the client bound socket name, by creating and caching
 * it if client->sockname is NULL, otherwise returning the cached value.
 * The Socket Name never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientSockname(client *c) {
    char sockname[NET_ADDR_STR_LEN];

    if (c->sockname == NULL) {
        genClientAddrString(c, sockname, sizeof(sockname), FD_TO_SOCK_NAME);
        c->sockname = sdsnew(sockname);
    }
    return c->sockname;
}

/* Concatenate a string representing the state of a client in a human
 * readable format, into the sds string 's'. */
sds catClientInfoString(sds s, client *client) {
    char flags[16], events[3], conninfo[CONN_INFO_LEN], *p;

    p = flags;
    if (client->flags & CLIENT_SLAVE) {
        if (client->flags & CLIENT_MONITOR)
            *p++ = 'O';
        else
            *p++ = 'S';
    }
    if (client->flags & CLIENT_MASTER) *p++ = 'M';
    if (client->flags & CLIENT_PUBSUB) *p++ = 'P';
    if (client->flags & CLIENT_MULTI) *p++ = 'x';
    if (client->flags & CLIENT_BLOCKED) *p++ = 'b';
    if (client->flags & CLIENT_TRACKING) *p++ = 't';
    if (client->flags & CLIENT_TRACKING_BROKEN_REDIR) *p++ = 'R';
    if (client->flags & CLIENT_TRACKING_BCAST) *p++ = 'B';
    if (client->flags & CLIENT_DIRTY_CAS) *p++ = 'd';
    if (client->flags & CLIENT_CLOSE_AFTER_REPLY) *p++ = 'c';
    if (client->flags & CLIENT_UNBLOCKED) *p++ = 'u';
    if (client->flags & CLIENT_CLOSE_ASAP) *p++ = 'A';
    if (client->flags & CLIENT_UNIX_SOCKET) *p++ = 'U';
    if (client->flags & CLIENT_READONLY) *p++ = 'r';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    p = events;
    if (client->conn) {
        if (connHasReadHandler(client->conn)) *p++ = 'r';
        if (connHasWriteHandler(client->conn)) *p++ = 'w';
    }
    *p = '\0';

    /* Compute the total memory consumed by this client. */
    size_t obufmem = getClientOutputBufferMemoryUsage(client);
    size_t total_mem = obufmem;
    total_mem += zmalloc_size(client); /* includes client->buf */
    total_mem += sdsZmallocSize(client->querybuf);
    /* For efficiency (less work keeping track of the argv memory), it doesn't include the used memory
     * i.e. unused sds space and internal fragmentation, just the string length. but this is enough to
     * spot problematic clients. */
    total_mem += client->argv_len_sum;
    if (client->argv)
        total_mem += zmalloc_size(client->argv);

    return sdscatfmt(s,
                     "id=%U addr=%s laddr=%s %s name=%s age=%I idle=%I flags=%s db=%i sub=%i psub=%i multi=%i qbuf=%U qbuf-free=%U argv-mem=%U obl=%U oll=%U omem=%U tot-mem=%U events=%s cmd=%s user=%s redir=%I",
                     (unsigned long long) client->id,
                     getClientPeerId(client),
                     getClientSockname(client),
                     connGetInfo(client->conn, conninfo, sizeof(conninfo)),
                     client->name ? (char *) client->name->ptr : "",
                     (long long) (server.unixtime - client->ctime),
                     (long long) (server.unixtime - client->lastinteraction),
                     flags,
                     client->db->id,
                     (int) dictSize(client->pubsub_channels),
                     (int) listLength(client->pubsub_patterns),
                     (client->flags & CLIENT_MULTI) ? client->mstate.count : -1,
                     (unsigned long long) sdslen(client->querybuf),
                     (unsigned long long) sdsavail(client->querybuf),
                     (unsigned long long) client->argv_len_sum,
                     (unsigned long long) client->bufpos,
                     (unsigned long long) listLength(client->reply),
                     (unsigned long long) obufmem, /* should not include client->buf since we want to see 0 for static clients. */
                     (unsigned long long) total_mem,
                     events,
                     client->lastcmd ? client->lastcmd->name : "NULL",
                     client->user ? client->user->name : "(superuser)",
                     (client->flags & CLIENT_TRACKING) ? (long long) client->client_tracking_redirection : -1);
}

sds getAllClientsInfoString(int type) {
    listNode *ln;
    listIter li;
    client *client;
    sds o = sdsnewlen(SDS_NOINIT, 200 * listLength(server.clients));
    sdsclear(o);
    listRewind(server.clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client = listNodeValue(ln);
        if (type != -1 && getClientType(client) != type) continue;
        o = catClientInfoString(o, client);
        o = sdscatlen(o, "\n", 1);
    }
    return o;
}

/* This function implements CLIENT SETNAME, including replying to the
 * user with an error if the charset is wrong (in that case C_ERR is
 * returned). If the function succeeeded C_OK is returned, and it's up
 * to the caller to send a reply if needed.
 *
 * Setting an empty string as name has the effect of unsetting the
 * currently set name: the client will remain unnamed.
 *
 * This function is also used to implement the HELLO SETNAME option. */
int clientSetNameOrReply(client *c, robj *name) {
    int len = sdslen(name->ptr);
    char *p = name->ptr;

    /* Setting the client name to an empty string actually removes
     * the current name. */
    if (len == 0) {
        if (c->name) decrRefCount(c->name);
        c->name = NULL;
        return C_OK;
    }

    /* Otherwise check if the charset is ok. We need to do this otherwise
     * CLIENT LIST format will break. You should always be able to
     * split by space to get the different fields. */
    for (int j = 0; j < len; j++) {
        if (p[j] < '!' || p[j] > '~') { /* ASCII is assumed. */
            addReplyError(c,
                          "Client names cannot contain spaces, "
                          "newlines or special characters.");
            return C_ERR;
        }
    }
    if (c->name) decrRefCount(c->name);
    c->name = name;
    incrRefCount(name);
    return C_OK;
}

/* Reset the client state to resemble a newly connected client.
 */
void resetCommand(client *c) {
    listNode *ln;

    /* MONITOR clients are also marked with CLIENT_SLAVE, we need to
     * distinguish between the two.
     */
    if (c->flags & CLIENT_MONITOR) {
        ln = listSearchKey(server.monitors, c);
        serverAssert(ln != NULL);
        listDelNode(server.monitors, ln);

        c->flags &= ~(CLIENT_MONITOR | CLIENT_SLAVE);
    }

    if (c->flags & (CLIENT_SLAVE | CLIENT_MASTER | CLIENT_MODULE)) {
        addReplyError(c, "can only reset normal client connections");
        return;
    }

    if (c->flags & CLIENT_TRACKING) disableTracking(c);
    selectDb(c, 0);
    c->resp = 2;

    clientSetDefaultAuth(c);
    moduleNotifyUserChanged(c);
    discardTransaction(c);

    pubsubUnsubscribeAllChannels(c, 0);
    pubsubUnsubscribeAllPatterns(c, 0);

    if (c->name) {
        decrRefCount(c->name);
        c->name = NULL;
    }

    /* Selectively clear state flags not covered above */
    c->flags &= ~(CLIENT_ASKING | CLIENT_READONLY | CLIENT_PUBSUB |
                  CLIENT_REPLY_OFF | CLIENT_REPLY_SKIP_NEXT);

    addReplyStatus(c, "RESET");
}

void clientCommand(client *c) {
    listNode *ln;
    listIter li;

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "help")) {
        const char *help[] = {
                "CACHING (YES|NO)",
                "    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
                "GETREDIR",
                "    Return the client ID we are redirecting to when tracking is enabled.",
                "GETNAME",
                "    Return the name of the current connection.",
                "ID",
                "    Return the ID of the current connection.",
                "INFO",
                "    Return information about the current client connection.",
                "KILL <ip:port>",
                "    Kill connection made from <ip:port>.",
                "KILL <option> <value> [<option> <value> [...]]",
                "    Kill connections. Options are:",
                "    * ADDR (<ip:port>|<unixsocket>:0)",
                "      Kill connections made from the specified address",
                "    * LADDR (<ip:port>|<unixsocket>:0)",
                "      Kill connections made to specified local address",
                "    * TYPE (normal|master|replica|pubsub)",
                "      Kill connections by type.",
                "    * USER <username>",
                "      Kill connections authenticated by <username>.",
                "    * SKIPME (YES|NO)",
                "      Skip killing current connection (default: yes).",
                "LIST [options ...]",
                "    Return information about client connections. Options:",
                "    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
                "      Return clients of specified type.",
                "UNPAUSE",
                "    Stop the current client pause, resuming traffic.",
                "PAUSE <timeout> [WRITE|ALL]",
                "    Suspend all, or just write, clients for <timout> milliseconds.",
                "REPLY (ON|OFF|SKIP)",
                "    Control the replies sent to the current connection.",
                "SETNAME <name>",
                "    Assign the name <name> to the current connection.",
                "UNBLOCK <clientid> [TIMEOUT|ERROR]",
                "    Unblock the specified blocked client.",
                "TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
                "         [OPTIN] [OPTOUT]",
                "    Control server assisted client side caching.",
                "TRACKINGINFO",
                "    Report tracking status for the current connection.",
                NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr, "id") && c->argc == 2) {
        /* CLIENT ID */
        addReplyLongLong(c, c->id);
    } else if (!strcasecmp(c->argv[1]->ptr, "info") && c->argc == 2) {
        /* CLIENT INFO */
        sds o = catClientInfoString(sdsempty(), c);
        o = sdscatlen(o, "\n", 1);
        addReplyVerbatim(c, o, sdslen(o), "txt");
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr, "list")) {
        /* CLIENT LIST */
        int type = -1;
        sds o = NULL;
        if (c->argc == 4 && !strcasecmp(c->argv[2]->ptr, "type")) {
            type = getClientTypeByName(c->argv[3]->ptr);
            if (type == -1) {
                addReplyErrorFormat(c, "Unknown client type '%s'",
                                    (char *) c->argv[3]->ptr);
                return;
            }
        } else if (c->argc > 3 && !strcasecmp(c->argv[2]->ptr, "id")) {
            int j;
            o = sdsempty();
            for (j = 3; j < c->argc; j++) {
                long long cid;
                if (getLongLongFromObjectOrReply(c, c->argv[j], &cid,
                                                 "Invalid client ID")) {
                    sdsfree(o);
                    return;
                }
                client *cl = lookupClientByID(cid);
                if (cl) {
                    o = catClientInfoString(o, cl);
                    o = sdscatlen(o, "\n", 1);
                }
            }
        } else if (c->argc != 2) {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }

        if (!o)
            o = getAllClientsInfoString(type);
        addReplyVerbatim(c, o, sdslen(o), "txt");
        sdsfree(o);
    } else if (!strcasecmp(c->argv[1]->ptr, "reply") && c->argc == 3) {
        /* CLIENT REPLY ON|OFF|SKIP */
        if (!strcasecmp(c->argv[2]->ptr, "on")) {
            c->flags &= ~(CLIENT_REPLY_SKIP | CLIENT_REPLY_OFF);
            addReply(c, shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr, "off")) {
            c->flags |= CLIENT_REPLY_OFF;
        } else if (!strcasecmp(c->argv[2]->ptr, "skip")) {
            if (!(c->flags & CLIENT_REPLY_OFF))
                c->flags |= CLIENT_REPLY_SKIP_NEXT;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "kill")) {
        /* CLIENT KILL <ip:port>
         * CLIENT KILL <option> [value] ... <option> [value] */
        char *addr = NULL;
        char *laddr = NULL;
        user *user = NULL;
        int type = -1;
        uint64_t id = 0;
        int skipme = 1;
        int killed = 0, close_this_client = 0;

        if (c->argc == 3) {
            /* Old style syntax: CLIENT KILL <addr> */
            addr = c->argv[2]->ptr;
            skipme = 0; /* With the old form, you can kill yourself. */
        } else if (c->argc > 3) {
            int i = 2; /* Next option index. */

            /* New style syntax: parse options. */
            while (i < c->argc) {
                int moreargs = c->argc > i + 1;

                if (!strcasecmp(c->argv[i]->ptr, "id") && moreargs) {
                    long long tmp;

                    if (getLongLongFromObjectOrReply(c, c->argv[i + 1], &tmp, NULL)
                        != C_OK)
                        return;
                    id = tmp;
                } else if (!strcasecmp(c->argv[i]->ptr, "type") && moreargs) {
                    type = getClientTypeByName(c->argv[i + 1]->ptr);
                    if (type == -1) {
                        addReplyErrorFormat(c, "Unknown client type '%s'",
                                            (char *) c->argv[i + 1]->ptr);
                        return;
                    }
                } else if (!strcasecmp(c->argv[i]->ptr, "addr") && moreargs) {
                    addr = c->argv[i + 1]->ptr;
                } else if (!strcasecmp(c->argv[i]->ptr, "laddr") && moreargs) {
                    laddr = c->argv[i + 1]->ptr;
                } else if (!strcasecmp(c->argv[i]->ptr, "user") && moreargs) {
                    user = ACLGetUserByName(c->argv[i + 1]->ptr,
                                            sdslen(c->argv[i + 1]->ptr));
                    if (user == NULL) {
                        addReplyErrorFormat(c, "No such user '%s'",
                                            (char *) c->argv[i + 1]->ptr);
                        return;
                    }
                } else if (!strcasecmp(c->argv[i]->ptr, "skipme") && moreargs) {
                    if (!strcasecmp(c->argv[i + 1]->ptr, "yes")) {
                        skipme = 1;
                    } else if (!strcasecmp(c->argv[i + 1]->ptr, "no")) {
                        skipme = 0;
                    } else {
                        addReplyErrorObject(c, shared.syntaxerr);
                        return;
                    }
                } else {
                    addReplyErrorObject(c, shared.syntaxerr);
                    return;
                }
                i += 2;
            }
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }

        /* Iterate clients killing all the matching clients. */
        listRewind(server.clients, &li);
        while ((ln = listNext(&li)) != NULL) {
            client *client = listNodeValue(ln);
            if (addr && strcmp(getClientPeerId(client), addr) != 0) continue;
            if (laddr && strcmp(getClientSockname(client), laddr) != 0) continue;
            if (type != -1 && getClientType(client) != type) continue;
            if (id != 0 && client->id != id) continue;
            if (user && client->user != user) continue;
            if (c == client && skipme) continue;

            /* Kill it. */
            if (c == client) {
                close_this_client = 1;
            } else {
                freeClient(client);
            }
            killed++;
        }

        /* Reply according to old/new format. */
        if (c->argc == 3) {
            if (killed == 0)
                addReplyError(c, "No such client");
            else
                addReply(c, shared.ok);
        } else {
            addReplyLongLong(c, killed);
        }

        /* If this client has to be closed, flag it as CLOSE_AFTER_REPLY
         * only after we queued the reply to its output buffers. */
        if (close_this_client) c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    } else if (!strcasecmp(c->argv[1]->ptr, "unblock") && (c->argc == 3 ||
                                                           c->argc == 4)) {
        /* CLIENT UNBLOCK <id> [timeout|error] */
        long long id;
        int unblock_error = 0;

        if (c->argc == 4) {
            if (!strcasecmp(c->argv[3]->ptr, "timeout")) {
                unblock_error = 0;
            } else if (!strcasecmp(c->argv[3]->ptr, "error")) {
                unblock_error = 1;
            } else {
                addReplyError(c,
                              "CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
                return;
            }
        }
        if (getLongLongFromObjectOrReply(c, c->argv[2], &id, NULL)
            != C_OK)
            return;
        struct client *target = lookupClientByID(id);
        if (target && target->flags & CLIENT_BLOCKED) {
            if (unblock_error)
                addReplyError(target,
                              "-UNBLOCKED client unblocked via CLIENT UNBLOCK");
            else
                replyToBlockedClientTimedOut(target);
            unblockClient(target);
            addReply(c, shared.cone);
        } else {
            addReply(c, shared.czero);
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "setname") && c->argc == 3) {
        /* CLIENT SETNAME */
        if (clientSetNameOrReply(c, c->argv[2]) == C_OK)
            addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr, "getname") && c->argc == 2) {
        /* CLIENT GETNAME */
        if (c->name)
            addReplyBulk(c, c->name);
        else
            addReplyNull(c);
    } else if (!strcasecmp(c->argv[1]->ptr, "unpause") && c->argc == 2) {
        /* CLIENT UNPAUSE */
        unpauseClients();
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr, "pause") && (c->argc == 3 ||
                                                         c->argc == 4)) {
        /* CLIENT PAUSE TIMEOUT [WRITE|ALL] */
        mstime_t end;
        int type = CLIENT_PAUSE_ALL;
        if (c->argc == 4) {
            if (!strcasecmp(c->argv[3]->ptr, "write")) {
                type = CLIENT_PAUSE_WRITE;
            } else if (!strcasecmp(c->argv[3]->ptr, "all")) {
                type = CLIENT_PAUSE_ALL;
            } else {
                addReplyError(c,
                              "CLIENT PAUSE mode must be WRITE or ALL");
                return;
            }
        }

        if (getTimeoutFromObjectOrReply(c, c->argv[2], &end,
                                        UNIT_MILLISECONDS) != C_OK)
            return;
        pauseClients(end, type);
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr, "tracking") && c->argc >= 3) {
        /* CLIENT TRACKING (on|off) [REDIRECT <id>] [BCAST] [PREFIX first]
         *                          [PREFIX second] [OPTIN] [OPTOUT] ... */
        long long redir = 0;
        uint64_t options = 0;
        robj **prefix = NULL;
        size_t numprefix = 0;

        /* Parse the options. */
        for (int j = 3; j < c->argc; j++) {
            int moreargs = (c->argc - 1) - j;

            if (!strcasecmp(c->argv[j]->ptr, "redirect") && moreargs) {
                j++;
                if (redir != 0) {
                    addReplyError(c, "A client can only redirect to a single "
                                     "other client");
                    zfree(prefix);
                    return;
                }

                if (getLongLongFromObjectOrReply(c, c->argv[j], &redir, NULL) !=
                    C_OK) {
                    zfree(prefix);
                    return;
                }
                /* We will require the client with the specified ID to exist
                 * right now, even if it is possible that it gets disconnected
                 * later. Still a valid sanity check. */
                if (lookupClientByID(redir) == NULL) {
                    addReplyError(c, "The client ID you want redirect to "
                                     "does not exist");
                    zfree(prefix);
                    return;
                }
            } else if (!strcasecmp(c->argv[j]->ptr, "bcast")) {
                options |= CLIENT_TRACKING_BCAST;
            } else if (!strcasecmp(c->argv[j]->ptr, "optin")) {
                options |= CLIENT_TRACKING_OPTIN;
            } else if (!strcasecmp(c->argv[j]->ptr, "optout")) {
                options |= CLIENT_TRACKING_OPTOUT;
            } else if (!strcasecmp(c->argv[j]->ptr, "noloop")) {
                options |= CLIENT_TRACKING_NOLOOP;
            } else if (!strcasecmp(c->argv[j]->ptr, "prefix") && moreargs) {
                j++;
                prefix = zrealloc(prefix, sizeof(robj *) * (numprefix + 1));
                prefix[numprefix++] = c->argv[j];
            } else {
                zfree(prefix);
                addReplyErrorObject(c, shared.syntaxerr);
                return;
            }
        }

        /* Options are ok: enable or disable the tracking for this client. */
        if (!strcasecmp(c->argv[2]->ptr, "on")) {
            /* Before enabling tracking, make sure options are compatible
             * among each other and with the current state of the client. */
            if (!(options & CLIENT_TRACKING_BCAST) && numprefix) {
                addReplyError(c,
                              "PREFIX option requires BCAST mode to be enabled");
                zfree(prefix);
                return;
            }

            if (c->flags & CLIENT_TRACKING) {
                int oldbcast = !!(c->flags & CLIENT_TRACKING_BCAST);
                int newbcast = !!(options & CLIENT_TRACKING_BCAST);
                if (oldbcast != newbcast) {
                    addReplyError(c,
                                  "You can't switch BCAST mode on/off before disabling "
                                  "tracking for this client, and then re-enabling it with "
                                  "a different mode.");
                    zfree(prefix);
                    return;
                }
            }

            if (options & CLIENT_TRACKING_BCAST &&
                options & (CLIENT_TRACKING_OPTIN | CLIENT_TRACKING_OPTOUT)) {
                addReplyError(c,
                              "OPTIN and OPTOUT are not compatible with BCAST");
                zfree(prefix);
                return;
            }

            if (options & CLIENT_TRACKING_OPTIN && options & CLIENT_TRACKING_OPTOUT) {
                addReplyError(c,
                              "You can't specify both OPTIN mode and OPTOUT mode");
                zfree(prefix);
                return;
            }

            if ((options & CLIENT_TRACKING_OPTIN && c->flags & CLIENT_TRACKING_OPTOUT) ||
                (options & CLIENT_TRACKING_OPTOUT && c->flags & CLIENT_TRACKING_OPTIN)) {
                addReplyError(c,
                              "You can't switch OPTIN/OPTOUT mode before disabling "
                              "tracking for this client, and then re-enabling it with "
                              "a different mode.");
                zfree(prefix);
                return;
            }

            if (options & CLIENT_TRACKING_BCAST) {
                if (!checkPrefixCollisionsOrReply(c, prefix, numprefix)) {
                    zfree(prefix);
                    return;
                }
            }

            enableTracking(c, redir, options, prefix, numprefix);
        } else if (!strcasecmp(c->argv[2]->ptr, "off")) {
            disableTracking(c);
        } else {
            zfree(prefix);
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
        zfree(prefix);
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr, "caching") && c->argc >= 3) {
        if (!(c->flags & CLIENT_TRACKING)) {
            addReplyError(c, "CLIENT CACHING can be called only when the "
                             "client is in tracking mode with OPTIN or "
                             "OPTOUT mode enabled");
            return;
        }

        char *opt = c->argv[2]->ptr;
        if (!strcasecmp(opt, "yes")) {
            if (c->flags & CLIENT_TRACKING_OPTIN) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c, "CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.");
                return;
            }
        } else if (!strcasecmp(opt, "no")) {
            if (c->flags & CLIENT_TRACKING_OPTOUT) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c, "CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.");
                return;
            }
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }

        /* Common reply for when we succeeded. */
        addReply(c, shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr, "getredir") && c->argc == 2) {
        /* CLIENT GETREDIR */
        if (c->flags & CLIENT_TRACKING) {
            addReplyLongLong(c, c->client_tracking_redirection);
        } else {
            addReplyLongLong(c, -1);
        }
    } else if (!strcasecmp(c->argv[1]->ptr, "trackinginfo") && c->argc == 2) {
        addReplyMapLen(c, 3);

        /* Flags */
        addReplyBulkCString(c, "flags");
        void *arraylen_ptr = addReplyDeferredLen(c);
        int numflags = 0;
        addReplyBulkCString(c, c->flags & CLIENT_TRACKING ? "on" : "off");
        numflags++;
        if (c->flags & CLIENT_TRACKING_BCAST) {
            addReplyBulkCString(c, "bcast");
            numflags++;
        }
        if (c->flags & CLIENT_TRACKING_OPTIN) {
            addReplyBulkCString(c, "optin");
            numflags++;
            if (c->flags & CLIENT_TRACKING_CACHING) {
                addReplyBulkCString(c, "caching-yes");
                numflags++;
            }
        }
        if (c->flags & CLIENT_TRACKING_OPTOUT) {
            addReplyBulkCString(c, "optout");
            numflags++;
            if (c->flags & CLIENT_TRACKING_CACHING) {
                addReplyBulkCString(c, "caching-no");
                numflags++;
            }
        }
        if (c->flags & CLIENT_TRACKING_NOLOOP) {
            addReplyBulkCString(c, "noloop");
            numflags++;
        }
        if (c->flags & CLIENT_TRACKING_BROKEN_REDIR) {
            addReplyBulkCString(c, "broken_redirect");
            numflags++;
        }
        setDeferredSetLen(c, arraylen_ptr, numflags);

        /* Redirect */
        addReplyBulkCString(c, "redirect");
        if (c->flags & CLIENT_TRACKING) {
            addReplyLongLong(c, c->client_tracking_redirection);
        } else {
            addReplyLongLong(c, -1);
        }

        /* Prefixes */
        addReplyBulkCString(c, "prefixes");
        if (c->client_tracking_prefixes) {
            addReplyArrayLen(c, raxSize(c->client_tracking_prefixes));
            raxIterator ri;
            raxStart(&ri, c->client_tracking_prefixes);
            raxSeek(&ri, "^", NULL, 0);
            while (raxNext(&ri)) {
                addReplyBulkCBuffer(c, ri.key, ri.key_len);
            }
            raxStop(&ri);
        } else {
            addReplyArrayLen(c, 0);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* HELLO [<protocol-version> [AUTH <user> <password>] [SETNAME <name>] ] */
void helloCommand(client *c) {
    long long ver = 0;
    int next_arg = 1;

    if (c->argc >= 2) {
        if (getLongLongFromObjectOrReply(c, c->argv[next_arg++], &ver,
                                         "Protocol version is not an integer or out of range") != C_OK) {
            return;
        }

        if (ver < 2 || ver > 3) {
            addReplyError(c, "-NOPROTO unsupported protocol version");
            return;
        }
    }

    for (int j = next_arg; j < c->argc; j++) {
        int moreargs = (c->argc - 1) - j;
        const char *opt = c->argv[j]->ptr;
        if (!strcasecmp(opt, "AUTH") && moreargs >= 2) {
            redactClientCommandArgument(c, j + 1);
            redactClientCommandArgument(c, j + 2);
            if (ACLAuthenticateUser(c, c->argv[j + 1], c->argv[j + 2]) == C_ERR) {
                addReplyError(c, "-WRONGPASS invalid username-password pair or user is disabled.");
                return;
            }
            j += 2;
        } else if (!strcasecmp(opt, "SETNAME") && moreargs) {
            if (clientSetNameOrReply(c, c->argv[j + 1]) == C_ERR) return;
            j++;
        } else {
            addReplyErrorFormat(c, "Syntax error in HELLO option '%s'", opt);
            return;
        }
    }

    /* At this point we need to be authenticated to continue. */
    if (!c->authenticated) {
        addReplyError(c, "-NOAUTH HELLO must be called with the client already "
                         "authenticated, otherwise the HELLO AUTH <user> <pass> "
                         "option can be used to authenticate the client and "
                         "select the RESP protocol version at the same time");
        return;
    }

    /* Let's switch to the specified RESP mode. */
    if (ver) c->resp = ver;
    addReplyMapLen(c, 6 + !server.sentinel_mode);

    addReplyBulkCString(c, "server");
    addReplyBulkCString(c, "redis");

    addReplyBulkCString(c, "version");
    addReplyBulkCString(c, REDIS_VERSION);

    addReplyBulkCString(c, "proto");
    addReplyLongLong(c, c->resp);

    addReplyBulkCString(c, "id");
    addReplyLongLong(c, c->id);

    addReplyBulkCString(c, "mode");
    if (server.sentinel_mode) addReplyBulkCString(c, "sentinel");
    else if (server.cluster_enabled) addReplyBulkCString(c, "cluster");
    else addReplyBulkCString(c, "standalone");

    if (!server.sentinel_mode) {
        addReplyBulkCString(c, "role");
        addReplyBulkCString(c, server.masterhost ? "replica" : "master");
    }

    addReplyBulkCString(c, "modules");
    addReplyLoadedModules(c);
}

/* This callback is bound to POST and "Host:" command names. Those are not
 * really commands, but are used in security attacks in order to talk to
 * Redis instances via HTTP, with a technique called "cross protocol scripting"
 * which exploits the fact that services like Redis will discard invalid
 * HTTP headers and will process what follows.
 *
 * As a protection against this attack, Redis will terminate the connection
 * when a POST or "Host:" header is seen, and will log the event from
 * time to time (to avoid creating a DOS as a result of too many logs). */
void securityWarningCommand(client *c) {
    static time_t logged_time;
    time_t now = time(NULL);

    if (llabs(now - logged_time) > 60) {
        serverLog(LL_WARNING,
                  "Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: commands to Redis. This is likely due to an attacker attempting to use Cross Protocol Scripting to compromise your Redis instance. Connection aborted.");
        logged_time = now;
    }
    freeClientAsync(c);
}

/* Keep track of the original command arguments so that we can generate
 * an accurate slowlog entry after the command has been executed. */
static void retainOriginalCommandVector(client *c) {
    /* We already rewrote this command, so don't rewrite it again */
    if (c->original_argv) return;
    c->original_argc = c->argc;
    c->original_argv = zmalloc(sizeof(robj *) * (c->argc));
    for (int j = 0; j < c->argc; j++) {
        c->original_argv[j] = c->argv[j];
        incrRefCount(c->argv[j]);
    }
}

/* Redact a given argument to prevent it from being shown
 * in the slowlog. This information is stored in the
 * original_argv array. */
void redactClientCommandArgument(client *c, int argc) {
    retainOriginalCommandVector(c);
    decrRefCount(c->argv[argc]);
    c->original_argv[argc] = shared.redacted;
}

/* Rewrite the command vector of the client. All the new objects ref count
 * is incremented. The old command vector is freed, and the old objects
 * ref count is decremented. */
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = zmalloc(sizeof(robj *) * argc);
    va_start(ap, argc);
    for (j = 0; j < argc; j++) {
        robj *a;

        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    replaceClientCommandVector(c, argc, argv);
    va_end(ap);
}

/* Completely replace the client command vector with the provided one. */
void replaceClientCommandVector(client *c, int argc, robj **argv) {
    int j;
    retainOriginalCommandVector(c);
    freeClientArgv(c);
    zfree(c->argv);
    c->argv = argv;
    c->argc = argc;
    c->argv_len_sum = 0;
    for (j = 0; j < c->argc; j++)
        if (c->argv[j])
            c->argv_len_sum += getStringObjectLen(c->argv[j]);
    c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
    serverAssertWithInfo(c, NULL, c->cmd != NULL);
}

/* Rewrite a single item in the command vector.
 * The new val ref count is incremented, and the old decremented.
 *
 * It is possible to specify an argument over the current size of the
 * argument vector: in this case the array of objects gets reallocated
 * and c->argc set to the max value. However it's up to the caller to
 *
 * 1. Make sure there are no "holes" and all the arguments are set.
 * 2. If the original argument vector was longer than the one we
 *    want to end with, it's up to the caller to set c->argc and
 *    free the no longer used objects on c->argv. */
void rewriteClientCommandArgument(client *c, int i, robj *newval) {
    robj *oldval;
    retainOriginalCommandVector(c);
    if (i >= c->argc) {
        c->argv = zrealloc(c->argv, sizeof(robj *) * (i + 1));
        c->argc = i + 1;
        c->argv[i] = NULL;
    }
    oldval = c->argv[i];
    if (oldval) c->argv_len_sum -= getStringObjectLen(oldval);
    if (newval) c->argv_len_sum += getStringObjectLen(newval);
    c->argv[i] = newval;
    incrRefCount(newval);
    if (oldval) decrRefCount(oldval);

    /* If this is the command name make sure to fix c->cmd. */
    if (i == 0) {
        c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
        serverAssertWithInfo(c, NULL, c->cmd != NULL);
    }
}

/* This function returns the number of bytes that Redis is
 * using to store the reply still not read by the client.
 *
 * Note: this function is very fast so can be called as many time as
 * the caller wishes. The main usage of this function currently is
 * enforcing the client output length limits. */
unsigned long getClientOutputBufferMemoryUsage(client *c) {
    unsigned long list_item_size = sizeof(listNode) + sizeof(clientReplyBlock);
    return c->reply_bytes + (list_item_size * listLength(c->reply));
}

/* Get the class of a client, used in order to enforce limits to different
 * classes of clients.
 *
 * The function will return one of the following:
 * CLIENT_TYPE_NORMAL -> Normal client
 * CLIENT_TYPE_SLAVE  -> Slave
 * CLIENT_TYPE_PUBSUB -> Client subscribed to Pub/Sub channels
 * CLIENT_TYPE_MASTER -> The client representing our replication master.
 */
int getClientType(client *c) {
    if (c->flags & CLIENT_MASTER) return CLIENT_TYPE_MASTER;
    /* Even though MONITOR clients are marked as replicas, we
     * want the expose them as normal clients. */
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR))
        return CLIENT_TYPE_SLAVE;
    if (c->flags & CLIENT_PUBSUB) return CLIENT_TYPE_PUBSUB;
    return CLIENT_TYPE_NORMAL;
}

int getClientTypeByName(char *name) {
    if (!strcasecmp(name, "normal")) return CLIENT_TYPE_NORMAL;
    else if (!strcasecmp(name, "slave")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name, "replica")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name, "pubsub")) return CLIENT_TYPE_PUBSUB;
    else if (!strcasecmp(name, "master")) return CLIENT_TYPE_MASTER;
    else return -1;
}

char *getClientTypeName(int class) {
    switch (class) {
        case CLIENT_TYPE_NORMAL:
            return "normal";
        case CLIENT_TYPE_SLAVE:
            return "slave";
        case CLIENT_TYPE_PUBSUB:
            return "pubsub";
        case CLIENT_TYPE_MASTER:
            return "master";
        default:
            return NULL;
    }
}

/* The function checks if the client reached output buffer soft or hard
 * limit, and also update the state needed to check the soft limit as
 * a side effect.
 *
 * Return value: non-zero if the client reached the soft or the hard limit.
 *               Otherwise zero is returned. */
int checkClientOutputBufferLimits(client *c) {
    int soft = 0, hard = 0, class;
    unsigned long used_mem = getClientOutputBufferMemoryUsage(c);

    class = getClientType(c);
    /* For the purpose of output buffer limiting, masters are handled
     * like normal clients. */
    if (class == CLIENT_TYPE_MASTER) class = CLIENT_TYPE_NORMAL;

    if (server.client_obuf_limits[class].hard_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].hard_limit_bytes)
        hard = 1;
    if (server.client_obuf_limits[class].soft_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].soft_limit_bytes)
        soft = 1;

    /* We need to check if the soft limit is reached continuously for the
     * specified amount of seconds. */
    if (soft) {
        if (c->obuf_soft_limit_reached_time == 0) {
            c->obuf_soft_limit_reached_time = server.unixtime;
            soft = 0; /* First time we see the soft limit reached */
        } else {
            time_t elapsed = server.unixtime - c->obuf_soft_limit_reached_time;

            if (elapsed <=
                server.client_obuf_limits[class].soft_limit_seconds) {
                soft = 0; /* The client still did not reached the max number of
                             seconds for the soft limit to be considered
                             reached. */
            }
        }
    } else {
        c->obuf_soft_limit_reached_time = 0;
    }
    return soft || hard;
}

/* Asynchronously close a client if soft or hard limit is reached on the
 * output buffer size. The caller can check if the client will be closed
 * checking if the client CLIENT_CLOSE_ASAP flag is set.
 *
 * Note: we need to close the client asynchronously because this function is
 * called from contexts where the client can't be freed safely, i.e. from the
 * lower level functions pushing data inside the client output buffers.
 * When `async` is set to 0, we close the client immediately, this is
 * useful when called from cron.
 *
 * Returns 1 if client was (flagged) closed. */
int closeClientOnOutputBufferLimitReached(client *c, int async) {
    if (!c->conn) return 0; /* It is unsafe to free fake clients. */
    serverAssert(c->reply_bytes < SIZE_MAX - (1024 * 64));
    if (c->reply_bytes == 0 || c->flags & CLIENT_CLOSE_ASAP) return 0;
    if (checkClientOutputBufferLimits(c)) {
        sds client = catClientInfoString(sdsempty(), c);

        if (async) {
            freeClientAsync(c);
            serverLog(LL_WARNING,
                      "Client %s scheduled to be closed ASAP for overcoming of output buffer limits.",
                      client);
        } else {
            freeClient(c);
            serverLog(LL_WARNING,
                      "Client %s closed for overcoming of output buffer limits.",
                      client);
        }
        sdsfree(client);
        return 1;
    }
    return 0;
}

/* Helper function used by performEvictions() in order to flush slaves
 * output buffers without returning control to the event loop.
 * This is also called by SHUTDOWN for a best-effort attempt to send
 * slaves the latest writes. */
void flushSlavesOutputBuffers(void) {
    listIter li;
    listNode *ln;

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = listNodeValue(ln);
        int can_receive_writes = connHasWriteHandler(slave->conn) ||
                                 (slave->flags & CLIENT_PENDING_WRITE);

        /* We don't want to send the pending data to the replica in a few
         * cases:
         *
         * 1. For some reason there is neither the write handler installed
         *    nor the client is flagged as to have pending writes: for some
         *    reason this replica may not be set to receive data. This is
         *    just for the sake of defensive programming.
         *
         * 2. The put_online_on_ack flag is true. To know why we don't want
         *    to send data to the replica in this case, please grep for the
         *    flag for this flag.
         *
         * 3. Obviously if the slave is not ONLINE.
         */
        if (slave->replstate == SLAVE_STATE_ONLINE &&
            can_receive_writes &&
            !slave->repl_put_online_on_ack &&
            clientHasPendingReplies(slave)) {
            writeToClient(slave, 0);
        }
    }
}

/* Pause clients up to the specified unixtime (in ms) for a given type of
 * commands.
 *
 * A main use case of this function is to allow pausing replication traffic
 * so that a failover without data loss to occur. Replicas will continue to receive
 * traffic to faciliate this functionality.
 * 
 * This function is also internally used by Redis Cluster for the manual
 * failover procedure implemented by CLUSTER FAILOVER.
 *
 * The function always succeed, even if there is already a pause in progress.
 * In such a case, the duration is set to the maximum and new end time and the
 * type is set to the more restrictive type of pause. */
void pauseClients(mstime_t end, pause_type type) {
    if (type > server.client_pause_type) {
        server.client_pause_type = type;
    }

    if (end > server.client_pause_end_time) {
        server.client_pause_end_time = end;
    }

    /* We allow write commands that were queued
     * up before and after to execute. We need
     * to track this state so that we don't assert
     * in propagate(). */
    if (server.in_exec) {
        server.client_pause_in_transaction = 1;
    }
}

/* Unpause clients and queue them for reprocessing. */
void unpauseClients(void) {
    listNode *ln;
    listIter li;
    client *c;

    server.client_pause_type = CLIENT_PAUSE_OFF;

    /* Unblock all of the clients so they are reprocessed. */
    listRewind(server.paused_clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        c = listNodeValue(ln);
        unblockClient(c);
    }
}

/* Returns true if clients are paused and false otherwise. */
int areClientsPaused(void) {
    return server.client_pause_type != CLIENT_PAUSE_OFF;
}

/* Checks if the current client pause has elapsed and unpause clients
 * if it has. Also returns true if clients are now paused and false 
 * otherwise. */
int checkClientPauseTimeoutAndReturnIfPaused(void) {
    if (!areClientsPaused())
        return 0;
    if (server.client_pause_end_time < server.mstime) {
        unpauseClients();
    }
    return areClientsPaused();
}

/* This function is called by Redis in order to process a few events from
 * time to time while blocked into some not interruptible operation.
 * This allows to reply to clients with the -LOADING error while loading the
 * data set at startup or after a full resynchronization with the master
 * and so forth.
 *
 * It calls the event loop in order to process a few events. Specifically we
 * try to call the event loop 4 times as long as we receive acknowledge that
 * some event was processed, in order to go forward with the accept, read,
 * write, close sequence needed to serve a client.
 *
 * The function returns the total number of events processed. */
// 让服务器在被阻塞的情况下(Redis 读取 RDB 文件或 AOF 文件时)该函数会被调用，仍然处理某些事件（事件驱动框架捕获到的事件），
// 这样就避免了因读取 RDB 或 AOF 文件造成 Redis 阻塞，而无法及时处理事件的情况。所以，当 processEventsWhileBlokced 函数执行处理客户端可读事件时，这些客户端读操作是不会被推迟执行的。
void processEventsWhileBlocked(void) {
    int iterations = 4; /* See the function top-comment. */

    /* Update our cached time since it is used to create and update the last
     * interaction time with clients and for other important things. */
    updateCachedTime(0);

    /* Note: when we are processing events while blocked (for instance during
     * busy Lua scripts), we set a global flag. When such flag is set, we
     * avoid handling the read part of clients using threaded I/O.
     * See https://github.com/redis/redis/issues/6988 for more info. */

    // 设置阻塞执行标志 1，在执行期间不允许客户端读操作推迟执行
    ProcessingEventsWhileBlocked = 1;
    while (iterations--) {
        long long startval = server.events_processed_while_blocked;
        // 执行事件处理逻辑
        long long ae_events = aeProcessEvents(server.el,
                                              AE_FILE_EVENTS | AE_DONT_WAIT |
                                              AE_CALL_BEFORE_SLEEP | AE_CALL_AFTER_SLEEP);
        /* Note that server.events_processed_while_blocked will also get
         * incremeted by callbacks called by the event loop handlers. */
        server.events_processed_while_blocked += ae_events;
        long long events = server.events_processed_while_blocked - startval;
        if (!events) break;
    }

    whileBlockedCron();

    // 取消阻塞执行标志
    ProcessingEventsWhileBlocked = 0;
}

/** ==========================================================================
 * Threaded I/O
 * 特别说明：
 * 1 线程职责：
 *   1-1：多 IO 线程本身并不会执行命令，它们只是利用多核并行地读取数据和解析命令，或是将 server 数据写回。所以，Redis 执行命令的线程还是主 IO 线程。这一点对于你理解多 IO 线程机制很重要，可以避免你误解 Redis 有多线程同时执行命令
 *   1-2：IO线程虽然不涉及命令执行，但是会协助主线程进行协议编解码，而RESP协议的设计很巧妙，对粘包拆包等处理也是其一大亮点
 * 2 为什么 startThreadedIO / stopThreadedIO 对线程互斥锁数组进行解锁和加锁操作？
 *   2-1：既然涉及到加锁/解锁操作，必然是为了「互斥」从而控制某些逻辑。
 *   2-2：是为了方便主线程动态，灵活调整IO线程而设计的，当待处理的 client 数量较少的时候或者压根就没有待处理的 Client 可以方便直接停止IO线程。
 *   -- 压根就没有待处理的 Client，因为IO线程初始化时是加锁的，这样情况没有解锁，因此会阻塞
 *              for (int j = 0; j < 1000000; j++) {
 *                    if (getIOPendingCount(id) != 0) break;
 *                  }
 *
 *              // 给主线程停止 IO 线程的的机会。也就是说，这里的目的是为了让主线程可以控制 IO 线程的开启 / 暂停
 *              if (getIOPendingCount(id) == 0) {
 *                   pthread_mutex_lock(&io_threads_mutex[id]);
 *                   pthread_mutex_unlock(&io_threads_mutex[id]);
 *                  continue;
 *                }
 *   -- 当待写的client客户端数量小于IO线程数量的两倍,就会停止IO线程避免多线程带来不必要的开销
 *       过程：
 *         1 handleClientsWithPendingWritesUsingThreads -> stopThreadedIOIfNeeded -> stopThreadedIO -> 设置io_threads_active为0 并 lock住IO线程（给每个线程对应的互斥锁做加锁操作）【注意：主线程除外，因为index是从1开始的】
 *         2 当待写的client客户端数量没有上来之前，都是使用主线程来处理
 *       影响：由于给每个 IO 线程对应的互斥锁加锁了，因此后续的 IO 线程就不能对其互斥锁加锁了，会失败阻塞，直到其它线程释放其互斥锁。在循环逻辑中，IO线程就可能会被阻塞。
 *   -- 当待写出client的数量上来的时候，stopThreadedIOIfNeeded函数中判断，待写出client数量大于2倍IO线程数量，返回0，然后调用startThreadedIO激活IO线程
 *       过程： handleClientsWithPendingWritesUsingThreads -> stopThreadedIOIfNeeded(发现需要IO线程，返回0) -> startThreadedIO(激活IO线程) -> 设置io_threads_active为1
 *       影响：释放每个 IO 线程对应的互斥锁，IO线程就可以从阻塞中醒来，继续拉取任务列表中的待处理客户端
 *
 *
 * ========================================================================== */

#define IO_THREADS_MAX_NUM 128
#define IO_THREADS_OP_READ 0
#define IO_THREADS_OP_WRITE 1

// IO 线程数组
pthread_t io_threads[IO_THREADS_MAX_NUM];

// 保存线程互斥锁
// 互斥量
pthread_mutex_t io_threads_mutex[IO_THREADS_MAX_NUM];

// 保存每个 IO 线程待处理的客户端个数
redisAtomic unsigned long io_threads_pending[IO_THREADS_MAX_NUM];

// 操作类型，读或者写
// IO_THREADS_OP_WRITE：表示该 IO 线程要做的是写操作，线程会调用 writeToClient 函数将数据写回客户端
// IO_THREADS_OP_READ：表示该 IO 线程要做的是读操作，线程会调用 readQueryFromClient 函数从客户端读取数据
int io_threads_op;      /* IO_THREADS_OP_WRITE or IO_THREADS_OP_READ. */

/**
 * 线程任务（要处理的客户端）列表，里面是客户端。
 *
 * 针对每个IO线程使用一个列表记录该线程要处理的客户端：
 * io_threads_list[0]: 是主线程的客户端列表
 * io_threads_list[1~IO_THREADS_MAX_NUM] : 是 IO线程的客户端列表
 */
list *io_threads_list[IO_THREADS_MAX_NUM];

static inline unsigned long getIOPendingCount(int i) {
    unsigned long count = 0;
    atomicGetWithSync(io_threads_pending[i], count);
    return count;
}

static inline void setIOPendingCount(int i, unsigned long count) {
    atomicSetWithSync(io_threads_pending[i], count);
}

/**
 * IO 线程任务体
 * 注意：
 * Redis 默认只开启多线程「写」client socket，如果要开启多线程「读」，还需配置 io-threads-do-reads = yes
 *
 * @param myid
 * @return
 */
void *IOThreadMain(void *myid) {
    /* The ID is the thread number (from 0 to server.iothreads_num-1), and is
     * used by the thread to just manipulate a single sub-array of clients.
     *
     * ID是线程号(从 0 到 server.iothreads_num-1 )，线程使用它来操作对应客户端列表
     */
    long id = (unsigned long) myid;
    char thdname[16];

    snprintf(thdname, sizeof(thdname), "io_thd_%ld", id);
    redis_set_thread_title(thdname);
    redisSetCpuAffinity(server.server_cpulist);
    makeThreadKillable();

    // 死循环
    // 每一个 IO 线程运行时都会不断检查是否有等待它处理的客户端，如果有，就根据操作类型，从客户端读取数据或是将数据写回客户。
    // 这些操作都是 Redis 要和客户端完成的 IO 操作，所以，这也是为什么我们把这些线程称为 IO 线程的原因。
    while (1) {
        /* Wait for start
         *
         * 等待被激活。条件是 io_threads_pending[id] 不为 0。
         * 注意：
         *  这里的等待比较特殊，没有使用简单的 sleep，避免了 sleep 时间设置不当可能导致糟糕的性能。
         *  但是也有个问题就是频繁 loop 可能一定程度上造成 cpu 占用较长
         */
        for (int j = 0; j < 1000000; j++) {
            if (getIOPendingCount(id) != 0) break;
        }

        /* Give the main thread a chance to stop this thread. */
        // 给主线程停止 IO 线程的的机会。也就是说，这里的目的是为了让主线程可以控制 IO 线程的开启 / 暂停
        // 怎么控制，通过对 IO 线程的互斥锁加锁和解锁，以控制线程阻塞和醒来
        if (getIOPendingCount(id) == 0) {
            pthread_mutex_lock(&io_threads_mutex[id]);
            pthread_mutex_unlock(&io_threads_mutex[id]);
            continue;
        }

        serverAssert(getIOPendingCount(id) != 0);

        /* Process: note that the main thread will never touch our list
         * before we drop the pending count to 0. */
        listIter li;
        listNode *ln;

        // 获取线程id 对应的任务列表（客户端列表）
        listRewind(io_threads_list[id], &li);

        // 遍历当前线程的任务列表
        while ((ln = listNext(&li))) {

            // 取出任务，其实就是客户端 Socket
            client *c = listNodeValue(ln);

            // 任务类型 - 写
            if (io_threads_op == IO_THREADS_OP_WRITE) {
                // 将输出缓冲区中的数据写回客户端。
                writeToClient(c, 0);

                // 任务类型 - 读
            } else if (io_threads_op == IO_THREADS_OP_READ) {
                // 读取客户端发送的消息
                // 这里需要注意重复调用了 readQueryFromClient，不过不用担心，有 CLIENT_PENDING_READ 标识可以进行识别
                readQueryFromClient(c->conn);

                // 未知类型
            } else {
                serverPanic("io_threads_op value is unknown");
            }
        }

        /* 执行到这里，说明当前 IO 线程处理完了它的任务队列中的任务 ，需要进行清理工作*/

        // 清空 io_threads_list[id] 任务
        listEmpty(io_threads_list[id]);

        // 将 io_threads_pending[id] 设置为 0，表示该线程的待处理任务数量为0
        setIOPendingCount(id, 0);
    }
}

/* Initialize the data structures needed for threaded I/O.
 *
 * 初始化 IO 线程所需要的数据结构，以及创建 IO 线程
 */
void initThreadedIO(void) {
    // 1 将 io 线程激活标志设置为 0 ，表示还没有被激活
    server.io_threads_active = 0; /* We start with threads not active. */

    /* Don't spawn any thread if the user selected a single thread:
     * we'll handle I/O directly from the main thread. */
    // 2 判断设置的 IO 线程数量，有三种结果

    // 2.1 如果用户选择了一个线程，不要生成任何 IO 线程，直接从主线程处理I/O
    if (server.io_threads_num == 1) return;

    // 2.2 io线程数量达到阈值（128）会报错并退出程序
    if (server.io_threads_num > IO_THREADS_MAX_NUM) {
        serverLog(LL_WARNING, "Fatal: too many I/O threads configured. "
                              "The maximum number is %d.", IO_THREADS_MAX_NUM);
        //退出程序
        exit(1);
    }

    // 2.3 如果 IO 线程数量 > 1 && < IO_THREADS_MAX_NUM(128) ，则执行一个循环流程
    /* Spawn and initialize the I/O threads.
     *
     * 派生和初始化IO线程
     */
    for (int i = 0; i < server.io_threads_num; i++) {
        /* Things we do for all the threads including the main thread. 我们为所有线程所做的事情，包括主线程。
         *
         * 2.3.1 创建 io 线程的任务（要处理的客户端）链表。这里包括主线程。约定，第一个任务链表是给主线程的。
         */
        io_threads_list[i] = listCreate();
        if (i == 0) continue; /* Thread 0 is the main thread. */

        /* Things we do only for the additional threads. 只对 io 线程做的事情 */
        pthread_t tid;

        // 2.3.2 初始化io_threads_mutex数组，即给数组中的每个互斥量进行初始化
        // 这里通过调用 pthread_mutex_init 方法初始互斥量
        pthread_mutex_init(&io_threads_mutex[i], NULL);

        // 2.3.3 初始化io_threads_pending数组，即每个 IO 线程处理的客户端个数
        setIOPendingCount(i, 0);

        // 2.3.4 初始化线程互斥锁的数组
        // 给互斥锁数组中每个线程对应的互斥锁做加锁操作，由于每次 IO 线程在执行时必须先拿到锁，才能执行后面的逻辑，
        // 因此，要使用 IO 线程就要先释放 IO线程对应的互斥锁
        pthread_mutex_lock(&io_threads_mutex[i]); /* Thread will be stopped. */

        // 2.3.5 调用 pthread_create 函数创建 io 线程，线程的任务体为 IOThreadMain 函数
        // 注意，i == 0 处理的是主线程的情况，因此不需要为其关联一个任务体
        if (pthread_create(&tid, NULL, IOThreadMain, (void *) (long) i) != 0) {
            serverLog(LL_WARNING, "Fatal: Can't initialize IO thread.");
            exit(1);
        }

        // 将 创建的 io 线程加入到io线程数组中
        io_threads[i] = tid;
    }
}

void killIOThreads(void) {
    int err, j;
    for (j = 0; j < server.io_threads_num; j++) {
        if (io_threads[j] == pthread_self()) continue;
        if (io_threads[j] && pthread_cancel(io_threads[j]) == 0) {
            if ((err = pthread_join(io_threads[j], NULL)) != 0) {
                serverLog(LL_WARNING,
                          "IO thread(tid:%lu) can not be joined: %s",
                          (unsigned long) io_threads[j], strerror(err));
            } else {
                serverLog(LL_WARNING,
                          "IO thread(tid:%lu) terminated", (unsigned long) io_threads[j]);
            }
        }
    }
}

/**
 * 设置 IO 线程激活标志 1
 */
void startThreadedIO(void) {
    serverAssert(server.io_threads_active == 0);
    for (int j = 1; j < server.io_threads_num; j++)
        //给互斥锁数组中每个线程对应的互斥锁做解锁操作
        pthread_mutex_unlock(&io_threads_mutex[j]);

    // 设置 IO 线程激活标识 io_threads_active 为 1
    server.io_threads_active = 1;
}

/**
 * 设置 IO 线程激活标志 0
 */
void stopThreadedIO(void) {
    /* We may have still clients with pending reads when this function
     * is called: handle them before stopping the threads. */
    handleClientsWithPendingReadsUsingThreads();
    serverAssert(server.io_threads_active == 1);
    for (int j = 1; j < server.io_threads_num; j++)
        // 给互斥锁数组中每个线程对应的互斥锁做加锁操作
        pthread_mutex_lock(&io_threads_mutex[j]);
    server.io_threads_active = 0;
}

/* This function checks if there are not enough pending clients to justify
 * taking the I/O threads active: in that case I/O threads are stopped if
 * currently active. We track the pending writes as a measure of clients
 * we need to handle in parallel, however the I/O threading is disabled
 * globally for reads as well if we have too little pending clients.
 *
 * The function returns 0 if the I/O threading should be used because there
 * are enough active threads, otherwise 1 is returned and the I/O threads
 * could be possibly stopped (if already active) as a side effect. */
// 主要是用来判断待写客户端数量，是否不足为 IO 线程数量的 2 倍
int stopThreadedIOIfNeeded(void) {
    int pending = listLength(server.clients_pending_write);

    /* Return ASAP if IO threads are disabled (single threaded mode). */
    if (server.io_threads_num == 1) return 1;

    // 待写客户端数量是否小于 IO 线程数量的 2 倍
    if (pending < (server.io_threads_num * 2)) {
        if (server.io_threads_active) stopThreadedIO();
        return 1;
    } else {
        return 0;
    }
}


/**
 * Redis 在对应的条件下使用 server.clients_pending_write 保存了推迟执行的客户端，表示待写入。而这些客户端分配给多 IO 线程执行就用到了该方法。该函数主要负责将 clients_pending_write 列表中的客户端分配给 IO 线程进行处理。
 *
 * 遍历每一个待写回数据的客户端，然后调用 writeToClient 函数，将客户端输出缓冲区中的数据写回。
 * 注意：处理器处理完客户端的请求后，会先把响应数据写到对应的 Client 的内存 buffer 中，在下一次处理IO事件之前，redis 会把每个 Client 的 buffer 数据写到 Client 的 Socket 中响应过去
 *
 * @see beforeSleep
 */
int handleClientsWithPendingWritesUsingThreads(void) {

    // 1 全局客户端等待写队列是否为空，如果为空说明没有要写回客户端的数据
    int processed = listLength(server.clients_pending_write);
    if (processed == 0) return 0; /* Return ASAP if there are no clients. */

    /* If I/O threads are disabled or we have few clients to serve, don't
     * use I/O threads, but the boring synchronous code.
     *
     * 2 如果I/O线程被禁用或者我们有很少的客户端要服务（待写客户端数量是否小于 IO 线程数量的 2 倍），不要使用I/O线程，而是由主 IO 线程直接处理待写客户端。
     */
    if (server.io_threads_num == 1 || stopThreadedIOIfNeeded()) {
        // 由主 IO 线程直接处理待写客户端
        // 这样做的目的，主要是为了在待写客户端数量不多时，避免采用多线程，从而节省 CPU 开销
        return handleClientsWithPendingWrites();
    }

    /* Start threads if needed. */
    // 3 判断 IO 线程是否已激活，如果没有激活，它就会调用 startThreadedIO 函数，把全局变量 server 的 io_threads_active 成员变量值设置为 1，表示 IO 线程已激活
    // todo 注意 给互斥锁数组中每个线程对应的互斥锁做解锁操作
    if (!server.io_threads_active) startThreadedIO();

    /* Distribute the clients across N different lists. */
    listIter li;
    listNode *ln;

    // 4 获取全局客户端等待写队列
    listRewind(server.clients_pending_write, &li);

    // 遍历客户端等待写队列
    int item_id = 0;
    while ((ln = listNext(&li))) {

        // 4.1 取出等待的客户端
        client *c = listNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;

        /* Remove clients from the list of pending writes since
         * they are going to be closed ASAP. */
        if (c->flags & CLIENT_CLOSE_ASAP) {
            listDelNode(server.clients_pending_write, ln);
            continue;
        }

        // 4.2 将写任务（待写客户端）按照「轮询方式」分配给 IO 线程的任务链表 io_threads_list 中
        // 这样的话，IO 线程会自动处理写任务，因为它们一旦开启就不断轮询任务
        int target_id = item_id % server.io_threads_num;
        listAddNodeTail(io_threads_list[target_id], c);
        item_id++;
    }

    /* Give the start condition to the waiting threads, by setting the
     * start condition atomic var.
     *
     * 5 通过设置启动条件原子变量，为等待的 IO 线程提供启动条件。
     */
    io_threads_op = IO_THREADS_OP_WRITE;

    // 6 将 io_threads_pending[j] 设置为对应的任务数，此时 IO 线程将从循环中被激活，开始执行任务
    // 执行完毕后，会将 io_threads_pending[j] 清零
    for (int j = 1; j < server.io_threads_num; j++) {
        int count = listLength(io_threads_list[j]);
        setIOPendingCount(j, count);
    }

    /* Also use the main thread to process a slice of clients.
     *
     * 7 用主线程来处理客户端切片，不能让它主线程一直干等着 IO 线程
     * 主线程处理的任务队列是 io_threads_list[0]
     */
    listRewind(io_threads_list[0], &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);

        // 将当前客户端输出缓冲区中的数据写回
        writeToClient(c, 0);
    }
    // 清理
    listEmpty(io_threads_list[0]);

    /* Wait for all the other threads to end their work.
     *
     *  8 等待所有其他 IO 线程结束它们的工作。
     */
    while (1) {
        unsigned long pending = 0;
        for (int j = 1; j < server.io_threads_num; j++)
            pending += getIOPendingCount(j);
        if (pending == 0) break;
    }

    /* Run the list of clients again to install the write handler where
     * needed.
     */
    // 9 再次检查 clients_pending_write 列表中，是否还有待写的客户端
    listRewind(server.clients_pending_write, &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);

        /* Install the write handler if there are pending writes in some
         * of the clients.
         *
         * 如果某些客户端中有挂起的写操作，则关联命令回复处理器
         */
        // 如果有的话，并且这些客户端还有留存在缓冲区中的数据，那么，handleClientsWithPendingWritesUsingThreads 函数就会调用 connSetWriteHandler 函数注册可写事件，而这个可写事件对应的回调函数是 sendReplyToClient 函数。
        // 等到事件循环流程再次执行时，刚才 handleClientsWithPendingWritesUsingThreads 函数注册的可写事件就会被处理，紧接着 sendReplyToClient 函数会执行，它会直接调用 writeToClient 函数，把客户端缓冲区中的数据写回。
        if (clientHasPendingReplies(c) &&
            // 如果缓冲区数据还没有写完，此时关联命令回复处理器 sendReplyToClient ，当写事件发生时会触发 sendReplyToClient 函数的回调
            // 它里面会调用 writeToClient 函数写回数据
            connSetWriteHandler(c->conn, sendReplyToClient) == AE_ERR) {
            freeClientAsync(c);
        }
    }

    // 清空写等待队列
    listEmpty(server.clients_pending_write);

    /* Update processed count on server */
    server.stat_io_writes_processed += processed;

    return processed;
}

/* Return 1 if we want to handle the client read later using threaded I/O.
 * This is called by the readable handler of the event loop.
 * As a side effect of calling this function the client is put in the
 * pending read clients and flagged as such.
 *
 * 返回 1 表示以后使用 IO线程 处理客户端读。
 *
 * 将客户端 c 加入到客户端等待读链表中
 *
 * 判断能否推迟从客户端读取数据，条件判断：
 * 1 server 全局变量 io_threads_active 值为 1
 *   表示多 IO 线程已经激活，这个变量值在 initThreadedIO 函数中是会被初始化为 0 的，也就是说，多 IO 线程初始化后，默认还没有激活
 * 2 server 全局变量 io_threads_do_reads 值为 1
 *   IO 线程可以用于处理延后执行的客户端读操作，这个变量值是在 Redis 配置文件 redis.conf 中，通过配置项 io-threads-do-reads 设置的，
 *   默认值为 no，也就是说，多 IO 线程机制默认并不会用于客户端读操作。所以，如果你想用多 IO 线程处理客户端读操作，就需要把 io-threads-do-reads 配置项设为 yes。
 * 3 ProcessingEventsWhileBlocked 变量值为 0
 *   这表示 processEventsWhileBlokced 函数没有在执行。ProcessingEventsWhileBlocked 是一个全局变量，它会在 processEventsWhileBlokced 函数执行时被设置为 1，
 *   在 processEventsWhileBlokced 函数执行完成时被设置为 0
 * 4 客户端现有标识不能有 CLIENT_MASTER、CLIENT_SLAVE 和 CLIENT_PENDING_READ
 *   CLIENT_MASTER 和 CLIENT_SLAVE 标识分别表示客户端是用于主从复制的客户端，也就是说，这些客户端不会推迟读操作。CLIENT_PENDING_READ 本身就表示一个客户端已经被设置为推迟读操作了，
 *   所以，对于已带有 CLIENT_PENDING_READ 标识的客户端，postponeClientRead 函数就不会再推迟它的读操作了
 * 只有上面四个条件都满足了，postponeClientRead 函数才会推迟当前客户端的读操作
 */
int postponeClientRead(client *c) {
    // 如果启用多线程模型，并且判断全局配置中是否支持多线程读
    if (server.io_threads_active &&
        server.io_threads_do_reads &&
        !ProcessingEventsWhileBlocked &&
        // 这里有个点需要注意，如果是 master-slave 同步也有可能被认为是普通 读任务，所以需要标识
        !(c->flags & (CLIENT_MASTER | CLIENT_SLAVE | CLIENT_PENDING_READ))) {

        // 将该客户端的状态标志设置 CLIENT_PENDING_REA ，表示推迟读操作
        c->flags |= CLIENT_PENDING_READ;

        // 将该客户端添加到 server 全局变量的 clients_pending_read 列表中
        listAddNodeHead(server.clients_pending_read, c);
        return 1;
    } else {
        return 0;
    }
}

/** When threaded I/O is also enabled for the reading + parsing side, the
 * readable handler will just put normal clients into a queue of clients to
 * process (instead of serving them synchronously). This function runs
 * the queue using the I/O threads, and process them in order to accumulate
 * the reads in the buffers, and also parse the first command available
 * rendering it in the client structures.
 *
 * Redis 在对应的条件下使用 server.clients_pending_read 保存了推迟执行的客户端，表示待读取。而这些客户端分配给多 IO 线程执行就用到了该方法。
 *
 * 该函数主要负责将 server.clients_pending_read 列表中的客户端分配给 IO 线程进行处理。
 *
 * @see beforeSleep
 */
int handleClientsWithPendingReadsUsingThreads(void) {
    // 是否开启了 IO 多线程，没有开启直接返回
    // 是否开启了允许使用 IO 线程处理待读客户端
    // 1 只有在 IO 线程激活，并且 IO 线程可以用于处理待读客户端时，才会继续执行，否则结束返回。
    if (!server.io_threads_active || !server.io_threads_do_reads) return 0;

    // 2 获取客户端等待读链表中元素数目，没有等等待的则直接返回
    int processed = listLength(server.clients_pending_read);
    if (processed == 0) return 0;

    /* Distribute the clients across N different lists. */
    listIter li;
    listNode *ln;

    // 2.1 获取全局客户端等待读队列，并尝试轮询方式逐一分配给各个 IO 线程
    // 将待处理任务(客户端)进行分配，轮询方式逐一分配给各个 IO 线程， 即基于任务到达时间片进行分配（客户端在列表中的序号先后顺序）
    listRewind(server.clients_pending_read, &li);

    // 遍历客户端等待读队列,并用客户端在列表中的序号，对 IO 线程数量进行取模运算。这样可以根据取模得到的余数，把该客户端分配给对应的 IO 线程进行处理。
    // 即 把待处理客户端，以「轮询方式」逐一分配给各个 IO 线程
    int item_id = 0;
    while ((ln = listNext(&li))) {
        // 取出等待的客户端
        client *c = listNodeValue(ln);

        // 将读任务均匀分发到各个 IO 线程的任务链表 io_threads_list 中
        int target_id = item_id % server.io_threads_num;
        // 把该客户端分配给对应的 IO 线程进行处理
        listAddNodeTail(io_threads_list[target_id], c);
        item_id++;
    }

    /* Give the start condition to the waiting threads, by setting the
     * start condition atomic var.
     *
     * 通过设置启动条件原子变量，为等待的 IO 线程提供启动条件。
     */
    // 3 将 IO 线程的操作标识设置为读操作，也就是 IO_THREADS_OP_READ
    io_threads_op = IO_THREADS_OP_READ;

    // 4 将 io_threads_pending[j] 设置为对应的任务数，此时 IO 线程将从循环中被激活，开始执行任务
    // 执行完毕后，会将 io_threads_pending[j] 清零
    for (int j = 1; j < server.io_threads_num; j++) {
        // 获取当前 io 线程的任务数
        int count = listLength(io_threads_list[j]);
        // 设置 IO 线程待处理的客户端的个数，从而激活 IO 线程
        setIOPendingCount(j, count);
    }

    /* Also use the main thread to process a slice of clients.
     *
     * 用主线程来处理客户端切片，不能让主线程一直干等着 IO 线程。
     * 主线程处理的任务队列是 io_threads_list[0]
     *
     * io_threads_list 数组对应的 0 号线程正是 IO 主线程，所以，这里就是让主 IO 线程来处理它的待读客户端。
     */
    // 5 获取0号列表中的所有客户端
    listRewind(io_threads_list[0], &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);
        // 将 io_threads_list 数组 0 号列表中的待读客户端逐一取出来，并调用 readQueryFromClient 函数进行处理，这是主线程执行的。
        readQueryFromClient(c->conn);
    }
    //处理完后，清空0号列表
    listEmpty(io_threads_list[0]);

    /* Wait for all the other threads to end their work.
     *
     * 等待所有其他 IO 线程结束它们的工作。
     */
    // 6 执行一个 while(1) 循环，等待所有 IO 线程完成待读客户端的处理
    // 注意：IO 线程在解析命令时只会解析第一个，其它的还需要主线程去解析
    while (1) {
        unsigned long pending = 0;
        for (int j = 1; j < server.io_threads_num; j++)
            pending += getIOPendingCount(j);
        if (pending == 0) break;
    }

    /* Run the list of clients again to process the new buffers.
     *
     * 主线程处理解析后的命令
     *
     */
    // 7 待读客户端处理完成后，再次遍历全局等待读队列 clients_pending_read
    while (listLength(server.clients_pending_read)) {

        // 依次取出全局等待队列 clients_pending_read 中的元素，也就是客户端
        ln = listFirst(server.clients_pending_read);
        client *c = listNodeValue(ln);

        // 7.1 设置客户端的状态标志
        c->flags &= ~CLIENT_PENDING_READ;

        // 7.2 渐进清空读等待队列
        listDelNode(server.clients_pending_read, ln);

        // 7.3 该客户端中的命令是否已经被某一个线程解析过（主线程也会解析），如果解析过就可以被执行了
        // IO 线程只会解析某个客户端缓冲中的一个命令，剩余的需要主线程解析
        if (processPendingCommandsAndResetClient(c) == C_ERR) {
            /* If the client is no longer valid, we avoid
             * processing the client later. So we just go
             * to the next. */
            continue;
        }

        // 7.4 解析客户端剩余命令（解析输入缓冲区）
        processInputBuffer(c);

        /* We may have pending replies if a thread readQueryFromClient() produced
         * replies and did not install a write handler (it can't).
         *
         * 如果线程 readQueryFromClient() 产生了应答并且没有关联写处理程序，那么需要交给 IO 线程处理
         */
        if (!(c->flags & CLIENT_PENDING_WRITE) && clientHasPendingReplies(c))
            clientInstallWriteHandler(c);
    }

    /* Update processed count on server */
    server.stat_io_reads_processed += processed;

    return processed;
}
