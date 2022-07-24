/* Asynchronous replication implementation.
 *
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
#include "cluster.h"
#include "bio.h"

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

void replicationDiscardCachedMaster(void);

void replicationResurrectCachedMaster(connection *conn);

void replicationSendAck(void);

void putSlaveOnline(client *slave);

int cancelReplicationHandshake(int reconnect);

/* We take a global flag to remember if this instance generated an RDB
 * because of replication, so that we can remove the RDB file in case
 * the instance is configured to have no persistence. */
int RDBGeneratedByReplication = 0;

/* --------------------------- Utility functions ---------------------------- */

/* Return the pointer to a string representing the slave ip:listening_port
 * pair. Mostly useful for logging, since we want to log a slave using its
 * IP address and its listening port which is more clear for the user, for
 * example: "Closing connection with replica 10.1.2.3:6380". */
char *replicationGetSlaveName(client *c) {
    static char buf[NET_HOST_PORT_STR_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';
    if (c->slave_addr ||
        connPeerToString(c->conn, ip, sizeof(ip), NULL) != -1) {
        char *addr = c->slave_addr ? c->slave_addr : ip;
        if (c->slave_listening_port)
            anetFormatAddr(buf, sizeof(buf), addr, c->slave_listening_port);
        else
            snprintf(buf, sizeof(buf), "%s:<unknown-replica-port>", addr);
    } else {
        snprintf(buf, sizeof(buf), "client id #%llu",
                 (unsigned long long) c->id);
    }
    return buf;
}

/* Plain unlink() can block for quite some time in order to actually apply
 * the file deletion to the filesystem. This call removes the file in a
 * background thread instead. We actually just do close() in the thread,
 * by using the fact that if there is another instance of the same file open,
 * the foreground unlink() will only remove the fs name, and deleting the
 * file's storage space will only happen once the last reference is lost. */
int bg_unlink(const char *filename) {
    int fd = open(filename, O_RDONLY | O_NONBLOCK);
    if (fd == -1) {
        /* Can't open the file? Fall back to unlinking in the main thread. */
        return unlink(filename);
    } else {
        /* The following unlink() removes the name but doesn't free the
         * file contents because a process still has it open. */
        int retval = unlink(filename);
        if (retval == -1) {
            /* If we got an unlink error, we just return it, closing the
             * new reference we have to the file. */
            int old_errno = errno;
            close(fd);  /* This would overwrite our errno. So we saved it. */
            errno = old_errno;
            return -1;
        }
        bioCreateCloseJob(fd);
        return 0; /* Success. */
    }
}

/* ---------------------------------- MASTER -------------------------------- */

/*说明：循环缓冲区是在主从节点复制过程中使用的*/

/*
 * 创建循环缓冲区
 *
 * 从配置文件中读取循环缓冲区配置项 repl-backlog-size 的大小，然后给循环缓冲区分配指定大小空间
 *
 * syncCommand 函数中调用该函数创建循环缓冲区，创建条件：
 * - 当前 Server 还没有循环缓冲区
 * - 从节点至少要有 1 个，当一个主节点有多个从节点时，这些从节点其实会共享使用一个循环缓冲区，而这样设计的目的，主要是避免给每个从节点开辟一块缓冲区，造成内存资源浪费。
 */
void createReplicationBacklog(void) {
    serverAssert(server.repl_backlog == NULL);
    // 1 根据指定的大小，分配循环缓冲区空间
    server.repl_backlog = zmalloc(server.repl_backlog_size);

    // 2 设置循环缓冲区当前真实的数据大小为 0 ，表示当前没有数据写入
    server.repl_backlog_histlen = 0;

    // 3 设置写指针为 0 ，表示当前可以从缓冲区头开始写入数据
    server.repl_backlog_idx = 0;

    // 4 把 repl_backlog_off，设置为 master_repl_offset 加 1 的值
    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream.
     *
     * 我们的缓冲区中没有任何数据，但实际上我们拥有的第一个字节就是将为复制流生成的下一个字节
     **/
    server.repl_backlog_off = server.master_repl_offset + 1;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to both update the
 * server.repl_backlog_size and to resize the buffer and setup it so that
 * it contains the same data as the previous one (possibly less data, but
 * the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
void resizeReplicationBacklog(long long newsize) {
    if (newsize < CONFIG_REPL_BACKLOG_MIN_SIZE)
        newsize = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog_size == newsize) return;

    server.repl_backlog_size = newsize;
    if (server.repl_backlog != NULL) {
        /* What we actually do is to flush the old buffer and realloc a new
         * empty one. It will refill with new data incrementally.
         * The reason is that copying a few gigabytes adds latency and even
         * worse often we need to alloc additional space before freeing the
         * old buffer. */
        zfree(server.repl_backlog);
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        /* Next byte we have is... the next since the buffer is empty. */
        server.repl_backlog_off = server.master_repl_offset + 1;
    }
}

void freeReplicationBacklog(void) {
    serverAssert(listLength(server.slaves) == 0);
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/* Add data to the replication backlog.
 *
 * 写入数据到循环缓冲区
 *
 * This function also increments the global replication offset stored at
 * server.master_repl_offset, because there is no case where we want to feed
 * the backlog without incrementing the offset.
 *
 * 此函数还增加存储在服务器上的全局复制偏移量 master_repl_offset
 */
/**
 * 循环缓冲区的写操作
 *
 * @param ptr 指向了要写入缓冲区的数据
 * @param len 要写的数据长度
 */
void feedReplicationBacklog(void *ptr, size_t len) {
    unsigned char *p = ptr;

    // 1 更新全局变量 master_repl_offset ，即在当前值的基础上加上要写入的数据长度 len
    server.master_repl_offset += len;


    /**
     * 2 根据要写的数据长度 len 执行一个循环流程，这个流程会循环执行，直到把要写入的数据都写进循环缓冲区。
     *
     * - 如果实际写入长度小于缓冲区剩余长度，那么就按照实际写入长度写数据。否则，就按照剩余空间长度写入数据。
     * - 可以看出，该函数每一轮都会尽量多写数据，不过每轮循环最多写入的数据长度是缓冲区的总长度。
     */
    /* This is a circular buffer, so write as much data we can at every
     * iteration and rewind the "idx" index if we reach the limit. */
    while (len) {

        // 2.1 计算本次能写入的数据长度（循环缓冲区当前的剩余空间长度），循环缓冲区大小 - 当前写指针
        // 如果剩余空间长度大于要写入数据的长度，则把 thislen 设置为实际要写入的数据长度。尽可能每一轮都会尽量多些数据
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;
        if (thislen > len) thislen = len;

        // 2.2 实际写入数据，即根据 thislen 的值，调用 memcpy 函数，将要写入的数据写到循环缓冲区中，
        // 写入的位置： repl_backlog_idx 指向的位置
        // 写入的长度： thislen
        memcpy(server.repl_backlog + server.repl_backlog_idx, p, thislen);

        // 2.3 更新描述缓冲区状态的变量

        // 2.3.1 更新写指针，因为缓冲区总长度 repl_backlog_size 的大小固定，所以，如果 repl_backlog_idx 的值等于 repl_backlog_size 的值了，
        // 那么 repl_backlog_idx 的值会被置为 0，表明此时循环缓冲区已经写满了。此时，写指针会指向循环缓冲区的头部，从头开始再次写入。
        server.repl_backlog_idx += thislen;
        if (server.repl_backlog_idx == server.repl_backlog_size)
            server.repl_backlog_idx = 0;

        // 2.3.2 更新剩余要写入的数据长度，控制循环次数
        len -= thislen;

        // 2.3.3 更新要写入循环缓冲区的数据指针位置，因为写入数据是从数据指针指向的数据获取
        p += thislen;

        // 2.3.4 在每轮循环的最后，都会加上刚刚写入的数据长度 thislen，用于表示循环缓冲区当前真实存储的数据长度
        server.repl_backlog_histlen += thislen;
    }


    // 3 todo 检查 记录循环缓冲区真实数据长度的 repl_backlog_histlen 是否大于循环缓冲区大小。
    // 如果大于，则将 repl_backlog_histlen 的值设置为循环缓冲区总长度。
    // 这也就是说，一旦循环缓冲区被写满后，repl_backlog_histlen 的值就会维持在循环缓冲区的总长度。
    if (server.repl_backlog_histlen > server.repl_backlog_size)
        server.repl_backlog_histlen = server.repl_backlog_size;

    // 4 更新当前循环缓冲区中第一个字节在全局复制偏移量中的偏移值。
    // 注意，+1 是必须的，因为表示的是循环缓冲区第一个字节在全局范围内的偏移量，最小为 1
    /* Set the offset of the first byte we have in the backlog. */
    server.repl_backlog_off = server.master_repl_offset -
                              server.repl_backlog_histlen + 1;
}

/* Wrapper for feedReplicationBacklog() that takes Redis string objects
 * as input. */
void feedReplicationBacklogWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr, sizeof(llstr), (long) o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBacklog(p, len);
}

int canFeedReplicaReplBuffer(client *replica) {
    /* Don't feed replicas that only want the RDB. */
    if (replica->flags & CLIENT_REPL_RDBONLY) return 0;

    /* Don't feed replicas that are still waiting for BGSAVE to start. */
    if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START) return 0;

    return 1;
}

/* Propagate write commands to slaves, and populate the replication backlog
 * as well. This function is used if the instance is a master: we use
 * the commands received by our clients in order to create the replication
 * stream. Instead if the instance is a slave and has sub-slaves attached,
 * we use replicationFeedSlavesFromMasterStream()
 *
 * 将写命令填充到复制积压缓冲区，然后传播到从节点，
 *
 * 如果实例是从属节点，并且附加了子从属节点，我们使用 replicationFeedSlavesFromMasterStream()
 *
 * 具体操作分为三个过程：
 * 1. 构建协议内容数据
 * 2. todo 将协议内容数据备份到 复制积压缓冲区
 * 3. todo 将协议内容数据发送给各个从服务器
 */
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j, len;
    char llstr[LONG_STR_SIZE];

    /* If the instance is not a top level master, return ASAP: we'll just proxy
     * the stream of data we receive from our master instead, in order to
     * propagate *identical* replication stream. In this way this slave can
     * advertise the same replication ID as the master (since it shares the
     * master replication history and has the same backlog and offsets). */
    // 1 非主节点，直接返回
    if (server.masterhost != NULL) return;

    /* If there aren't slaves, and there is no backlog buffer to populate,
     * we can return ASAP. */
    // 2 backlog 为空，且没有从服务器，直接返回
    if (server.repl_backlog == NULL && listLength(slaves) == 0) return;

    /* We can't have slaves attached and no backlog. */
    serverAssert(!(listLength(slaves) != 0 && server.repl_backlog == NULL));

    /* Send SELECT command to every slave if needed. */
    // 3 如果有需要的话，发送 SELECT 命令指定数据库
    if (server.slaveseldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr, sizeof(llstr), dictid);
            selectcmd = createObject(OBJ_STRING,
                                     sdscatprintf(sdsempty(),
                                                  "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                                                  dictid_len, llstr));
        }

        /* Add the SELECT command into the backlog. */
        // 将 SELECT 命令添加到 backlog
        if (server.repl_backlog) feedReplicationBacklogWithObject(selectcmd);

        /* Send it to slaves. */
        // 发送给所有从节点
        listRewind(slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;

            if (!canFeedReplicaReplBuffer(slave)) continue;
            addReply(slave, selectcmd);
        }

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    server.slaveseldb = dictid;

    /* Write the command to the replication backlog if any. */
    // 4 将命令（RESP 协议内容）写入到 backlog
    // todo 先写入 backlog 再发送到每个从节点
    if (server.repl_backlog) {
        char aux[LONG_STR_SIZE + 3];

        /* Add the multi bulk reply length. */
        aux[0] = '*';
        len = ll2string(aux + 1, sizeof(aux) - 1, argc);
        aux[len + 1] = '\r';
        aux[len + 2] = '\n';
        feedReplicationBacklog(aux, len + 3);

        for (j = 0; j < argc; j++) {
            long objlen = stringObjectLen(argv[j]);

            /* We need to feed the buffer with the object as a bulk reply
             * not just as a plain string, so create the $..CRLF payload len
             * and add the final CRLF */
            // 将参数从对象转换成协议格式
            aux[0] = '$';
            len = ll2string(aux + 1, sizeof(aux) - 1, objlen);
            aux[len + 1] = '\r';
            aux[len + 2] = '\n';
            feedReplicationBacklog(aux, len + 3);
            feedReplicationBacklogWithObject(argv[j]);
            feedReplicationBacklog(aux + len + 1, 2);
        }
    }

    /* Write the command to every slave. */
    // 5 将命令写入每个从节点（从客户端缓冲区-复制缓冲区）
    listRewind(slaves, &li);
    while ((ln = listNext(&li))) {
        // 指向从服务器
        client *slave = ln->value;

        // 不要给正在等待 BGSAVE 开始的从服务器发送命令，因为这种接下来是全量复制
        if (!canFeedReplicaReplBuffer(slave)) continue;

        /* Feed slaves that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the initial SYNC completes),
         * or are already in sync with the master.
         *
         * 向已经接收完和正在接收 RDB 文件的从服务器发送命令，如果从服务器正在接收主服务器发送的 RDB 文件，那么在初次 SYNC 完成之前，
         * 主服务器发送的内容会被放进一个缓冲区里面
         */

        /** todo 将命令写入到从节点客户端缓冲区，即复制缓冲区; 后续等待 IO 线程或者主线程将缓冲区数据通过网卡写回到从节点*/
        /* Add the multi bulk length. */
        addReplyArrayLen(slave, argc);

        /* Finally any additional argument that was not stored inside the
         * static buffer if any (from j to argc). */
        for (j = 0; j < argc; j++)
            addReplyBulk(slave, argv[j]);
    }
}

/* This is a debugging function that gets called when we detect something
 * wrong with the replication protocol: the goal is to peek into the
 * replication backlog and show a few final bytes to make simpler to
 * guess what kind of bug it could be. */
void showLatestBacklog(void) {
    if (server.repl_backlog == NULL) return;

    long long dumplen = 256;
    if (server.repl_backlog_histlen < dumplen)
        dumplen = server.repl_backlog_histlen;

    /* Identify the first byte to dump. */
    long long idx =
            (server.repl_backlog_idx + (server.repl_backlog_size - dumplen)) %
            server.repl_backlog_size;

    /* Scan the circular buffer to collect 'dumplen' bytes. */
    sds dump = sdsempty();
    while (dumplen) {
        long long thislen =
                ((server.repl_backlog_size - idx) < dumplen) ?
                (server.repl_backlog_size - idx) : dumplen;

        dump = sdscatrepr(dump, server.repl_backlog + idx, thislen);
        dumplen -= thislen;
        idx = 0;
    }

    /* Finally log such bytes: this is vital debugging info to
     * understand what happened. */
    serverLog(LL_WARNING, "Latest backlog is: '%s'", dump);
    sdsfree(dump);
}

/* This function is used in order to proxy what we receive from our master
 * to our sub-slaves. */
#include <ctype.h>

/*
 * 实例是从属节点，并且附加了子从属节点
 */
void replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen) {
    listNode *ln;
    listIter li;

    /* Debugging: this is handy to see the stream sent from master
     * to slaves. Disabled with if(0). */
    if (0) {
        printf("%zu:", buflen);
        for (size_t j = 0; j < buflen; j++) {
            printf("%c", isprint(buf[j]) ? buf[j] : '.');
        }
        printf("\n");
    }

    if (server.repl_backlog) feedReplicationBacklog(buf, buflen);
    listRewind(slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        if (!canFeedReplicaReplBuffer(slave)) continue;
        addReplyProto(slave, buf, buflen);
    }
}

void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    if (!(listLength(server.monitors) && !server.loading)) return;
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    cmdrepr = sdscatprintf(cmdrepr, "%ld.%06ld ", (long) tv.tv_sec, (long) tv.tv_usec);
    if (c->flags & CLIENT_LUA) {
        cmdrepr = sdscatprintf(cmdrepr, "[%d lua] ", dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr, "[%d unix:%s] ", dictid, server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr, "[%d %s] ", dictid, getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long) argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr, (char *) argv[j]->ptr,
                                 sdslen(argv[j]->ptr));
        }
        if (j != argc - 1)
            cmdrepr = sdscatlen(cmdrepr, " ", 1);
    }
    cmdrepr = sdscatlen(cmdrepr, "\r\n", 2);
    cmdobj = createObject(OBJ_STRING, cmdrepr);

    listRewind(monitors, &li);
    while ((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor, cmdobj);
    }
    decrRefCount(cmdobj);
}

/* Feed the slave 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
/**
 * 读取循环缓冲区中的数据
 *
 * @param c 从节点连接主节点的客户端
 * @param offset 从节点发送的全局读取位置
 * @return
 */
long long addReplyReplicationBacklog(client *c, long long offset) {
    long long j, skip, len;

    serverLog(LL_DEBUG, "[PSYNC] Replica request offset: %lld", offset);

    // 1 判断当前主节点的循环缓冲区是否为空，为空直接返回
    if (server.repl_backlog_histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld",
              server.repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld",
              server.repl_backlog_off);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld",
              server.repl_backlog_histlen);
    serverLog(LL_DEBUG, "[PSYNC] Current index: %lld",
              server.repl_backlog_idx);

    /**
     * 2 计算从节点读数据时要跳过的数据长度 skip 。计算方法：从节点发送的全局读取位置 offset - repl_backlog_off
     *
     * repl_backlog_off 表示仍然在循环缓冲区中的最早保存的数据（第一个没有还没有被覆盖的数据）的首字节在全局氛围内的偏移量。而从节点的全局读取位置和 repl_backlog_off 不一定一致，
     * 所以两者相减，就是从节点要跳过的数据长度。
     * todo 注意为负数的情况。*/
    /* Compute the amount of bytes we need to discard. */
    skip = offset - server.repl_backlog_off;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);


    /**
     * 3 计算缓冲区中当前保存的最早数据的首字节在缓冲区中的位置（注意，是基于缓冲区的位置，不是针对在全局范围的位置）。
     *   有了该位置，从节点才能把全局读取位置转换到缓冲区中的读取位置。下面的代码不好理解，分两种情况进行说明：
     *
     * - 缓冲区还没有写满：此时，repl_backlog_histlen = repl_backlog_idx，j = 0。也就是说，当缓冲区还没写满时，缓冲区中当前最早保存的数据的首字节，就是在缓冲区头，这是因为缓冲区没有被覆盖重写。
     * - 缓冲区被覆盖重写，即缓冲区已经写满过，并且已从头再次写入数据：此时，repl_backlog_histlen = repl_backlog_size ，j = repl_backlog_idx 。这也好理解，repl_backlog_idx 指向了下一次
     *   写入的数据位置，当缓冲区写满过，这个位置上是有数据的，而这个数据正是缓冲区中最早保存数据的首字节。一旦再次写入时，这个位置就会被覆盖重写了。
     */
    /* Point j to the oldest byte, that is actually our server.repl_backlog_off byte. */
    j = (server.repl_backlog_idx + (server.repl_backlog_size - server.repl_backlog_histlen)) % server.repl_backlog_size;
    serverLog(LL_DEBUG, "[PSYNC] Index of first byte: %lld", j);

    // 4 根据缓冲区中最早还没有被覆盖数据的位置，结合要跳过的长度，计算从节点的全局读取位置在缓冲区中的对应位置。
    // 因为这个位置值可能超越缓冲区长度边界，所以它要对 repl_backlog_size 取模。这样一来，就得到了从节点的全局读取位置在缓冲区中的对应位置了。
    /* Discard the amount of data to seek to the specified 'offset'. */
    j = (j + skip) % server.repl_backlog_size;

    /*-------------------- 执行到这里，就知道从节点要在循环缓冲区的哪个位置开始读取数据了 -----------------*/


    // 5 计算实际要读取的数据长度 len，这是用缓冲区中数据的实际长度减去要跳过的数据长度
    /* Feed slave with data. Since it is a circular buffer we have to
     * split the reply in two parts if we are cross-boundary. */
    len = server.repl_backlog_histlen - skip;
    serverLog(LL_DEBUG, "[PSYNC] Reply total length: %lld", len);

    // 6 执行一个循环流程来实际读取数据。
    // todo 之所以要设计一个循环流程来读取数据，是因为在循环缓冲区中，从节点可能从读取起始位置一直读到缓冲区尾后，还没有读完，还要再从缓冲区头继续读取。这就要分成两次来读取了
    while (len) {

        // 判断 当读取的起始位置 j 到循环缓冲区尾的长度与要读取的长度 len 的大小：
        // 6.1 < len : 表明从节点还要从头继续读数据，此时函数就先从读取起始位置一直读到缓冲区末尾 server.repl_backlog_size - j
        // 6.2 >= len : 直接读取要读的长度 len 即可
        long long thislen = ((server.repl_backlog_size - j) < len) ? (server.repl_backlog_size - j) : len;

        serverLog(LL_DEBUG, "[PSYNC] addReply() length: %lld", thislen);

        //6.3 todo 调用 addReplySds 函数，向从节点的客户端C缓冲区写入返回的数据。返回的数据使用 SDS 进行包装。
        // todo 也就是从复制积压缓冲区中读取数据，然后写入到复制缓冲区
        addReplySds(c, sdsnewlen(server.repl_backlog + j, thislen));

        // 6.4 更新要读取数据长度
        len -= thislen;

        // 位置更新为 0 ，表示下次要读取的话就从头开始读取一定长度的数据
        j = 0;
    }

    return server.repl_backlog_histlen - skip;
}

/* Return the offset to provide as reply to the PSYNC command received
 * from the slave. The returned value is only valid immediately after
 * the BGSAVE process started and before executing any other command
 * from clients. */
long long getPsyncInitialOffset(void) {
    return server.master_repl_offset;
}

/* Send a FULLRESYNC reply in the specific case of a full resynchronization,
 * as a side effect setup the slave for a full sync in different ways:
 *
 * 响应全量复制 FULLRESYNC ID offset
 *
 * 1) Remember, into the slave client structure, the replication offset
 *    we sent here, so that if new slaves will later attach to the same
 *    background RDB saving process (by duplicating this client output
 *    buffer), we can get the right offset from this slave.
 * 2) Set the replication state of the slave to WAIT_BGSAVE_END so that
 *    we start accumulating differences from this point.
 * 3) Force the replication stream to re-emit a SELECT statement so
 *    the new slave incremental differences will start selecting the
 *    right database number.
 *
 * Normally this function should be called immediately after a successful
 * BGSAVE for replication was started, or when there is one already in
 * progress that we attached our slave to. */
int replicationSetupSlaveForFullResync(client *slave, long long offset) {
    char buf[128];
    int buflen;

    slave->psync_initial_offset = offset;
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * slave as well. Set slaveseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    server.slaveseldb = -1;

    /* Don't send this reply to slaves that approached us with
     * the old SYNC command. */
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        buflen = snprintf(buf, sizeof(buf), "+FULLRESYNC %s %lld\r\n",
                          server.replid, offset);
        if (connWrite(slave->conn, buf, buflen) != buflen) {
            freeClientAsync(slave);
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function handles the PSYNC command from the point of view of a
 * master receiving a request for partial resynchronization.
 *
 * 该函数用于处理主节点收到从节点发送的 PSYNC 命令
 *
 * On success return C_OK, otherwise C_ERR is returned and we proceed
 * with the usual full resync.
 * 成功返回 C_OK，否则返回 C_ERR，继续进行通常全量同步。
 *
 */
int masterTryPartialResynchronization(client *c) {
    long long psync_offset, psync_len;
    char *master_replid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    /* Parse the replication offset asked by the slave. Go to full sync
     * on parse error: this should never happen but we try to handle
     * it in a robust way compared to aborting. */
    // 1 解析从节点请求的复制偏移量。解析错误时转到完全同步：这永远不会发生，但与中止相比，我们尝试以一种稳健的方式处理它。
    if (getLongLongFromObjectOrReply(c, c->argv[2], &psync_offset, NULL) !=
        C_OK)
        goto need_full_resync;

    /* Is the replication ID of this master the same advertised by the wannabe
     * slave via PSYNC? If the replication ID changed this master has a
     * different replication history, and there is no way to continue.
     *
     * Note that there are two potentially valid replication IDs: the ID1
     * and the ID2. The ID2 however is only valid up to a specific offset. */
    if (strcasecmp(master_replid, server.replid) &&
        (strcasecmp(master_replid, server.replid2) ||
         psync_offset > server.second_replid_offset)) {
        /* Replid "?" is used by slaves that want to force a full resync. */
        if (master_replid[0] != '?') {
            if (strcasecmp(master_replid, server.replid) &&
                strcasecmp(master_replid, server.replid2)) {
                serverLog(LL_NOTICE, "Partial resynchronization not accepted: "
                                     "Replication ID mismatch (Replica asked for '%s', my "
                                     "replication IDs are '%s' and '%s')",
                          master_replid, server.replid, server.replid2);
            } else {
                serverLog(LL_NOTICE, "Partial resynchronization not accepted: "
                                     "Requested offset for second ID was %lld, but I can reply "
                                     "up to %lld", psync_offset, server.second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE, "Full resync requested by replica %s",
                      replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* We still have the data our slave is asking for? */
    // 2 判断是否可以正常从主节点同步数据
    if (!server.repl_backlog || // 没有复制积压缓冲区
        psync_offset < server.repl_backlog_off || // 复制积压缓冲区中没有要复制的偏移量 psync_offset 对应的数据
        psync_offset > (server.repl_backlog_off + server.repl_backlog_histlen)) {
        serverLog(LL_NOTICE,
                  "Unable to partial resync with replica %s for lack of backlog (Replica request was: %lld).",
                  replicationGetSlaveName(c), psync_offset);
        if (psync_offset > server.master_repl_offset) {
            serverLog(LL_WARNING,
                      "Warning: replica %s tried to PSYNC with an offset that is greater than the master replication offset.",
                      replicationGetSlaveName(c));
        }
        // 不能从正常从主节点同步数据，只能全量同步数据
        goto need_full_resync;
    }

    /* If we reached this point, we are able to perform a partial resync:
     * 如果我们达到了这一点，我们就可以执行增量同步：
     *
     * 1) Set client state to make it a slave.
     *    设置从节点的客户端状态为 CLIENT_SLAVE
     * 2) Inform the client we can continue with +CONTINUE
     *    todo 响应从节点主节点执行增量复制 +CONTINUE
     * 3) Send the backlog data (from the offset to the end) to the slave.
     *    将复制积压数据（从偏移量到末尾）发送到从节点。
     */
    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;

    // todo 更新从节点的 ack 时间，也就是不仅仅是从节点发送 ACK 心跳会更新，其它请求也会
    c->repl_ack_time = server.unixtime;
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.slaves, c);
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * empty so this write will never fail actually. */
    if (c->slave_capa & SLAVE_CAPA_PSYNC2) {
        buflen = snprintf(buf, sizeof(buf), "+CONTINUE %s\r\n", server.replid);
    } else {
        buflen = snprintf(buf, sizeof(buf), "+CONTINUE\r\n");
    }
    // todo 响应从节点主节点执行增量复制 +CONTINUE，成功后接着发送数据
    if (connWrite(c->conn, buf, buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }

    // 3 todo 读取循环缓冲区中的数据，并发送给从节点；即主节点处理增量复制请求时，是立即将数据发送给从节点
    psync_len = addReplyReplicationBacklog(c, psync_offset);
    serverLog(LL_NOTICE,
              "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
              replicationGetSlaveName(c),
              psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT, since the slave already
     * has this state from the previous connection with the master. */

    // todo 更新没有延迟的从节点数量
    // 针对 min-slaves-max-lag 参数
    refreshGoodSlavesCount();

    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);

    // 返回 C_OK，表明是增量同步，不需要全量同步
    return C_OK; /* The caller can return, no full resync needed. */

    need_full_resync:
    /* We need a full resync for some reason... Note that we can't
     * reply to PSYNC right now if a full SYNC is needed. The reply
     * must include the master offset at the time the RDB file we transfer
     * is generated, so we need to delay the reply to that moment.
     *
     * todo 如果需要全量同步，现在无法回复 PSYNC。回复必须包含传输的 RDB 文件生成时的主偏移量，因此需要将回复延迟到那一刻，即该方法外部处理。
     */
    return C_ERR;
}

/* Start a BGSAVE for replication goals, which is, selecting the disk or
 * socket target depending on the configuration, and making sure that
 * the script cache is flushed before to start.
 *
 * 为复制目标启动 BGSAVE，即根据配置选择磁盘或套接字目标，并确保在启动之前刷新脚本缓存。
 *
 * The mincapa argument is the bitwise AND among all the slaves capabilities
 * of the slaves waiting for this BGSAVE, so represents the slave capabilities
 * all the slaves support. Can be tested via SLAVE_CAPA_* macros.
 *
 * Side effects, other than starting a BGSAVE:
 *
 * 1) Handle the slaves in WAIT_START state, by preparing them for a full
 *    sync if the BGSAVE was successfully started, or sending them an error
 *    and dropping them from the list of slaves.
 *    处理处于 WAIT_START 状态的从节点，如果 BGSAVE 已成功启动，则准备它们进行全量同步，或者向它们发送错误并将它们从从节点列表中删除。
 *
 * 2) Flush the Lua scripting script cache if the BGSAVE was actually
 *    started.
 *    如果实际启动了 BGSAVE，则刷新 Lua 脚本缓存。
 *
 * Returns C_OK on success or C_ERR otherwise. */
int startBgsaveForReplication(int mincapa) {
    int retval;

    // 是否无盘复制
    int socket_target = server.repl_diskless_sync && (mincapa & SLAVE_CAPA_EOF);
    listIter li;
    listNode *ln;

    serverLog(LL_NOTICE, "Starting BGSAVE for SYNC with target: %s",
              socket_target ? "replicas sockets" : "disk");

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    /* Only do rdbSave* when rsiptr is not NULL,
     * otherwise slave will miss repl-stream-db. */
    if (rsiptr) {
        // 无盘复制
        if (socket_target)
            // 创建子进程，让子进程直接发送 RDB 文件的二进制数据给从节点
            retval = rdbSaveToSlavesSockets(rsiptr);
        else
            // 创建子进程，让子进程生成 RDB 文件然后发送给从节点
            retval = rdbSaveBackground(server.rdb_filename, rsiptr);
    } else {
        serverLog(LL_WARNING,
                  "BGSAVE for replication: replication information not available, can't generate the RDB file right now. Try later.");
        retval = C_ERR;
    }

    /* If we succeeded to start a BGSAVE with disk target, let's remember
     * this fact, so that we can later delete the file if needed. Note
     * that we don't set the flag to 1 if the feature is disabled, otherwise
     * it would never be cleared: the file is not deleted. This way if
     * the user enables it later with CONFIG SET, we are fine. */
    if (retval == C_OK && !socket_target && server.rdb_del_sync_files)
        RDBGeneratedByReplication = 1;

    /* If we failed to BGSAVE, remove the slaves waiting for a full
     * resynchronization from the list of slaves, inform them with
     * an error about what happened, close the connection ASAP. */
    if (retval == C_ERR) {
        serverLog(LL_WARNING, "BGSAVE for replication failed");
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                slave->replstate = REPL_STATE_NONE;
                slave->flags &= ~CLIENT_SLAVE;
                listDelNode(server.slaves, ln);
                addReplyError(slave,
                              "BGSAVE failed, replication can't continue");
                slave->flags |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }
        return retval;
    }

    /* If the target is socket, rdbSaveToSlavesSockets() already setup
     * the slaves for a full resync.
     *
     * Otherwise for disk target do it now.*/
    if (!socket_target) {
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                replicationSetupSlaveForFullResync(slave,
                                                   getPsyncInitialOffset());
            }
        }
    }

    /* Flush the script cache, since we need that slave differences are
     * accumulated without requiring slaves to match our cached scripts. */
    if (retval == C_OK) replicationScriptCacheFlush();
    return retval;
}

/* SYNC and PSYNC command implementation.
 *
 * SYNC and PSYNC 命令实现函数
 * todo 从节点向主节点发送 PSYNC 命令，不支持 PSYNC 命令会发 SYNC
 */
void syncCommand(client *c) {
    /* ignore SYNC if already slave or in monitor mode */
    // 是 SLAVE ，或者处于 MONITOR 模式，返回
    if (c->flags & CLIENT_SLAVE) return;

    /* Check if this is a failover request to a replica with the same replid and
     * become a master if so. */
    if (c->argc > 3 && !strcasecmp(c->argv[0]->ptr, "psync") &&
        !strcasecmp(c->argv[3]->ptr, "failover")) {
        serverLog(LL_WARNING, "Failover request received for replid %s.",
                  (unsigned char *) c->argv[1]->ptr);
        if (!server.masterhost) {
            addReplyError(c, "PSYNC FAILOVER can't be sent to a master.");
            return;
        }

        if (!strcasecmp(c->argv[1]->ptr, server.replid)) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(), c);
            serverLog(LL_NOTICE,
                      "MASTER MODE enabled (failover request from '%s')", client);
            sdsfree(client);
        } else {
            addReplyError(c, "PSYNC FAILOVER replid must match my replid.");
            return;
        }
    }

    /* Don't let replicas sync with us while we're failing over */
    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c, "-NOMASTERLINK Can't SYNC while failing over");
        return;
    }

    /* Refuse SYNC requests if we are a slave but the link with our master
     * is not ok... */
    if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED) {
        addReplyError(c, "-NOMASTERLINK Can't SYNC while not connected with my master");
        return;
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed. */
    if (clientHasPendingReplies(c)) {
        addReplyError(c, "SYNC and PSYNC are invalid with pending output");
        return;
    }

    serverLog(LL_NOTICE, "Replica %s asks for synchronization",
              replicationGetSlaveName(c));

    /* Try a partial resynchronization if this is a PSYNC command.
     * 如果这是 PSYNC 命令，尝试增量同步。
     *
     * If it fails, we continue with usual full resynchronization, however
     * when this happens masterTryPartialResynchronization() already
     * replied with:
     * 如果失败，继续进行全量同步，但是当这种情况发生时 masterTryPartialResynchronization() 回复以下内容
     *
     * +FULLRESYNC <replid> <offset>
     *
     * So the slave knows the new replid and offset to try a PSYNC later
     * if the connection with the master is lost.
     *
     * 因此，slave 知道新的 replid 和 offset，以后在与 master 的连接丢失时尝试 PSYNC。
     */
    if (!strcasecmp(c->argv[0]->ptr, "psync")) {
        // 处理从节点发送的 psync 命令：
        // todo 返回 C_OK: 说明执行的是增量同步，该方法会把所需要的增量数据从复制积压缓冲区写入到从节点的复制缓冲区中，等待后续写回从节点
        // 返回 C_ERR：说明需要执行全量同步
        if (masterTryPartialResynchronization(c) == C_OK) { // 1) 响应 +CONTINUE  2) 将需要的增量数据写回从节点复制缓存区
            server.stat_sync_partial_ok++;
            return; /* No full resync needed, return. */

            // 返回 C_ERR
        } else {
            // 获取复制的主节点运行 ID
            char *master_replid = c->argv[1]->ptr;

            /* Increment stats for failed PSYNCs, but only if the
             * replid is not "?", as this is used by slaves to force a full
             * resync on purpose when they are not albe to partially
             * resync. */
            // 如果复制的主节点运行 ID 不是 ？，说明原本打算增量复制，但是进行到这里说明增量复制失败了
            if (master_replid[0] != '?') server.stat_sync_partial_err++;
        }


        // 从节点使用的是 SYNC
    } else {
        /* If a slave uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like redis-cli --slave). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        c->flags |= CLIENT_PRE_PSYNC;
    }

    /*-------------------------------- todo 以下是全量复制的逻辑；主节点根据从节点上报的是否支持无盘复制，决定使用子进程以哪种方式将 RDB 发送给从节点，这是异步的  -----------------*/
    /* Full resynchronization. */
    // 全量同步
    server.stat_sync_full++;

    /* Setup the slave as one waiting for BGSAVE to start. The following code
     * paths will change the state if we handle the slave differently. */
    // todo 将从节点客户端的复制状态设置为 等待 bgsave 启动
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;

    // 是否支持合并 TCP 数据包
    if (server.repl_disable_tcp_nodelay)
        connDisableTcpNoDelay(c->conn); /* Non critical if it fails. */

    c->repldbfd = -1;
    c->flags |= CLIENT_SLAVE;
    listAddNodeTail(server.slaves, c);


    /* Create the replication backlog if needed. */
    // 创建循环缓冲区必要条件：当前还没有循环缓冲区 && 当前的从节点只有 1 个
    // 也就是说，当一个主节点有多个从节点时，这些从节点其实会共享使用一个循环缓冲区，而这样设计的目的，主要是避免给每个从节点开辟一块缓冲区，造成内存资源浪费。
    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL) {
        /* When we create the backlog from scratch, we always use a new
         * replication ID and clear the ID2, since there is no valid
         * past history. */
        changeReplicationId();
        clearReplicationId2();

        // 创建循环缓冲区
        createReplicationBacklog();
        serverLog(LL_NOTICE, "Replication backlog created, my new "
                             "replication IDs are '%s' and '%s'",
                  server.replid, server.replid2);
    }

    /* CASE 1: BGSAVE is in progress, with disk target. */
    // 情况 1：BGSAVE 正在进行中，落盘的情况。
    if (server.child_type == CHILD_TYPE_RDB &&
        server.rdb_child_type == RDB_CHILD_TYPE_DISK) {
        /* Ok a background save is in progress.
         *
         * Let's check if it is a good one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save.
         *
         * 如果有至少一个 slave 在等待这个 BGSAVE 完成，那么说明正在进行的 BGSAVE 所产生的 RDB 也可以为其他 slave 所用
         */
        client *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            slave = ln->value;
            /* If the client needs a buffer of commands, we can't use
             * a replica without replication buffer. */
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
                (!(slave->flags & CLIENT_REPL_RDBONLY) ||
                 (c->flags & CLIENT_REPL_RDBONLY)))
                break;
        }
        /* To attach this slave, we check that it has at least all the
         * capabilities of the slave that triggered the current BGSAVE. */
        if (ln && ((c->slave_capa & slave->slave_capa) == slave->slave_capa)) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer.
             * We don't copy buffer if clients don't want. */
            // 幸运的情况下，可以使用目前 BGSAVE 所生成的 RDB
            if (!(c->flags & CLIENT_REPL_RDBONLY)) copyClientOutputBuffer(c, slave);

            // 响应从节点全量复制  +FULLRESYNC
            replicationSetupSlaveForFullResync(c, slave->psync_initial_offset);
            serverLog(LL_NOTICE, "Waiting for end of BGSAVE for SYNC");

            // 不好运的情况下，必须等待下一个 BGSAVE
            // todo bgsave 保证只能同时执行一个
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences. */
            serverLog(LL_NOTICE, "Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

        /* CASE 2: BGSAVE is in progress, with socket target. */
        // 情况 2：BGSAVE 正在进行中，无盘的情况。
        // todo 无盘复制不能共享 RDB
    } else if (server.child_type == CHILD_TYPE_RDB &&
               server.rdb_child_type == RDB_CHILD_TYPE_SOCKET) {
        /* There is an RDB child process but it is writing directly to
         * children sockets. We need to wait for the next BGSAVE
         * in order to synchronize. */
        // 虽然有 BGSAVE 在进行中，但是是直接网络传输给从节点的，因此需要等待下个 BGSAVE 才能同步
        // todo bgsave 保证只能同时执行一个
        serverLog(LL_NOTICE, "Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC");

        /* CASE 3: There is no BGSAVE is progress. */
        // 情况 3： 没有 BGSAVE 在进行，开始一个新的 BGSAVE
    } else {
        if (server.repl_diskless_sync && (c->slave_capa & SLAVE_CAPA_EOF) &&
            server.repl_diskless_sync_delay) {
            /* Diskless replication RDB child is created inside
             * replicationCron() since we want to delay its start a
             * few seconds to wait for more slaves to arrive.
             * 无盘复制 RDB 子节点是在 replicationCron() 内部创建的，因为我们希望将其启动延迟几秒钟以等待更多从属节点到达。
             */
            serverLog(LL_NOTICE, "Delay next BGSAVE for diskless SYNC");
        } else {
            /* We don't have a BGSAVE in progress, let's start one. Diskless
             * or disk-based mode is determined by replica's capacity.
             *
             * 我们没有正在进行的 BGSAVE，让我们开始一个。无盘或基于磁盘的模式由副本的容量决定。
             */
            if (!hasActiveChildProcess()) {
                // 开发进行全量复制：创建子进程进行有盘或无盘的 RDB 复制流程
                // todo 即由子进程执行，是异步的
                startBgsaveForReplication(c->slave_capa);
            } else {
                serverLog(LL_NOTICE,
                          "No BGSAVE in progress, but another BG operation is active. "
                          "BGSAVE for replication delayed");
            }
        }
    }
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * todo 用于处理从节点的 REPLCONF 命令
 *
 * This command is used by a replica in order to configure the replication
 * process before starting it with the SYNC command.
 * This command is also used by a master in order to get the replication
 * offset from a replica.
 *
 * Currently we support these options:
 *
 * - listening-port <port>
 * 从节点上报端口
 * - ip-address <ip>
 * 从节点上报 ip
 * What is the listening ip and port of the Replica redis instance, so that
 * the master can accurately lists replicas and their listening ports in the
 * INFO output.
 *
 * - capa <eof|psync2>
 * What is the capabilities of this instance.
 * 通知主节点从节点的能力
 * eof: supports EOF-style RDB transfer for diskless replication.
 * 是否支持无盘复制，即支持 EOF 样式的 RDB 传输以进行无盘复制。
 * psync2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
 * 是否支持 psync2 ，尽可能避免做全量同步
 * - ack <offset>
 * Replica informs the master the amount of replication stream that it
 * processed so far.
 * 从节点 通知 master 到目前为止它的复制偏移量。
 *
 * - getack
 * Unlike other subcommands, this is used by master to get the replication
 * offset from a replica.
 * 与其他子命令不同，master 使用它来获取副本的复制偏移量。
 *
 * - rdb-only
 * Only wants RDB snapshot without replication buffer. */
void replconfCommand(client *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j += 2) {
        // - listening-port <port>
        if (!strcasecmp(c->argv[j]->ptr, "listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c, c->argv[j + 1],
                                          &port, NULL) != C_OK))
                return;
            c->slave_listening_port = port;

            // - ip-address <ip>
        } else if (!strcasecmp(c->argv[j]->ptr, "ip-address")) {
            sds addr = c->argv[j + 1]->ptr;
            if (sdslen(addr) < NET_HOST_STR_LEN) {
                if (c->slave_addr) sdsfree(c->slave_addr);
                c->slave_addr = sdsdup(addr);
            } else {
                addReplyErrorFormat(c, "REPLCONF ip-address provided by "
                                       "replica instance is too long: %zd bytes", sdslen(addr));
                return;
            }

            //  - capa <eof|psync2>
        } else if (!strcasecmp(c->argv[j]->ptr, "capa")) {
            /* Ignore capabilities not understood by this master. */
            if (!strcasecmp(c->argv[j + 1]->ptr, "eof"))
                c->slave_capa |= SLAVE_CAPA_EOF;
            else if (!strcasecmp(c->argv[j + 1]->ptr, "psync2"))
                c->slave_capa |= SLAVE_CAPA_PSYNC2;

            // - ack <offset>
        } else if (!strcasecmp(c->argv[j]->ptr, "ack")) {
            /* REPLCONF ACK is used by slave to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use.
             *
             * 从节点使用 REPLCONF ACK 通知主节点到目前为止它处理的复制流的数量。这是普通客户端永远不应使用的内部唯一命令。
             */
            long long offset;

            if (!(c->flags & CLIENT_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j + 1], &offset) != C_OK))
                return;

            // 更新从节点的复制偏移量
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;

            // todo 更新当前服务节点的从节点的 ack 上报时间；该时间用于计算延迟时长，然后和 min-slaves-max-lag 比较
            // todo repl_ack_time 不仅仅作为 ack 上报时间，也作为从节点向主节点通信的更新时间
            c->repl_ack_time = server.unixtime;
            /* If this was a diskless replication, we need to really put
             * the slave online when the first ACK is received (which
             * confirms slave is online and ready to get more data). This
             * allows for simpler and less CPU intensive EOF detection
             * when streaming RDB files.
             * There's a chance the ACK got to us before we detected that the
             * bgsave is done (since that depends on cron ticks), so run a
             * quick check first (instead of waiting for the next ACK. */
            if (server.child_type == CHILD_TYPE_RDB && c->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
                checkChildrenDone();
            if (c->repl_put_online_on_ack && c->replstate == SLAVE_STATE_ONLINE)
                putSlaveOnline(c);
            /* Note: this command does not reply anything! */
            return;

            // - getack
        } else if (!strcasecmp(c->argv[j]->ptr, "getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the slave. */
            if (server.masterhost && server.master) replicationSendAck();
            return;

            // - rdb-only
        } else if (!strcasecmp(c->argv[j]->ptr, "rdb-only")) {
            /* REPLCONF RDB-ONLY is used to identify the client only wants
             * RDB snapshot without replication buffer. */
            long rdb_only = 0;
            if (getRangeLongFromObjectOrReply(c, c->argv[j + 1],
                                              0, 1, &rdb_only, NULL) != C_OK)
                return;
            if (rdb_only == 1) c->flags |= CLIENT_REPL_RDBONLY;
            else c->flags &= ~CLIENT_REPL_RDBONLY;

            // 未知的命令参数选项
        } else {
            addReplyErrorFormat(c, "Unrecognized REPLCONF option: %s",
                                (char *) c->argv[j]->ptr);
            return;
        }
    }
    // 响应从节点
    addReply(c, shared.ok);
}

/* This function puts a replica in the online state, and should be called just
 * after a replica received the RDB file for the initial synchronization, and
 * we are finally ready to send the incremental stream of commands.
 *
 * It does a few things:
 * 1) Close the replica's connection async if it doesn't need replication
 *    commands buffer stream, since it actually isn't a valid replica.
 * 2) Put the slave in ONLINE state. Note that the function may also be called
 *    for a replicas that are already in ONLINE state, but having the flag
 *    repl_put_online_on_ack set to true: we still have to install the write
 *    handler in that case. This function will take care of that.
 * 3) Make sure the writable event is re-installed, since calling the SYNC
 *    command disables it, so that we can accumulate output buffer without
 *    sending it to the replica.
 * 4) Update the count of "good replicas". */
void putSlaveOnline(client *slave) {
    slave->replstate = SLAVE_STATE_ONLINE;
    slave->repl_put_online_on_ack = 0;
    slave->repl_ack_time = server.unixtime; /* Prevent false timeout. */

    if (slave->flags & CLIENT_REPL_RDBONLY) {
        serverLog(LL_NOTICE,
                  "Close the connection with replica %s as RDB transfer is complete",
                  replicationGetSlaveName(slave));
        freeClientAsync(slave);
        return;
    }
    if (connSetWriteHandler(slave->conn, sendReplyToClient) == C_ERR) {
        serverLog(LL_WARNING, "Unable to register writable event for replica bulk transfer: %s", strerror(errno));
        freeClient(slave);
        return;
    }
    refreshGoodSlavesCount();
    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
    serverLog(LL_NOTICE, "Synchronization with replica %s succeeded",
              replicationGetSlaveName(slave));
}

/* We call this function periodically to remove an RDB file that was
 * generated because of replication, in an instance that is otherwise
 * without any persistence. We don't want instances without persistence
 * to take RDB files around, this violates certain policies in certain
 * environments. */
void removeRDBUsedToSyncReplicas(void) {
    /* If the feature is disabled, return ASAP but also clear the
     * RDBGeneratedByReplication flag in case it was set. Otherwise if the
     * feature was enabled, but gets disabled later with CONFIG SET, the
     * flag may remain set to one: then next time the feature is re-enabled
     * via CONFIG SET we have have it set even if no RDB was generated
     * because of replication recently. */
    if (!server.rdb_del_sync_files) {
        RDBGeneratedByReplication = 0;
        return;
    }

    if (allPersistenceDisabled() && RDBGeneratedByReplication) {
        client *slave;
        listNode *ln;
        listIter li;

        int delrdb = 1;
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
                slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END ||
                slave->replstate == SLAVE_STATE_SEND_BULK) {
                delrdb = 0;
                break; /* No need to check the other replicas. */
            }
        }
        if (delrdb) {
            struct stat sb;
            if (lstat(server.rdb_filename, &sb) != -1) {
                RDBGeneratedByReplication = 0;
                serverLog(LL_NOTICE,
                          "Removing the RDB file used to feed replicas "
                          "in a persistence-less instance");
                bg_unlink(server.rdb_filename);
            }
        }
    }
}

void sendBulkToSlave(connection *conn) {
    client *slave = connGetPrivateData(conn);
    char buf[PROTO_IOBUF_LEN];
    ssize_t nwritten, buflen;

    /* Before sending the RDB file, we send the preamble as configured by the
     * replication process. Currently the preamble is just the bulk count of
     * the file in the form "$<length>\r\n". */
    if (slave->replpreamble) {
        nwritten = connWrite(conn, slave->replpreamble, sdslen(slave->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_VERBOSE,
                      "Write error sending RDB preamble to replica: %s",
                      connGetLastError(conn));
            freeClient(slave);
            return;
        }
        atomicIncr(server.stat_net_output_bytes, nwritten);
        sdsrange(slave->replpreamble, nwritten, -1);
        if (sdslen(slave->replpreamble) == 0) {
            sdsfree(slave->replpreamble);
            slave->replpreamble = NULL;
            /* fall through sending data. */
        } else {
            return;
        }
    }

    /* If the preamble was already transferred, send the RDB bulk data. */
    lseek(slave->repldbfd, slave->repldboff, SEEK_SET);
    buflen = read(slave->repldbfd, buf, PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING, "Read error sending DB to replica: %s",
                  (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    if ((nwritten = connWrite(conn, buf, buflen)) == -1) {
        if (connGetState(conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_WARNING, "Write error sending DB to replica: %s",
                      connGetLastError(conn));
            freeClient(slave);
        }
        return;
    }
    slave->repldboff += nwritten;
    atomicIncr(server.stat_net_output_bytes, nwritten);
    if (slave->repldboff == slave->repldbsize) {
        close(slave->repldbfd);
        slave->repldbfd = -1;
        connSetWriteHandler(slave->conn, NULL);
        putSlaveOnline(slave);
    }
}

/* Remove one write handler from the list of connections waiting to be writable
 * during rdb pipe transfer. */
void rdbPipeWriteHandlerConnRemoved(struct connection *conn) {
    if (!connHasWriteHandler(conn))
        return;
    connSetWriteHandler(conn, NULL);
    client *slave = connGetPrivateData(conn);
    slave->repl_last_partial_write = 0;
    server.rdb_pipe_numconns_writing--;
    /* if there are no more writes for now for this conn, or write error: */
    if (server.rdb_pipe_numconns_writing == 0) {
        if (aeCreateFileEvent(server.el, server.rdb_pipe_read, AE_READABLE, rdbPipeReadHandler, NULL) == AE_ERR) {
            serverPanic("Unrecoverable error creating server.rdb_pipe_read file event.");
        }
    }
}

/* Called in diskless master during transfer of data from the rdb pipe, when
 * the replica becomes writable again. */
void rdbPipeWriteHandler(struct connection *conn) {
    serverAssert(server.rdb_pipe_bufflen > 0);
    client *slave = connGetPrivateData(conn);
    int nwritten;
    if ((nwritten = connWrite(conn, server.rdb_pipe_buff + slave->repldboff,
                              server.rdb_pipe_bufflen - slave->repldboff)) == -1) {
        if (connGetState(conn) == CONN_STATE_CONNECTED)
            return; /* equivalent to EAGAIN */
        serverLog(LL_WARNING, "Write error sending DB to replica: %s",
                  connGetLastError(conn));
        freeClient(slave);
        return;
    } else {
        slave->repldboff += nwritten;
        atomicIncr(server.stat_net_output_bytes, nwritten);
        if (slave->repldboff < server.rdb_pipe_bufflen) {
            slave->repl_last_partial_write = server.unixtime;
            return; /* more data to write.. */
        }
    }
    rdbPipeWriteHandlerConnRemoved(conn);
}

/* Called in diskless master, when there's data to read from the child's rdb pipe */
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    int i;
    if (!server.rdb_pipe_buff)
        server.rdb_pipe_buff = zmalloc(PROTO_IOBUF_LEN);
    serverAssert(server.rdb_pipe_numconns_writing == 0);

    while (1) {
        server.rdb_pipe_bufflen = read(fd, server.rdb_pipe_buff, PROTO_IOBUF_LEN);
        if (server.rdb_pipe_bufflen < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            serverLog(LL_WARNING, "Diskless rdb transfer, read error sending DB to replicas: %s", strerror(errno));
            for (i = 0; i < server.rdb_pipe_numconns; i++) {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                client *slave = connGetPrivateData(conn);
                freeClient(slave);
                server.rdb_pipe_conns[i] = NULL;
            }
            killRDBChild();
            return;
        }

        if (server.rdb_pipe_bufflen == 0) {
            /* EOF - write end was closed. */
            int stillUp = 0;
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            for (i = 0; i < server.rdb_pipe_numconns; i++) {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                stillUp++;
            }
            serverLog(LL_WARNING, "Diskless rdb transfer, done reading from pipe, %d replicas still up.", stillUp);
            /* Now that the replicas have finished reading, notify the child that it's safe to exit. 
             * When the server detectes the child has exited, it can mark the replica as online, and
             * start streaming the replication buffers. */
            close(server.rdb_child_exit_pipe);
            server.rdb_child_exit_pipe = -1;
            return;
        }

        int stillAlive = 0;
        for (i = 0; i < server.rdb_pipe_numconns; i++) {
            int nwritten;
            connection *conn = server.rdb_pipe_conns[i];
            if (!conn)
                continue;

            client *slave = connGetPrivateData(conn);
            if ((nwritten = connWrite(conn, server.rdb_pipe_buff, server.rdb_pipe_bufflen)) == -1) {
                if (connGetState(conn) != CONN_STATE_CONNECTED) {
                    serverLog(LL_WARNING, "Diskless rdb transfer, write error sending DB to replica: %s",
                              connGetLastError(conn));
                    freeClient(slave);
                    server.rdb_pipe_conns[i] = NULL;
                    continue;
                }
                /* An error and still in connected state, is equivalent to EAGAIN */
                slave->repldboff = 0;
            } else {
                /* Note: when use diskless replication, 'repldboff' is the offset
                 * of 'rdb_pipe_buff' sent rather than the offset of entire RDB. */
                slave->repldboff = nwritten;
                atomicIncr(server.stat_net_output_bytes, nwritten);
            }
            /* If we were unable to write all the data to one of the replicas,
             * setup write handler (and disable pipe read handler, below) */
            if (nwritten != server.rdb_pipe_bufflen) {
                slave->repl_last_partial_write = server.unixtime;
                server.rdb_pipe_numconns_writing++;
                connSetWriteHandler(conn, rdbPipeWriteHandler);
            }
            stillAlive++;
        }

        if (stillAlive == 0) {
            serverLog(LL_WARNING, "Diskless rdb transfer, last replica dropped, killing fork child.");
            killRDBChild();
        }
        /*  Remove the pipe read handler if at least one write handler was set. */
        if (server.rdb_pipe_numconns_writing || stillAlive == 0) {
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            break;
        }
    }
}

/* This function is called at the end of every background saving,
 * or when the replication RDB transfer strategy is modified from
 * disk to socket or the other way around.
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization, and
 * to schedule a new BGSAVE if there are slaves that attached while a
 * BGSAVE was in progress, but it was not a good one for replication (no
 * other slave was accumulating differences).
 *
 * The argument bgsaveerr is C_OK if the background saving succeeded
 * otherwise C_ERR is passed to the function.
 * The 'type' argument is the type of the child that terminated
 * (if it had a disk or socket target). */
void updateSlavesWaitingBgsave(int bgsaveerr, int type) {
    listNode *ln;
    listIter li;

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            if (bgsaveerr != C_OK) {
                freeClient(slave);
                serverLog(LL_WARNING, "SYNC failed. BGSAVE child returned an error");
                continue;
            }

            /* If this was an RDB on disk save, we have to prepare to send
             * the RDB from disk to the slave socket. Otherwise if this was
             * already an RDB -> Slaves socket transfer, used in the case of
             * diskless replication, our work is trivial, we can just put
             * the slave online. */
            if (type == RDB_CHILD_TYPE_SOCKET) {
                serverLog(LL_NOTICE,
                          "Streamed RDB transfer with replica %s succeeded (socket). Waiting for REPLCONF ACK from slave to enable streaming",
                          replicationGetSlaveName(slave));
                /* Note: we wait for a REPLCONF ACK message from the replica in
                 * order to really put it online (install the write handler
                 * so that the accumulated data can be transferred). However
                 * we change the replication state ASAP, since our slave
                 * is technically online now.
                 *
                 * So things work like that:
                 *
                 * 1. We end trasnferring the RDB file via socket.
                 * 2. The replica is put ONLINE but the write handler
                 *    is not installed.
                 * 3. The replica however goes really online, and pings us
                 *    back via REPLCONF ACK commands.
                 * 4. Now we finally install the write handler, and send
                 *    the buffers accumulated so far to the replica.
                 *
                 * But why we do that? Because the replica, when we stream
                 * the RDB directly via the socket, must detect the RDB
                 * EOF (end of file), that is a special random string at the
                 * end of the RDB (for streamed RDBs we don't know the length
                 * in advance). Detecting such final EOF string is much
                 * simpler and less CPU intensive if no more data is sent
                 * after such final EOF. So we don't want to glue the end of
                 * the RDB trasfer with the start of the other replication
                 * data. */
                slave->replstate = SLAVE_STATE_ONLINE;
                slave->repl_put_online_on_ack = 1;
                slave->repl_ack_time = server.unixtime; /* Timeout otherwise. */
            } else {
                if ((slave->repldbfd = open(server.rdb_filename, O_RDONLY)) == -1 ||
                    redis_fstat(slave->repldbfd, &buf) == -1) {
                    freeClient(slave);
                    serverLog(LL_WARNING, "SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                    continue;
                }
                slave->repldboff = 0;
                slave->repldbsize = buf.st_size;
                slave->replstate = SLAVE_STATE_SEND_BULK;
                slave->replpreamble = sdscatprintf(sdsempty(), "$%lld\r\n",
                                                   (unsigned long long) slave->repldbsize);

                connSetWriteHandler(slave->conn, NULL);
                if (connSetWriteHandler(slave->conn, sendBulkToSlave) == C_ERR) {
                    freeClient(slave);
                    continue;
                }
            }
        }
    }
}

/* Change the current instance replication ID with a new, random one.
 * This will prevent successful PSYNCs between this master and other
 * slaves, so the command should be called when something happens that
 * alters the current story of the dataset. */
void changeReplicationId(void) {
    getRandomHexChars(server.replid, CONFIG_RUN_ID_SIZE);
    server.replid[CONFIG_RUN_ID_SIZE] = '\0';
}

/* Clear (invalidate) the secondary replication ID. This happens, for
 * example, after a full resynchronization, when we start a new replication
 * history. */
void clearReplicationId2(void) {
    memset(server.replid2, '0', sizeof(server.replid));
    server.replid2[CONFIG_RUN_ID_SIZE] = '\0';
    server.second_replid_offset = -1;
}

/* Use the current replication ID / offset as secondary replication
 * ID, and change the current one in order to start a new history.
 * This should be used when an instance is switched from slave to master
 * so that it can serve PSYNC requests performed using the master
 * replication ID. */
void shiftReplicationId(void) {
    memcpy(server.replid2, server.replid, sizeof(server.replid));
    /* We set the second replid offset to the master offset + 1, since
     * the slave will ask for the first byte it has not yet received, so
     * we need to add one to the offset: for example if, as a slave, we are
     * sure we have the same history as the master for 50 bytes, after we
     * are turned into a master, we can accept a PSYNC request with offset
     * 51, since the slave asking has the same history up to the 50th
     * byte, and is asking for the new bytes starting at offset 51. */
    server.second_replid_offset = server.master_repl_offset + 1;
    changeReplicationId();
    serverLog(LL_WARNING, "Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s",
              server.replid2, server.second_replid_offset, server.replid);
}

/* ----------------------------------- SLAVE -------------------------------- */

/* Returns 1 if the given replication state is a handshake state,
 * 0 otherwise. */
int slaveIsInHandshakeState(void) {
    return server.repl_state >= REPL_STATE_RECEIVE_PING_REPLY &&
           server.repl_state <= REPL_STATE_RECEIVE_PSYNC_REPLY;
}

/* Avoid the master to detect the slave is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entirely or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the master. */
void replicationSendNewlineToMaster(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        /* Pinging back in this stage is best-effort. */
        if (server.repl_transfer_s) connWrite(server.repl_transfer_s, "\n", 1);
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the master. */
void replicationEmptyDbCallback(void *privdata) {
    UNUSED(privdata);
    if (server.repl_state == REPL_STATE_TRANSFER)
        replicationSendNewlineToMaster();
}

/* Once we have a link with the master and the synchronization was
 * performed, this function materializes the master client we store
 * at server.master, starting from the specified file descriptor. */
void replicationCreateMasterClient(connection *conn, int dbid) {
    server.master = createClient(conn);
    if (conn)
        connSetReadHandler(server.master->conn, readQueryFromClient);

    /**
     * Important note:
     * The CLIENT_DENY_BLOCKING flag is not, and should not, be set here.
     * For commands like BLPOP, it makes no sense to block the master
     * connection, and such blocking attempt will probably cause deadlock and
     * break the replication. We consider such a thing as a bug because
     * commands as BLPOP should never be sent on the replication link.
     * A possible use-case for blocking the replication link is if a module wants
     * to pass the execution to a background thread and unblock after the
     * execution is done. This is the reason why we allow blocking the replication
     * connection. */
    server.master->flags |= CLIENT_MASTER;

    server.master->authenticated = 1;
    server.master->reploff = server.master_initial_offset;
    server.master->read_reploff = server.master->reploff;
    server.master->user = NULL; /* This client can do everything. */
    memcpy(server.master->replid, server.master_replid,
           sizeof(server.master_replid));
    /* If master offset is set to -1, this master is old and is not
     * PSYNC capable, so we flag it accordingly. */
    if (server.master->reploff == -1)
        server.master->flags |= CLIENT_PRE_PSYNC;
    if (dbid != -1) selectDb(server.master, dbid);
}

/* This function will try to re-enable the AOF file after the
 * master-replica synchronization: if it fails after multiple attempts
 * the replica cannot be considered reliable and exists with an
 * error. */
void restartAOFAfterSYNC() {
    unsigned int tries, max_tries = 10;
    for (tries = 0; tries < max_tries; ++tries) {
        if (startAppendOnly() == C_OK) break;
        serverLog(LL_WARNING,
                  "Failed enabling the AOF after successful master synchronization! "
                  "Trying it again in one second.");
        sleep(1);
    }
    if (tries == max_tries) {
        serverLog(LL_WARNING,
                  "FATAL: this replica instance finished the synchronization with "
                  "its master, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

static int useDisklessLoad() {
    /* compute boolean decision to use diskless load */
    int enabled = server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB ||
                  (server.repl_diskless_load == REPL_DISKLESS_LOAD_WHEN_DB_EMPTY && dbTotalServerKeyCount() == 0);
    /* Check all modules handle read errors, otherwise it's not safe to use diskless load. */
    if (enabled && !moduleAllDatatypesHandleErrors()) {
        serverLog(LL_WARNING,
                  "Skipping diskless-load because there are modules that don't handle read errors.");
        enabled = 0;
    }
    return enabled;
}

/* Helper function for readSyncBulkPayload() to make backups of the current
 * databases before socket-loading the new ones. The backups may be restored
 * by disklessLoadRestoreBackup or freed by disklessLoadDiscardBackup later. */
dbBackup *disklessLoadMakeBackup(void) {
    return backupDb();
}

/* Helper function for readSyncBulkPayload(): when replica-side diskless
 * database loading is used, Redis makes a backup of the existing databases
 * before loading the new ones from the socket.
 *
 * If the socket loading went wrong, we want to restore the old backups
 * into the server databases. */
void disklessLoadRestoreBackup(dbBackup *buckup) {
    restoreDbBackup(buckup);
}

/* Helper function for readSyncBulkPayload() to discard our old backups
 * when the loading succeeded. */
void disklessLoadDiscardBackup(dbBackup *buckup, int flag) {
    discardDbBackup(buckup, flag, replicationEmptyDbCallback);
}

/* Asynchronously read the SYNC payload we receive from a master */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */

/**
 * 异步 RDB 文件读取函数
 * @param conn
 */
void readSyncBulkPayload(connection *conn) {
    char buf[PROTO_IOBUF_LEN];
    ssize_t nread, readlen, nwritten;

    // 是否无盘加载
    int use_diskless_load = useDisklessLoad();
    dbBackup *diskless_load_backup = NULL;

    // 根据 slave-lazy-flush 选择是否异步
    int empty_db_flags = server.repl_slave_lazy_flush ? EMPTYDB_ASYNC :
                         EMPTYDB_NO_FLAGS;
    off_t left;

    /* Static vars used to hold the EOF mark, and the last bytes received
     * from the server: when they match, we reached the end of the transfer. */
    static char eofmark[CONFIG_RUN_ID_SIZE];
    static char lastbytes[CONFIG_RUN_ID_SIZE];
    static int usemark = 0;

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the master reply. */
    // 读取 RDB 文件的大小
    if (server.repl_transfer_size == -1) {
        // 调用读函数
        if (connSyncReadLine(conn, buf, 1024, server.repl_syncio_timeout * 1000) == -1) {
            serverLog(LL_WARNING,
                      "I/O error reading bulk count from MASTER: %s",
                      strerror(errno));
            goto error;
        }

        // 出错
        if (buf[0] == '-') {
            serverLog(LL_WARNING,
                      "MASTER aborted replication with an error: %s",
                      buf + 1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            serverLog(LL_WARNING,
                      "Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?",
                      buf);
            goto error;
        }

        /* There are two possible forms for the bulk payload. One is the
         * usual $<count> bulk format. The other is used for diskless transfers
         * when the master does not know beforehand the size of the file to
         * transfer. In the latter case, the following format is used:
         *
         * $EOF:<40 bytes delimiter>
         *
         * At the end of the file the announced delimiter is transmitted. The
         * delimiter is long and random enough that the probability of a
         * collision with the actual file content can be ignored. */
        if (strncmp(buf + 1, "EOF:", 4) == 0 && strlen(buf + 5) >= CONFIG_RUN_ID_SIZE) {
            usemark = 1;
            memcpy(eofmark, buf + 5, CONFIG_RUN_ID_SIZE);
            memset(lastbytes, 0, CONFIG_RUN_ID_SIZE);
            /* Set any repl_transfer_size to avoid entering this code path
             * at the next call. */
            server.repl_transfer_size = 0;
            serverLog(LL_NOTICE,
                      "MASTER <-> REPLICA sync: receiving streamed RDB from master with EOF %s",
                      use_diskless_load ? "to parser" : "to disk");
        } else {
            usemark = 0;

            // 分析 RDB 文件大小，根据该大小判断是否读取完毕
            server.repl_transfer_size = strtol(buf + 1, NULL, 10);
            serverLog(LL_NOTICE,
                      "MASTER <-> REPLICA sync: receiving %lld bytes from master %s",
                      (long long) server.repl_transfer_size,
                      use_diskless_load ? "to parser" : "to disk");
        }
        return;
    }

    // 无盘加载
    if (!use_diskless_load) {
        /* Read the data from the socket, store it to a file and search
         * for the EOF. */
        if (usemark) {
            readlen = sizeof(buf);
        } else {
            left = server.repl_transfer_size - server.repl_transfer_read;
            readlen = (left < (signed) sizeof(buf)) ? left : (signed) sizeof(buf);
        }

        // 读取
        nread = connRead(conn, buf, readlen);
        if (nread <= 0) {
            if (connGetState(conn) == CONN_STATE_CONNECTED) {
                /* equivalent to EAGAIN */
                return;
            }
            serverLog(LL_WARNING, "I/O error trying to sync with MASTER: %s",
                      (nread == -1) ? strerror(errno) : "connection lost");
            cancelReplicationHandshake(1);
            return;
        }
        atomicIncr(server.stat_net_input_bytes, nread);

        /* When a mark is used, we want to detect EOF asap in order to avoid
         * writing the EOF mark into the file... */
        int eof_reached = 0;

        if (usemark) {
            /* Update the last bytes array, and check if it matches our
             * delimiter. */
            if (nread >= CONFIG_RUN_ID_SIZE) {
                memcpy(lastbytes, buf + nread - CONFIG_RUN_ID_SIZE,
                       CONFIG_RUN_ID_SIZE);
            } else {
                int rem = CONFIG_RUN_ID_SIZE - nread;
                memmove(lastbytes, lastbytes + nread, rem);
                memcpy(lastbytes + rem, buf, nread);
            }
            if (memcmp(lastbytes, eofmark, CONFIG_RUN_ID_SIZE) == 0)
                eof_reached = 1;
        }

        /* Update the last I/O time for the replication transfer (used in
         * order to detect timeouts during replication), and write what we
         * got from the socket to the dump file on disk. */
        server.repl_transfer_lastio = server.unixtime;
        if ((nwritten = write(server.repl_transfer_fd, buf, nread)) != nread) {
            serverLog(LL_WARNING,
                      "Write error or short write writing to the DB dump file "
                      "needed for MASTER <-> REPLICA synchronization: %s",
                      (nwritten == -1) ? strerror(errno) : "short write");
            goto error;
        }
        server.repl_transfer_read += nread;

        /* Delete the last 40 bytes from the file if we reached EOF. */
        if (usemark && eof_reached) {
            if (ftruncate(server.repl_transfer_fd,
                          server.repl_transfer_read - CONFIG_RUN_ID_SIZE) == -1) {
                serverLog(LL_WARNING,
                          "Error truncating the RDB file received from the master "
                          "for SYNC: %s", strerror(errno));
                goto error;
            }
        }

        /* Sync data on disk from time to time, otherwise at the end of the
         * transfer we may suffer a big delay as the memory buffers are copied
         * into the actual disk. */
        // todo 定期将读入的文件 fsync 到磁盘，以免 buffer 太多，一下子写入时撑爆 IO
        if (server.repl_transfer_read >=
            server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC) {
            off_t sync_size = server.repl_transfer_read -
                              server.repl_transfer_last_fsync_off;
            rdb_fsync_range(server.repl_transfer_fd,
                            server.repl_transfer_last_fsync_off, sync_size);
            server.repl_transfer_last_fsync_off += sync_size;
        }

        /* Check if the transfer is now complete */
        // 检查 RDB 是否已经传送完毕
        if (!usemark) {
            if (server.repl_transfer_read == server.repl_transfer_size)
                eof_reached = 1;
        }

        /* If the transfer is yet not complete, we need to read more, so
         * return ASAP and wait for the handler to be called again. */
        // 没有传送完毕，则返回，因为需要读取更多数据。
        if (!eof_reached) return;
    }

    /* We reach this point in one of the following cases:
     *
     * 1. The replica is using diskless replication, that is, it reads data
     *    directly from the socket to the Redis memory, without using
     *    a temporary RDB file on disk. In that case we just block and
     *    read everything from the socket.
     *
     * 2. Or when we are done reading from the socket to the RDB file, in
     *    such case we want just to read the RDB file in memory. */
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Flushing old data");

    /* We need to stop any AOF rewriting child before flusing and parsing
     * the RDB, otherwise we'll create a copy-on-write disaster. */
    if (server.aof_state != AOF_OFF) stopAppendOnly();

    /* When diskless RDB loading is used by replicas, it may be configured
     * in order to save the current DB instead of throwing it away,
     * so that we can restore it in case of failed transfer. */
    if (use_diskless_load &&
        server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
        /* Create a backup of server.db[] and initialize to empty
         * dictionaries. */
        diskless_load_backup = disklessLoadMakeBackup();
    }
    /* We call to emptyDb even in case of REPL_DISKLESS_LOAD_SWAPDB
     * (Where disklessLoadMakeBackup left server.db empty) because we
     * want to execute all the auxiliary logic of emptyDb (Namely,
     * fire module events) */
    emptyDb(-1, empty_db_flags, replicationEmptyDbCallback);

    /* Before loading the DB into memory we need to delete the readable
     * handler, otherwise it will get called recursively since
     * rdbLoad() will call the event loop to process events from time to
     * time for non blocking loading. */
    connSetReadHandler(conn, NULL);
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Loading DB in memory");
    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
    if (use_diskless_load) {
        rio rdb;
        rioInitWithConn(&rdb, conn, server.repl_transfer_size);

        /* Put the socket in blocking mode to simplify RDB transfer.
         * We'll restore it when the RDB is received. */
        connBlock(conn);
        connRecvTimeout(conn, server.repl_timeout * 1000);
        startLoading(server.repl_transfer_size, RDBFLAGS_REPLICATION);

        if (rdbLoadRio(&rdb, RDBFLAGS_REPLICATION, &rsi) != C_OK) {
            /* RDB loading failed. */
            stopLoading(0);
            serverLog(LL_WARNING,
                      "Failed trying to load the MASTER synchronization DB "
                      "from socket");
            cancelReplicationHandshake(1);
            rioFreeConn(&rdb, NULL);

            /* Remove the half-loaded data in case we started with
             * an empty replica. */
            emptyDb(-1, empty_db_flags, replicationEmptyDbCallback);

            if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
                /* Restore the backed up databases. */
                disklessLoadRestoreBackup(diskless_load_backup);
            }

            /* Note that there's no point in restarting the AOF on SYNC
             * failure, it'll be restarted when sync succeeds or the replica
             * gets promoted. */
            return;
        }

        /* RDB loading succeeded if we reach this point. */
        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* Delete the backup databases we created before starting to load
             * the new RDB. Now the RDB was loaded with success so the old
             * data is useless. */
            disklessLoadDiscardBackup(diskless_load_backup, empty_db_flags);
        }

        /* Verify the end mark is correct. */
        if (usemark) {
            if (!rioRead(&rdb, buf, CONFIG_RUN_ID_SIZE) ||
                memcmp(buf, eofmark, CONFIG_RUN_ID_SIZE) != 0) {
                stopLoading(0);
                serverLog(LL_WARNING, "Replication stream EOF marker is broken");
                cancelReplicationHandshake(1);
                rioFreeConn(&rdb, NULL);
                return;
            }
        }

        stopLoading(1);

        /* Cleanup and restore the socket to the original state to continue
         * with the normal replication. */
        rioFreeConn(&rdb, NULL);
        connNonBlock(conn);
        connRecvTimeout(conn, 0);
    } else {
        /* Ensure background save doesn't overwrite synced data */
        if (server.child_type == CHILD_TYPE_RDB) {
            serverLog(LL_NOTICE,
                      "Replica is about to load the RDB file received from the "
                      "master, but there is a pending RDB child running. "
                      "Killing process %ld and removing its temp file to avoid "
                      "any race",
                      (long) server.child_pid);
            killRDBChild();
        }

        /* Make sure the new file (also used for persistence) is fully synced
         * (not covered by earlier calls to rdb_fsync_range). */
        if (fsync(server.repl_transfer_fd) == -1) {
            serverLog(LL_WARNING,
                      "Failed trying to sync the temp DB to disk in "
                      "MASTER <-> REPLICA synchronization: %s",
                      strerror(errno));
            cancelReplicationHandshake(1);
            return;
        }

        /* Rename rdb like renaming rewrite aof asynchronously. */
        int old_rdb_fd = open(server.rdb_filename, O_RDONLY | O_NONBLOCK);
        if (rename(server.repl_transfer_tmpfile, server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                      "Failed trying to rename the temp DB into %s in "
                      "MASTER <-> REPLICA synchronization: %s",
                      server.rdb_filename, strerror(errno));
            cancelReplicationHandshake(1);
            if (old_rdb_fd != -1) close(old_rdb_fd);
            return;
        }
        /* Close old rdb asynchronously. */
        if (old_rdb_fd != -1) bioCreateCloseJob(old_rdb_fd);

        if (rdbLoad(server.rdb_filename, &rsi, RDBFLAGS_REPLICATION) != C_OK) {
            serverLog(LL_WARNING,
                      "Failed trying to load the MASTER synchronization "
                      "DB from disk");
            cancelReplicationHandshake(1);
            if (server.rdb_del_sync_files && allPersistenceDisabled()) {
                serverLog(LL_NOTICE, "Removing the RDB file obtained from "
                                     "the master. This replica has persistence "
                                     "disabled");
                bg_unlink(server.rdb_filename);
            }
            /* Note that there's no point in restarting the AOF on sync failure,
               it'll be restarted when sync succeeds or replica promoted. */
            return;
        }

        /* Cleanup. */
        if (server.rdb_del_sync_files && allPersistenceDisabled()) {
            serverLog(LL_NOTICE, "Removing the RDB file obtained from "
                                 "the master. This replica has persistence "
                                 "disabled");
            bg_unlink(server.rdb_filename);
        }

        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);
        server.repl_transfer_fd = -1;
        server.repl_transfer_tmpfile = NULL;
    }

    /* Final setup of the connected slave <- master link */
    replicationCreateMasterClient(server.repl_transfer_s, rsi.repl_stream_db);
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* After a full resynchronization we use the replication ID and
     * offset of the master. The secondary ID / offset are cleared since
     * we are starting a new history. */
    memcpy(server.replid, server.master->replid, sizeof(server.replid));
    server.master_repl_offset = server.master->reploff;
    clearReplicationId2();

    /* Let's create the replication backlog if needed. Slaves need to
     * accumulate the backlog regardless of the fact they have sub-slaves
     * or not, in order to behave correctly if they are promoted to
     * masters after a failover. */
    if (server.repl_backlog == NULL) createReplicationBacklog();
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Finished with success");

    if (server.supervised_mode == SUPERVISED_SYSTEMD) {
        redisCommunicateSystemd(
                "STATUS=MASTER <-> REPLICA sync: Finished with success. Ready to accept connections in read-write mode.\n");
    }

    /* Send the initial ACK immediately to put this replica in online state. */
    if (usemark) replicationSendAck();

    /* Restart the AOF subsystem now that we finished the sync. This
     * will trigger an AOF rewrite, and when done will start appending
     * to the new file. */
    if (server.aof_enabled) restartAOFAfterSYNC();
    return;

    error:
    cancelReplicationHandshake(1);
    return;
}

/**
 * 同步读取响应
 * @param conn
 * @return
 */
char *receiveSynchronousResponse(connection *conn) {
    char buf[256];
    /* Read the reply from the server. */
    // 超时时间内读取主节点的响应
    if (connSyncReadLine(conn, buf, sizeof(buf), server.repl_syncio_timeout * 1000) == -1) {
        return sdscatprintf(sdsempty(), "-Reading from master: %s",
                            strerror(errno));
    }

    // 更新主从复制过程，从主节点读取的时间
    server.repl_transfer_lastio = server.unixtime;
    return sdsnew(buf);
}

/* Send a pre-formatted multi-bulk command to the connection. */
char *sendCommandRaw(connection *conn, sds cmd) {
    if (connSyncWrite(conn, cmd, sdslen(cmd), server.repl_syncio_timeout * 1000) == -1) {
        return sdscatprintf(sdsempty(), "-Writing to master: %s",
                            connGetLastError(conn));
    }
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection.
 * Used to send AUTH and REPLCONF commands to the master before starting the
 * replication.
 *
 * Takes a list of char* arguments, terminated by a NULL argument.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendCommand(connection *conn, ...) {
    va_list ap;
    sds cmd = sdsempty();
    sds cmdargs = sdsempty();
    size_t argslen = 0;
    char *arg;

    /* Create the command to send to the master, we use redis binary
     * protocol to make sure correct arguments are sent. This function
     * is not safe for all binary data. */
    va_start(ap, conn);
    while (1) {
        arg = va_arg(ap, char*);
        if (arg == NULL) break;
        cmdargs = sdscatprintf(cmdargs, "$%zu\r\n%s\r\n", strlen(arg), arg);
        argslen++;
    }

    cmd = sdscatprintf(cmd, "*%zu\r\n", argslen);
    cmd = sdscatsds(cmd, cmdargs);
    sdsfree(cmdargs);

    va_end(ap);
    char *err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if (err)
        return err;
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection. 
 * Used to send AUTH and REPLCONF commands to the master before starting the
 * replication.
 *
 * argv_lens is optional, when NULL, strlen is used.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendCommandArgv(connection *conn, int argc, char **argv, size_t *argv_lens) {
    sds cmd = sdsempty();
    char *arg;
    int i;

    /* Create the command to send to the master. */
    cmd = sdscatfmt(cmd, "*%i\r\n", argc);
    for (i = 0; i < argc; i++) {
        int len;
        arg = argv[i];
        len = argv_lens ? argv_lens[i] : strlen(arg);
        cmd = sdscatfmt(cmd, "$%i\r\n", len);
        cmd = sdscatlen(cmd, arg, len);
        cmd = sdscatlen(cmd, "\r\n", 2);
    }
    char *err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if (err)
        return err;
    return NULL;
}


#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
/* Try a partial resynchronization with the master if we are about to reconnect.
 * 在重连接之后，尝试进行部分重同步。
 *
 *
 * If there is no cached master structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the master replid and the master replication
 * global offset.
 *
 * 如果 master 缓存为空，那么通过 "PSYNC ? -1" 命令来触发一次 full resync ，
 * 让主服务器的 run id 和复制偏移量可以传到附属节点里面。
 *
 * This function is designed to be called from syncWithMaster(), so the
 * following assumptions are made:
 * 这个函数由 syncWithMaster() 函数调用，它做了以下假设：
 *
 * 1) We pass the function an already connected socket "fd".
 *    一个已连接套接字 fd 会被传入函数
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the server.master client structure.
 *    函数不会关闭 fd 。
 *    当部分同步成功时，函数会将 fd 用作 server.master 客户端结构中的文件描述符。
 *
 * The function is split in two halves: if read_reply is 0, the function
 * writes the PSYNC command on the socket, and a new function call is
 * needed, with read_reply set to 1, in order to read the reply of the
 * command. This is useful in order to support non blocking operations, so
 * that we write, return into the event loop, and read when there are data.
 *
 * When read_reply is 0 the function returns PSYNC_WRITE_ERR if there
 * was a write error, or PSYNC_WAIT_REPLY to signal we need another call
 * with read_reply set to 1. However even when read_reply is set to 1
 * the function may return PSYNC_WAIT_REPLY again to signal there were
 * insufficient data to read to complete its work. We should re-enter
 * into the event loop and wait in such a case.
 *
 * The function returns:
 *
 * PSYNC_CONTINUE: If the PSYNC command succeeded and we can continue.
 *                  PSYNC 命令成功，可以继续。
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the master replid and global replication
 *                   offset is saved.
 *                    主服务器支持 PSYNC 功能，但目前情况需要执行 full resync 。
 *                    在这种情况下， run_id 和全局复制偏移量会被保存。
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 *                      主服务器不支持 PSYNC ，调用者应该下降到 SYNC 命令。
 *
 * PSYNC_WRITE_ERROR: There was an error writing the command to the socket.
 *                    将命令写入套接字时出错。
 * PSYNC_WAIT_REPLY: Call again the function with read_reply set to 1.
 *                   再次调用 read_reply 设置为 1 的函数。
 * PSYNC_TRY_LATER: Master is currently in a transient error condition.
 *                  主节点当前处于瞬态错误状态。
 * Notable side effects:
 *
 * 1) As a side effect of the function call the function removes the readable
 *    event handler from "fd", unless the return value is PSYNC_WAIT_REPLY.
 * 2) server.master_initial_offset is set to the right value according
 *    to the master reply. This will be used to populate the 'server.master'
 *    structure replication offset.
 */

/*
 * 从库向主库发送数据同步的命令。
 *
 * 主库收到命令后，会根据从库发送的主库 ID、复制进度值 offset，来判断是进行全量复制还是增量复制，或者是返回错误。
 *
 * 同步执行
 *
 * todo PSYNC 命令发送失败或者收到响应，会移除 conn 上的 syncWithMaster 函数，因为接下来的复制可以通过命令传播或者补发（也算增量复制），而不需要 psync 命令了，
 * 除非又重新触发 psync 整个过程
 */
int slaveTryPartialResynchronization(connection *conn, int read_reply) {
    char *psync_replid;
    char psync_offset[32];
    sds reply;

    /* Writing half */
    // 发送 PSYNC 命令
    if (!read_reply) {
        /* Initially set master_initial_offset to -1 to mark the current
         * master replid and offset as not valid. Later if we'll be able to do
         * a FULL resync using the PSYNC command we'll set the offset at the
         * right value, so that this information will be propagated to the
         * client structure representing the master into server.master. */
        // 从库第一次和主库同步时，设置 offset 为 -1
        server.master_initial_offset = -1;

        // 缓存存在，尝试部分重同步
        // 命令为 "PSYNC <master_run_id> <repl_offset>"
        if (server.cached_master) {
            psync_replid = server.cached_master->replid;
            snprintf(psync_offset, sizeof(psync_offset), "%lld", server.cached_master->reploff + 1);
            serverLog(LL_NOTICE, "Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);

            // 缓存不存在
            // 发送 "PSYNC ? -1" ，要求完整重同步
        } else {
            serverLog(LL_NOTICE, "Partial resynchronization not possible (no cached master)");
            psync_replid = "?";
            memcpy(psync_offset, "-1", 3);
        }

        /* Issue the PSYNC command, if this is a master with a failover in
         * progress then send the failover argument to the replica to cause it
         * to become a master */
        // 向主服务器发出 PSYNC 命令
        // 如果这是一个正在进行故障转移的主服务器，则将故障转移参数发送到副本以使其成为主服务器
        if (server.failover_state == FAILOVER_IN_PROGRESS) {
            reply = sendCommand(conn, "PSYNC", psync_replid, psync_offset, "FAILOVER", NULL);
        } else {
            reply = sendCommand(conn, "PSYNC", psync_replid, psync_offset, NULL);
        }

        /*todo 主节点有对应的函数处理 PSYNC 命令，在那里会执行增量复制和全量复制，即将数据响应给当前从节点*/

        /*发送完命令给主服务器，就返回了 */

        if (reply != NULL) {
            serverLog(LL_WARNING, "Unable to send PSYNC to master: %s", reply);
            sdsfree(reply);
            // 清除读处理函数
            connSetReadHandler(conn, NULL);
            return PSYNC_WRITE_ERROR;
        }

        return PSYNC_WAIT_REPLY;
    }

    /* Reading half */
    // todo 同步读取主库的 PSYNC 的响应；注意，主节点一般先返回响应（FULLRESYNC、CONTINUE)，紧接着返回数据；
    reply = receiveSynchronousResponse(conn);

    // 响应为空，有两种情况：1）主节点发送的保活数据包 /n  2) 主节点针对当前从节点的全量复制不能共享 RDB，要等待下一个 bgsave 才会把数据发送过来；
    if (sdslen(reply) == 0) {
        /* The master may send empty newlines after it receives PSYNC
         * and before to reply, just to keep the connection alive. */
        sdsfree(reply);

        // todo 主节点可能会在收到 PSYNC 之后和回复之前发送空换行符，以保持连接处于活动状态。
        return PSYNC_WAIT_REPLY;
    }

    // todo 移除读处理函数，后续主节点发送的数据不能再通过 syncWithMaster 处理器读取
    connSetReadHandler(conn, NULL);

    /* 根据主库的响应，有以下几种情况 */

    // 主库返回 FULLRESYNC ，全量复制
    if (!strncmp(reply, "+FULLRESYNC", 11)) {
        char *replid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the replid
         * and the replication offset.
         *
         * todo FULL RESYNC，解析回复以提取replid和复制偏移量。
         */
        replid = strchr(reply, ' ');
        if (replid) {
            replid++;
            offset = strchr(replid, ' ');
            if (offset) offset++;
        }
        if (!replid || !offset || (offset - replid - 1) != CONFIG_RUN_ID_SIZE) {
            serverLog(LL_WARNING,
                      "Master replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the master supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the master
             * replid to make sure next PSYNCs will fail. */
            memset(server.master_replid, 0, CONFIG_RUN_ID_SIZE + 1);
        } else {
            memcpy(server.master_replid, replid, offset - replid - 1);
            server.master_replid[CONFIG_RUN_ID_SIZE] = '\0';
            server.master_initial_offset = strtoll(offset, NULL, 10);
            serverLog(LL_NOTICE, "Full resync from master: %s:%lld",
                      server.master_replid,
                      server.master_initial_offset);
        }
        /* We are going to full resync, discard the cached master structure. */
        // 要开始全量同步，缓存中的 master 已经没用了，清除它
        replicationDiscardCachedMaster();
        sdsfree(reply);


        return PSYNC_FULLRESYNC;
    }

    //主库返回CONTINUE，执行增量复制
    if (!strncmp(reply, "+CONTINUE", 9)) {
        /* Partial resync was accepted. */
        serverLog(LL_NOTICE,
                  "Successful partial resynchronization with master.");

        /* Check the new replication ID advertised by the master. If it
         * changed, we need to set the new ID as primary ID, and set or
         * secondary ID as the old master ID up to the current offset, so
         * that our sub-slaves will be able to PSYNC with us after a
         * disconnection. */
        char *start = reply + 10;
        char *end = reply + 9;
        while (end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end - start == CONFIG_RUN_ID_SIZE) {
            char new[CONFIG_RUN_ID_SIZE + 1];
            memcpy(new, start, CONFIG_RUN_ID_SIZE);
            new[CONFIG_RUN_ID_SIZE] = '\0';

            // 判断复制的主节点信息
            if (strcmp(new, server.cached_master->replid)) {
                /* Master ID changed. */
                serverLog(LL_WARNING, "Master replication ID changed to %s", new);

                /* Set the old ID as our ID2, up to the current offset+1. */
                memcpy(server.replid2, server.cached_master->replid,
                       sizeof(server.replid2));
                server.second_replid_offset = server.master_repl_offset + 1;

                /* Update the cached master ID and our own primary ID to the
                 * new one. */
                memcpy(server.replid, new, sizeof(server.replid));
                memcpy(server.cached_master->replid, new, sizeof(server.replid));

                /* Disconnect all the sub-slaves: they need to be notified. */
                disconnectSlaves();
            }
        }

        /* Setup the replication to continue. */
        sdsfree(reply);
        // 将缓存中的 master 设为当前 master
        replicationResurrectCachedMaster(conn);

        /* If this instance was restarted and we read the metadata to
         * PSYNC from the persistence file, our replication backlog could
         * be still not initialized. Create it. */
        // 如果此实例重新启动并且我们从持久性文件中将元数据读取到 PSYNC，我们的复制积压可能仍未初始化。
        if (server.repl_backlog == NULL) createReplicationBacklog();

        return PSYNC_CONTINUE;
    }

    /* If we reach this point we received either an error (since the master does
     * not understand PSYNC or because it is in a special state and cannot
     * serve our request), or an unexpected reply from the master.
     *
     * Return PSYNC_NOT_SUPPORTED on errors we don't understand, otherwise
     * return PSYNC_TRY_LATER if we believe this is a transient error. */

    // 从节点收到错误
    if (!strncmp(reply, "-NOMASTERLINK", 13) ||
        !strncmp(reply, "-LOADING", 8)) {
        serverLog(LL_NOTICE,
                  "Master is currently unable to PSYNC "
                  "but should be in the future: %s", reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    // 主库返回错误信息
    if (strncmp(reply, "-ERR", 4)) {
        /* If it's not an error, log the unexpected event. */
        serverLog(LL_WARNING,
                  "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        serverLog(LL_NOTICE,
                  "Master does not support PSYNC or is in "
                  "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    replicationDiscardCachedMaster();
    return PSYNC_NOT_SUPPORTED;
}

/* This handler fires when the non blocking connect was able to
 * establish a connection with the master.
 *
 * 当非阻塞连接能够与主服务器建立连接时，将触发此处理程序。
 *
 * todo 专门处理复制工作的函数，如接收 RDB 文件、接收传播的写命令。
 * todo 主节点没有特别处理，对于主节点来说，从节点连接到它就是一个客户端，它只需要被动接受即可。
 * todo 主节点发送的 PING 和 \n 消息，以及从节点发给主节点的 ACK 消息，会驱动该函数不断执行
 */
void syncWithMaster(connection *conn) {
    char tmpfile[256], *err = NULL;
    int dfd = -1, maxtries = 5;
    int psync_result;

    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP. */
    // 1 如果处于 SLAVEOF NO ONE 模式，那么关闭 fd，因此此时非主从环境
    if (server.repl_state == REPL_STATE_NONE) {
        connClose(conn);
        return;
    }

    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state. */
    // 检查套接字的状态
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING, "Error condition on socket for SYNC: %s",
                  connGetLastError(conn));
        goto error;
    }

    /* Send a PING to check the master is able to reply without errors. */
    // 2 发送一个 PING 命令给主节点，确认主节点可以访问
    if (server.repl_state == REPL_STATE_CONNECTING) {
        serverLog(LL_NOTICE, "Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        // 删除可写事件，以便可读事件保持注册状态，我们可以等待 PONG 等回复。
        // todo 可以看到，当连接中有读事件，也是交给 syncWithMaster 函数处理的
        connSetReadHandler(conn, syncWithMaster);
        // todo 删除可写事件，接下来在没有关联写事件前，套接字的写不会触发任何函数，也就是读取的全部由 syncWithMaster 函数处理
        connSetWriteHandler(conn, NULL);

        // 更新状态为 REPL_STATE_RECEIVE_PING_REPLY
        server.repl_state = REPL_STATE_RECEIVE_PING_REPLY;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        // 发送 PING
        err = sendCommand(conn, "PING", NULL);
        if (err) goto write_error;

        // 返回，等待主节点 PONG 到达
        return;
    }

    /* Receive the PONG command. */
    // 3 接收 PONG 命令
    if (server.repl_state == REPL_STATE_RECEIVE_PING_REPLY) {
        // 尝试在指定时间限制内同步读取 PONG
        err = receiveSynchronousResponse(conn);
        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        // 接收到的数据只有两种可能：
        // 第一种是 +PONG ，第二种是因为未验证而出现的 -NOAUTH 错误
        if (err[0] != '+' &&
            strncmp(err, "-NOAUTH", 7) != 0 &&
            strncmp(err, "-NOPERM", 7) != 0 &&
            strncmp(err, "-ERR operation not permitted", 28) != 0) {
            serverLog(LL_WARNING, "Error reply to PING from master: '%s'", err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE,
                      "Master replied to PING, replication can continue...");
        }
        sdsfree(err);
        err = NULL;

        // 更新状态为 REPL_STATE_SEND_HANDSHAKE
        server.repl_state = REPL_STATE_SEND_HANDSHAKE;
    }

    // 4 进行身份认证等操作
    if (server.repl_state == REPL_STATE_SEND_HANDSHAKE) {
        /* AUTH with the master if required. */
        if (server.masterauth) {
            char *args[3] = {"AUTH", NULL, NULL};
            size_t lens[3] = {4, 0, 0};
            int argc = 1;
            if (server.masteruser) {
                args[argc] = server.masteruser;
                lens[argc] = strlen(server.masteruser);
                argc++;
            }
            args[argc] = server.masterauth;
            lens[argc] = sdslen(server.masterauth);
            argc++;
            // 向主节点发送身份认证信息
            err = sendCommandArgv(conn, argc, args, lens);
            if (err) goto write_error;
        }

        /* Set the slave port, so that Master's INFO command can list the
         * slave listening port correctly. */
        {
            int port;
            if (server.slave_announce_port)
                port = server.slave_announce_port;
            else if (server.tls_replication && server.tls_port)
                port = server.tls_port;
            else
                port = server.port;
            sds portstr = sdsfromlonglong(port);
            // 向主节点发送监听端口信息
            err = sendCommand(conn, "REPLCONF",
                              "listening-port", portstr, NULL);
            sdsfree(portstr);
            if (err) goto write_error;
        }

        /* Set the slave ip, so that Master's INFO command can list the
         * slave IP address port correctly in case of port forwarding or NAT.
         * Skip REPLCONF ip-address if there is no slave-announce-ip option set. */
        if (server.slave_announce_ip) {
            // 向主节点发送 ip
            err = sendCommand(conn, "REPLCONF",
                              "ip-address", server.slave_announce_ip, NULL);
            if (err) goto write_error;
        }

        /* Inform the master of our (slave) capabilities.
         * todo 通知主节点，当前从节点的能力
         *
         * EOF: supports EOF-style RDB transfer for diskless replication.
         * 支持 EOF 样式的 RDB 传输以进行无盘复制。即 todo 是否支持无盘复制
         *
         * PSYNC2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
         * todo 是否支持 PSYNC2
         *
         * The master will ignore capabilities it does not understand.
         * 主节点会忽略它不理解的能力。
         */
        err = sendCommand(conn, "REPLCONF",
                          "capa", "eof", "capa", "psync2", NULL);
        if (err) goto write_error;

        /* todo 发送 REPLCONF xxx 命令到主节点，主节点的有对应命令函数去处理，根据不同的 xxx 选项参数进行分别处理，并返回给当前从节点*/

        // 更新状态为 REPL_STATE_RECEIVE_AUTH_REPLY
        server.repl_state = REPL_STATE_RECEIVE_AUTH_REPLY;
        return;
    }


    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY && !server.masterauth)
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;

    /* Receive AUTH reply. */
    // 5 收到认证响应
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH_REPLY) {
        // 同步读取主节点响应结果
        err = receiveSynchronousResponse(conn);
        if (err[0] == '-') {
            serverLog(LL_WARNING, "Unable to AUTH to MASTER: %s", err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        err = NULL;

        // 更新状态为 REPL_STATE_RECEIVE_PORT_REPLY
        server.repl_state = REPL_STATE_RECEIVE_PORT_REPLY;
        return;
    }

    /* Receive REPLCONF listening-port reply. */
    // 6 从服务器收到它发给主服务器端口的回复
    if (server.repl_state == REPL_STATE_RECEIVE_PORT_REPLY) {
        // 同步读取
        err = receiveSynchronousResponse(conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE, "(Non critical) Master does not understand "
                                 "REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
        // 更新状态 REPL_STATE_RECEIVE_IP_REPLY
        server.repl_state = REPL_STATE_RECEIVE_IP_REPLY;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY && !server.slave_announce_ip)
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;

    /* Receive REPLCONF ip-address reply. */
    // 7 从服务器收到 ip 的回复
    if (server.repl_state == REPL_STATE_RECEIVE_IP_REPLY) {
        // 同步读取
        err = receiveSynchronousResponse(conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE, "(Non critical) Master does not understand "
                                 "REPLCONF ip-address: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;
        return;
    }

    /* Receive CAPA reply. */
    // 8 从服务器收到 capa 回复
    if (server.repl_state == REPL_STATE_RECEIVE_CAPA_REPLY) {
        // 同步读取
        err = receiveSynchronousResponse(conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF capa. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE, "(Non critical) Master does not understand "
                                 "REPLCONF capa: %s", err);
        }
        sdsfree(err);
        err = NULL;

        // 更新状态为 REPL_STATE_SEND_PSYNC，该状态就可以向主节点发送 PSYNC 了，请求复制。其他状态不能
        server.repl_state = REPL_STATE_SEND_PSYNC;
    }

    /* Try a partial resynchonization. If we don't have a cached master
     * slaveTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the master replid
     * and the global offset, to try a partial resync at the next
     * reconnection attempt. */
    // 9 如果从服务器状态为 REPL_STATE_SEND_PSYNC ，那么可以向主服务器发送 PSYNC 命令，请求复制
    // todo 重要
    if (server.repl_state == REPL_STATE_SEND_PSYNC) {
        // 向主服务器发送 PSYNC 命令（参数 0 区分），不等待响应结果
        if (slaveTryPartialResynchronization(conn, 0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            abortFailover("Write error to failover target");
            goto write_error;
        }

        // 更新状态为 REPL_STATE_RECEIVE_PSYNC_REPLY
        server.repl_state = REPL_STATE_RECEIVE_PSYNC_REPLY;

        // 发送 PSYNC 后返回
        return;
    }

    /* If reached this point, we should be in REPL_STATE_RECEIVE_PSYNC. */
    // 执行到这里必须是 REPL_STATE_RECEIVE_PSYNC_REPLY 状态，否则表示出错
    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC_REPLY) {
        serverLog(LL_WARNING, "syncWithMaster(): state machine error, "
                              "state should be RECEIVE_PSYNC but is %d",
                  server.repl_state);
        goto error;
    }


    // 10 读取主服务器的 PSYNC 命令的响应(1 区分）
    // todo 阻塞等待主节点对应 PSYNC 的响应结果，也就是响应码（FULLRESYNC、CONTINUE)，然后移除读处理函数，
    //  后续主节点发送的数据不能再通过 syncWithMaster 处理器读取，也就是后续通过命令传播了，除非有问题再尝试非命令传播的方式
    psync_result = slaveTryPartialResynchronization(conn, 1);

    // todo 忽略主节点用于和从节点保持连接处于活动状态的响应 \n
    if (psync_result == PSYNC_WAIT_REPLY) return; /* Try again later... */

    /* Check the status of the planned failover. We expect PSYNC_CONTINUE,
     * but there is nothing technically wrong with a full resync which
     * could happen in edge cases. */
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        if (psync_result == PSYNC_CONTINUE || psync_result == PSYNC_FULLRESYNC) {
            clearFailoverState();
        } else {
            abortFailover("Failover target rejected psync request");
            return;
        }
    }

    /* If the master is in an transient error, we should try to PSYNC
     * from scratch later, so go to the error path. This happens when
     * the server is loading the dataset or is not connected with its
     * master and so forth. */
    if (psync_result == PSYNC_TRY_LATER) goto error;

    /* Note: if PSYNC does not return WAIT_REPLY, it will take care of
     * uninstalling the read handler from the file descriptor. */

    // 10.1 主节点响应，执行增量复制
    // todo 主节点会将复制积压缓冲区中对应的数据发给当前从节点，主节点此时就是从节点的一个客户端，从节点接受增量数据（命令数据）即可
    // todo 相当于客户端执行写命令
    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Master accepted a Partial Resynchronization.");
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            redisCommunicateSystemd(
                    "STATUS=MASTER <-> REPLICA sync: Partial Resynchronization accepted. Ready to accept connections in read-write mode.\n");
        }
        // 返回
        return;
    }

    // 如果执行到这里，那么 psync_result == PSYNC_FULLRESYNC 或 PSYNC_NOT_SUPPORTED


    /* PSYNC failed or is not supported: we want our slaves to resync with us
     * as well, if we have any sub-slaves. The master may transfer us an
     * entirely different data set and we have no way to incrementally feed
     * our slaves after that. */
    disconnectSlaves(); /* Force our slaves to resync with us as well. */
    freeReplicationBacklog(); /* Don't allow our chained slaves to PSYNC. */

    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the server.master_replid and master_initial_offset are
     * already populated. */
    // 10.2 主服务器不支持 PSYNC ，发送 SYNC
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE, "Retrying with SYNC...");
        // 向主服务器发送 SYNC 命令
        if (connSyncWrite(conn, "SYNC\r\n", 6, server.repl_syncio_timeout * 1000) == -1) {
            serverLog(LL_WARNING, "I/O error writing to MASTER: %s",
                      strerror(errno));
            goto error;
        }
    }

    /* Prepare a suitable temp file for bulk transfer */
    // 10.3 全量复制
    // 打开一个临时文件，用于写入和保存接下来从主服务器传来的 RDB 文件数据
    if (!useDisklessLoad()) {
        while (maxtries--) {
            snprintf(tmpfile, 256,
                     "temp-%d.%ld.rdb", (int) server.unixtime, (long int) getpid());
            dfd = open(tmpfile, O_CREAT | O_WRONLY | O_EXCL, 0644);
            if (dfd != -1) break;
            sleep(1);
        }
        if (dfd == -1) {
            serverLog(LL_WARNING, "Opening the temp file needed for MASTER <-> REPLICA synchronization: %s",
                      strerror(errno));
            goto error;
        }

        // 设置临时文件名
        server.repl_transfer_tmpfile = zstrdup(tmpfile);
        server.repl_transfer_fd = dfd;
    }

    /* Setup the non blocking download of the bulk file. */
    // todo 如果执行全量复制的话，创建 readSyncBulkPayload 回调函数，用于读取主服务接下来返回的 RDB 文件
    if (connSetReadHandler(conn, readSyncBulkPayload) == C_ERR) {
        char conninfo[CONN_INFO_LEN];
        serverLog(LL_WARNING,
                  "Can't create readable event for SYNC: %s (%s)",
                  strerror(errno), connGetInfo(conn, conninfo, sizeof(conninfo)));
        goto error;
    }

    // todo 将从库状态设置为 REPL_STATE_TRANSFER，表示开 始进行实际的数据同步，比如主库把 RDB 文件传输给从库。
    server.repl_state = REPL_STATE_TRANSFER;

    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_lastio = server.unixtime;
    return;

    // 11 出错
    error:
    if (dfd != -1) close(dfd);
    connClose(conn);
    server.repl_transfer_s = NULL;
    if (server.repl_transfer_fd != -1)
        close(server.repl_transfer_fd);
    if (server.repl_transfer_tmpfile)
        zfree(server.repl_transfer_tmpfile);
    server.repl_transfer_tmpfile = NULL;
    server.repl_transfer_fd = -1;

    // 出错，重置状态为 REPL_STATE_CONNECT
    server.repl_state = REPL_STATE_CONNECT;
    return;

    write_error: /* Handle sendCommand() errors. */
    serverLog(LL_WARNING, "Sending command to master in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}

/**
 * 连接主服务器
 * @return
 */
int connectWithMaster(void) {
    // 初始化 connection
    server.repl_transfer_s = server.tls_replication ? connCreateTLS() : connCreateSocket();

    // 连接主服务器
    // 监听连接套接字的读写事件，使用 syncWithMaster 函数处理
    if (connConnect(server.repl_transfer_s, server.masterhost, server.masterport,
                    NET_FIRST_BIND_ADDR, syncWithMaster) == C_ERR) {

        serverLog(LL_WARNING, "Unable to connect to MASTER: %s",
                  connGetLastError(server.repl_transfer_s));
        connClose(server.repl_transfer_s);
        server.repl_transfer_s = NULL;
        return C_ERR;
    }


    // 初始化统计变量
    server.repl_transfer_lastio = server.unixtime;

    // todo 将状态改为已连接
    server.repl_state = REPL_STATE_CONNECTING;
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync started");
    return C_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void undoConnectWithMaster(void) {
    connClose(server.repl_transfer_s);
    server.repl_transfer_s = NULL;
}

/* Abort the async download of the bulk dataset while SYNC-ing with master.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void replicationAbortSyncTransfer(void) {
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    undoConnectWithMaster();
    if (server.repl_transfer_fd != -1) {
        close(server.repl_transfer_fd);
        bg_unlink(server.repl_transfer_tmpfile);
        zfree(server.repl_transfer_tmpfile);
        server.repl_transfer_tmpfile = NULL;
        server.repl_transfer_fd = -1;
    }
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REPL_STATE_CONNECT.
 *
 * Otherwise zero is returned and no operation is performed at all. */
int cancelReplicationHandshake(int reconnect) {

    if (server.repl_state == REPL_STATE_TRANSFER) {
        // 清理复制过程数据
        replicationAbortSyncTransfer();

        // 重置复制状态
        server.repl_state = REPL_STATE_CONNECT;

    } else if (server.repl_state == REPL_STATE_CONNECTING ||
               slaveIsInHandshakeState()) {
        undoConnectWithMaster();
        server.repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }

    if (!reconnect)
        return 1;

    /* try to re-connect without waiting for replicationCron, this is needed
     * for the "diskless loading short read" test. */
    serverLog(LL_NOTICE, "Reconnecting to MASTER %s:%d after failure",
              server.masterhost, server.masterport);
    connectWithMaster();

    return 1;
}

/**
 * Set replication to the specified master address and port.
 *
 * 将当前服务节点设置为指定地址的从服务器
 *
 * @param ip 目标主节点的 IP
 * @param port 目标主节点的 PORT
 */
void replicationSetMaster(char *ip, int port) {
    // 根据是否保存了服务器地址判断当前服务节点是否是主节点
    int was_master = server.masterhost == NULL;

    // 清除当前服务节点原有的主服务器地址（如果有的话）
    sdsfree(server.masterhost);
    server.masterhost = NULL;

    // 清理用来和主库连接的客户端（如果当前是从节点的话）
    if (server.master) {
        freeClient(server.master);
    }
    disconnectAllBlockedClients(); /* Clients blocked in master, now slave. */

    /* Setting masterhost only after the call to freeClient since it calls
     * replicationHandleMasterDisconnection which can trigger a re-connect
     * directly from within that call. */
    // 为当前服务节点设置主节点的 IP 和 PORT
    server.masterhost = sdsnew(ip);
    server.masterport = port;

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Force our slaves to resync with us as well.
     * 强制所有从服务器执行重同步
     *
     * They may hopefully be able to partially resync with us, but we can notify the replid change. */
    // 断开所有从服务器的连接（如果当前是主节点并且有从节点）
    disconnectSlaves();

    // 重置复制状态机（如果之前有的情况下）
    cancelReplicationHandshake(0);

    /* Before destroying our master state, create a cached master using
     * our own parameters, to later PSYNC with the new master. */
    // 如果是主库，那么要进行处理，因为接下来它要成为从库了
    if (was_master) {
        // 清空可能有的 master 缓存，因为已经不会执行 PSYNC 了
        replicationDiscardCachedMaster();
        // 释放 backlog 等，同理 PSYNC 目前已经不会执行了
        replicationCacheMasterUsingMyself();
    }

    /* Fire the role change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA,
                          NULL);

    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    // todo 设置复制状态为 REPL_STATE_CONNECT，进入复制状态
    server.repl_state = REPL_STATE_CONNECT;
    serverLog(LL_NOTICE, "Connecting to MASTER %s:%d",
              server.masterhost, server.masterport);

    // todo 尝试连接主节点，连接成功后状态会变成 REPL_STATE_CONNECTING
    connectWithMaster();
}

/* Cancel replication, setting the instance as a master itself. */
void replicationUnsetMaster(void) {
    if (server.masterhost == NULL) return; /* Nothing to do. */

    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    /* Clear masterhost first, since the freeClient calls
     * replicationHandleMasterDisconnection which can attempt to re-connect. */
    sdsfree(server.masterhost);
    server.masterhost = NULL;
    if (server.master) freeClient(server.master);
    replicationDiscardCachedMaster();
    cancelReplicationHandshake(0);
    /* When a slave is turned into a master, the current replication ID
     * (that was inherited from the master at synchronization time) is
     * used as secondary ID up to the current offset, and a new replication
     * ID is created to continue with a new replication history.
     *
     * NOTE: this function MUST be called after we call
     * freeClient(server.master), since there we adjust the replication
     * offset trimming the final PINGs. See Github issue #7320. */
    shiftReplicationId();
    /* Disconnecting all the slaves is required: we need to inform slaves
     * of the replication ID change (see shiftReplicationId() call). However
     * the slaves will be able to partially resync with us, so it will be
     * a very fast reconnection. */
    disconnectSlaves();
    server.repl_state = REPL_STATE_NONE;

    /* We need to make sure the new master will start the replication stream
     * with a SELECT statement. This is forced after a full resync, but
     * with PSYNC version 2, there is no need for full resync after a
     * master switch. */
    server.slaveseldb = -1;

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Once we turn from slave to master, we consider the starting time without
     * slaves (that is used to count the replication backlog time to live) as
     * starting from now. Otherwise the backlog will be freed after a
     * failover if slaves do not connect immediately. */
    server.repl_no_slaves_since = server.unixtime;

    /* Reset down time so it'll be ready for when we turn into replica again. */
    server.repl_down_since = 0;

    /* Fire the role change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER,
                          NULL);

    /* Restart the AOF subsystem in case we shut it down during a sync when
     * we were still a slave. */
    if (server.aof_enabled && server.aof_state == AOF_OFF) restartAOFAfterSYNC();
}

/* This function is called when the slave lose the connection with the
 * master into an unexpected way. */
void replicationHandleMasterDisconnection(void) {
    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    server.master = NULL;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unixtime;
    /* We lost connection with our master, don't disconnect slaves yet,
     * maybe we'll be able to PSYNC with our master later. We'll disconnect
     * the slaves only if we'll have to do a full resync with our master. */

    /* Try to re-connect immediately rather than wait for replicationCron
     * waiting 1 second may risk backlog being recycled. */
    if (server.masterhost) {
        serverLog(LL_NOTICE, "Reconnecting to MASTER %s:%d",
                  server.masterhost, server.masterport);
        connectWithMaster();
    }
}

/*
 * replicaofCommand 命令处理函数
 */
void replicaofCommand(client *c) {
    /* SLAVEOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the master node. */
    if (server.cluster_enabled) {
        addReplyError(c, "REPLICAOF not allowed in cluster mode.");
        return;
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c, "REPLICAOF not allowed while failing over.");
        return;
    }

    /* The special host/port combination "NO" "ONE" turns the instance
     * into a master. Otherwise the new master address is set. */
    if (!strcasecmp(c->argv[1]->ptr, "no") &&
        !strcasecmp(c->argv[2]->ptr, "one")) {
        if (server.masterhost) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(), c);
            serverLog(LL_NOTICE, "MASTER MODE enabled (user request from '%s')",
                      client);
            sdsfree(client);
        }
    } else {
        long port;

        if (c->flags & CLIENT_SLAVE) {
            /* If a client is already a replica they cannot run this command,
             * because it involves flushing all replicas (including this
             * client) */
            addReplyError(c, "Command is not valid when client is a replica.");
            return;
        }

        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != C_OK))
            return;

        /* Check if we are already attached to the specified master */
        if (server.masterhost && !strcasecmp(server.masterhost, c->argv[1]->ptr)
            && server.masterport == port) {
            serverLog(LL_NOTICE, "REPLICAOF would result into synchronization "
                                 "with the master we are already connected "
                                 "with. No operation performed.");
            addReplySds(c, sdsnew("+OK Already connected to specified "
                                  "master\r\n"));
            return;
        }


        /* There was no previous master or the user specified a different one,
         * we can continue. */
        // todo 设置从节点复制初始状态 REPL_STATE_CONNECT;内部会尝试和主节点建立连接，并为连接关联通道处理器 syncWithMaster
        replicationSetMaster(c->argv[1]->ptr, port);

        sds client = catClientInfoString(sdsempty(), c);
        serverLog(LL_NOTICE, "REPLICAOF %s:%d enabled (user request from '%s')",
                  server.masterhost, server.masterport, client);
        sdsfree(client);
    }
    addReply(c, shared.ok);
}

/* ROLE command: provide information about the role of the instance
 * (master or slave) and additional information related to replication
 * in an easy to process format. */
void roleCommand(client *c) {
    if (server.masterhost == NULL) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int slaves = 0;

        addReplyArrayLen(c, 3);
        addReplyBulkCBuffer(c, "master", 6);
        addReplyLongLong(c, server.master_repl_offset);
        mbcount = addReplyDeferredLen(c);
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;
            char ip[NET_IP_STR_LEN], *slaveaddr = slave->slave_addr;

            if (!slaveaddr) {
                if (connPeerToString(slave->conn, ip, sizeof(ip), NULL) == -1)
                    continue;
                slaveaddr = ip;
            }
            if (slave->replstate != SLAVE_STATE_ONLINE) continue;
            addReplyArrayLen(c, 3);
            addReplyBulkCString(c, slaveaddr);
            addReplyBulkLongLong(c, slave->slave_listening_port);
            addReplyBulkLongLong(c, slave->repl_ack_off);
            slaves++;
        }
        setDeferredArrayLen(c, mbcount, slaves);
    } else {
        char *slavestate = NULL;

        addReplyArrayLen(c, 5);
        addReplyBulkCBuffer(c, "slave", 5);
        addReplyBulkCString(c, server.masterhost);
        addReplyLongLong(c, server.masterport);
        if (slaveIsInHandshakeState()) {
            slavestate = "handshake";
        } else {
            switch (server.repl_state) {
                case REPL_STATE_NONE:
                    slavestate = "none";
                    break;
                case REPL_STATE_CONNECT:
                    slavestate = "connect";
                    break;
                case REPL_STATE_CONNECTING:
                    slavestate = "connecting";
                    break;
                case REPL_STATE_TRANSFER:
                    slavestate = "sync";
                    break;
                case REPL_STATE_CONNECTED:
                    slavestate = "connected";
                    break;
                default:
                    slavestate = "unknown";
                    break;
            }
        }
        addReplyBulkCString(c, slavestate);
        addReplyLongLong(c, server.master ? server.master->reploff : -1);
    }
}

/* Send a REPLCONF ACK command to the master to inform it about the current
 * processed offset. If we are not connected with a master, the command has
 * no effects.
 *
 * 向主设备发送一个 REPLCONF ACK 命令，通知它当前处理的偏移量（从节点的复制偏移量）
 *
 */
void replicationSendAck(void) {
    client *c = server.master;

    if (c != NULL) {
        c->flags |= CLIENT_MASTER_FORCE_REPLY;
        addReplyArrayLen(c, 3);
        addReplyBulkCString(c, "REPLCONF");
        addReplyBulkCString(c, "ACK");
        addReplyBulkLongLong(c, c->reploff);
        c->flags &= ~CLIENT_MASTER_FORCE_REPLY;
    }
}

/* ---------------------- MASTER CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our master's client structure after a transient disconnection.
 * It is cached into server.cached_master and flushed away using the following
 * functions. */

/* This function is called by freeClient() in order to cache the master
 * client structure instead of destroying it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached master are:
 *
 * replicationDiscardCachedMaster() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedMaster() that is used after a successful PSYNC
 * handshake in order to reactivate the cached master.
 */
void replicationCacheMaster(client *c) {
    serverAssert(server.master != NULL && server.cached_master == NULL);
    serverLog(LL_NOTICE, "Caching the disconnected master state.");

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the master client so that's ready to accept new commands:
     * we want to discard te non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the master. */
    sdsclear(server.master->querybuf);
    sdsclear(server.master->pending_querybuf);
    server.master->read_reploff = server.master->reploff;
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    server.cached_master = server.master;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }
    /* Invalidate the Sock Name cache. */
    if (c->sockname) {
        sdsfree(c->sockname);
        c->sockname = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    replicationHandleMasterDisconnection();
}

/* This function is called when a master is turend into a slave, in order to
 * create from scratch a cached master for the new client, that will allow
 * to PSYNC with the slave that was promoted as the new master after a
 * failover.
 *
 * Assuming this instance was previously the master instance of the new master,
 * the new master will accept its replication ID, and potentiall also the
 * current offset if no data was lost during the failover. So we use our
 * current replication ID and offset in order to synthesize a cached master. */
void replicationCacheMasterUsingMyself(void) {
    serverLog(LL_NOTICE,
              "Before turning into a replica, using my own master parameters "
              "to synthesize a cached master: I may be able to synchronize with "
              "the new master with just a partial transfer.");

    /* This will be used to populate the field server.master->reploff
     * by replicationCreateMasterClient(). We'll later set the created
     * master as server.cached_master, so the replica will use such
     * offset for PSYNC. */
    server.master_initial_offset = server.master_repl_offset;

    /* The master client we create can be set to any DBID, because
     * the new master will start its replication stream with SELECT. */
    replicationCreateMasterClient(NULL, -1);

    /* Use our own ID / offset. */
    memcpy(server.master->replid, server.replid, sizeof(server.replid));

    /* Set as cached master. */
    unlinkClient(server.master);
    server.cached_master = server.master;
    server.master = NULL;
}

/* Free a cached master, called when there are no longer the conditions for
 * a partial resync on reconnection. */
void replicationDiscardCachedMaster(void) {
    if (server.cached_master == NULL) return;

    serverLog(LL_NOTICE, "Discarding previously cached master state.");
    server.cached_master->flags &= ~CLIENT_MASTER;
    freeClient(server.cached_master);
    server.cached_master = NULL;
}

/* Turn the cached master into the current master, using the file descriptor
 * passed as argument as the socket for the new master.
 *
 * 将缓存中的 master 设置为从服务器的当前 master 。
 *
 * This function is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from were this
 * master left.
 *
 * 当增量复制准备就绪后，调用该函数。和 master 断开之前遗留下来的数据可以继续使用。
 *
 */
void replicationResurrectCachedMaster(connection *conn) {

    // 设置 master
    server.master = server.cached_master;
    server.cached_master = NULL;
    server.master->conn = conn;
    connSetPrivateData(server.master->conn, server.master);
    server.master->flags &= ~(CLIENT_CLOSE_AFTER_REPLY | CLIENT_CLOSE_ASAP);
    server.master->authenticated = 1;
    server.master->lastinteraction = server.unixtime;

    // todo 回到已连接状态
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* Re-add to the list of clients. */
    // 将主节点（这一个客户端）重新加入到从节点的客户端列表中
    linkClient(server.master);

    // 监听 master 的读事件
    if (connSetReadHandler(server.master->conn, readQueryFromClient)) {
        serverLog(LL_WARNING, "Error resurrecting the cached master, impossible to add the readable handler: %s",
                  strerror(errno));
        freeClientAsync(server.master); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    // 如果写缓冲区中有待处理的数据，我们可能还需要安装写处理程序 sendReplyToClient，以便于及时将写缓冲区的数据写出去
    if (clientHasPendingReplies(server.master)) {
        if (connSetWriteHandler(server.master->conn, sendReplyToClient)) {
            serverLog(LL_WARNING, "Error resurrecting the cached master, impossible to add the writable handler: %s",
                      strerror(errno));
            freeClientAsync(server.master); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * 计算那些延迟值少于等于 min-slaves-max-lag 的从服务器数量。
 *
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less).
 *
 * todo 如果服务器开启了 min-slaves-max-lag 选项，那么在这个选项所指定的条件达不到时，主服务器将拒绝写操作执行。
 */
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    // 主服务拒绝执行写操作的条件配置项，两都都要存在
    // 1 最小从节点数
    // 2 从节点最大延迟时间（从节点没有给主节点通信的最大时间）
    if (!server.repl_min_slaves_to_write ||
        !server.repl_min_slaves_max_lag)
        return;

    // 遍历从节点
    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        // 计算延迟时间
        // todo 根据从服务器定时上报的 ack 命令时间
        time_t lag = server.unixtime - slave->repl_ack_time;

        // 统计在最大延迟时间内的从节点数
        if (slave->replstate == SLAVE_STATE_ONLINE &&
            lag <= server.repl_min_slaves_max_lag)
            good++;
    }

    // 更新状态良好的从服务器数量
    server.repl_good_slaves_count = good;
}

/* ----------------------- REPLICATION SCRIPT CACHE --------------------------
 * The goal of this code is to keep track of scripts already sent to every
 * connected slave, in order to be able to replicate EVALSHA as it is without
 * translating it to EVAL every time it is possible.
 *
 * We use a capped collection implemented by a hash table for fast lookup
 * of scripts we can send as EVALSHA, plus a linked list that is used for
 * eviction of the oldest entry when the max number of items is reached.
 *
 * We don't care about taking a different cache for every different slave
 * since to fill the cache again is not very costly, the goal of this code
 * is to avoid that the same big script is transmitted a big number of times
 * per second wasting bandwidth and processor speed, but it is not a problem
 * if we need to rebuild the cache from scratch from time to time, every used
 * script will need to be transmitted a single time to reappear in the cache.
 *
 * This is how the system works:
 *
 * 1) Every time a new slave connects, we flush the whole script cache.
 * 2) We only send as EVALSHA what was sent to the master as EVALSHA, without
 *    trying to convert EVAL into EVALSHA specifically for slaves.
 * 3) Every time we transmit a script as EVAL to the slaves, we also add the
 *    corresponding SHA1 of the script into the cache as we are sure every
 *    slave knows about the script starting from now.
 * 4) On SCRIPT FLUSH command, we replicate the command to all the slaves
 *    and at the same time flush the script cache.
 * 5) When the last slave disconnects, flush the cache.
 * 6) We handle SCRIPT LOAD as well since that's how scripts are loaded
 *    in the master sometimes.
 */

/* Initialize the script cache, only called at startup. */
void replicationScriptCacheInit(void) {
    server.repl_scriptcache_size = 10000;
    server.repl_scriptcache_dict = dictCreate(&replScriptCacheDictType, NULL);
    server.repl_scriptcache_fifo = listCreate();
}

/* Empty the script cache. Should be called every time we are no longer sure
 * that every slave knows about all the scripts in our set, or when the
 * current AOF "context" is no longer aware of the script. In general we
 * should flush the cache:
 *
 * 1) Every time a new slave reconnects to this master and performs a
 *    full SYNC (PSYNC does not require flushing).
 * 2) Every time an AOF rewrite is performed.
 * 3) Every time we are left without slaves at all, and AOF is off, in order
 *    to reclaim otherwise unused memory.
 */
void replicationScriptCacheFlush(void) {
    dictEmpty(server.repl_scriptcache_dict, NULL);
    listRelease(server.repl_scriptcache_fifo);
    server.repl_scriptcache_fifo = listCreate();
}

/* Add an entry into the script cache, if we reach max number of entries the
 * oldest is removed from the list. */
void replicationScriptCacheAdd(sds sha1) {
    int retval;
    sds key = sdsdup(sha1);

    /* Evict oldest. */
    if (listLength(server.repl_scriptcache_fifo) == server.repl_scriptcache_size) {
        listNode *ln = listLast(server.repl_scriptcache_fifo);
        sds oldest = listNodeValue(ln);

        retval = dictDelete(server.repl_scriptcache_dict, oldest);
        serverAssert(retval == DICT_OK);
        listDelNode(server.repl_scriptcache_fifo, ln);
    }

    /* Add current. */
    retval = dictAdd(server.repl_scriptcache_dict, key, NULL);
    listAddNodeHead(server.repl_scriptcache_fifo, key);
    serverAssert(retval == DICT_OK);
}

/* Returns non-zero if the specified entry exists inside the cache, that is,
 * if all the slaves are aware of this script SHA1. */
int replicationScriptCacheExists(sds sha1) {
    return dictFind(server.repl_scriptcache_dict, sha1) != NULL;
}

/* ----------------------- SYNCHRONOUS REPLICATION --------------------------
 * Redis synchronous replication design can be summarized in points:
 *
 * - Redis masters have a global replication offset, used by PSYNC.
 * - Master increment the offset every time new commands are sent to slaves.
 * - Slaves ping back masters with the offset processed so far.
 *
 * So synchronous replication adds a new WAIT command in the form:
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * That returns the number of replicas that processed the query when
 * we finally have at least num_replicas, or when the timeout was
 * reached.
 *
 * The command is implemented in this way:
 *
 * - Every time a client processes a command, we remember the replication
 *   offset after sending that command to the slaves.
 * - When WAIT is called, we ask slaves to send an acknowledgement ASAP.
 *   The client is blocked at the same time (see blocked.c).
 * - Once we receive enough ACKs for a given offset or when the timeout
 *   is reached, the WAIT command is unblocked and the reply sent to the
 *   client.
 */

/* This just set a flag so that we broadcast a REPLCONF GETACK command
 * to all the slaves in the beforeSleep() function. Note that this way
 * we "group" all the clients that want to wait for synchronous replication
 * in a given event loop iteration, and send a single GETACK for them all. */
void replicationRequestAckFromSlaves(void) {
    server.get_ack_from_slaves = 1;
}

/* Return the number of slaves that already acknowledged the specified
 * replication offset. */
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate != SLAVE_STATE_ONLINE) continue;
        if (slave->repl_ack_off >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands). */
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    if (server.masterhost) {
        addReplyError(c,
                      "WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }

    /* Argument parsing. */
    if (getLongFromObjectOrReply(c, c->argv[1], &numreplicas, NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c, c->argv[2], &timeout, UNIT_MILLISECONDS)
        != C_OK)
        return;

    /* First try without blocking at all. */
    ackreplicas = replicationCountAcksByOffset(c->woff);
    if (ackreplicas >= numreplicas || c->flags & CLIENT_MULTI) {
        addReplyLongLong(c, ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from slaves. */
    c->bpop.timeout = timeout;
    c->bpop.reploffset = offset;
    c->bpop.numreplicas = numreplicas;
    listAddNodeHead(server.clients_waiting_acks, c);
    blockClient(c, BLOCKED_WAIT);

    /* Make sure that the server will send an ACK request to all the slaves
     * before returning to the event loop. */
    replicationRequestAckFromSlaves();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingReplicas(client *c) {
    listNode *ln = listSearchKey(server.clients_waiting_acks, c);
    serverAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks, ln);
}

/* Check if there are clients blocked in WAIT that can be unblocked since
 * we received enough ACKs from slaves. */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    int last_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(server.clients_waiting_acks, &li);
    while ((ln = listNext(&li))) {
        client *c = ln->value;

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        if (last_offset && last_offset >= c->bpop.reploffset &&
            last_numreplicas >= c->bpop.numreplicas) {
            unblockClient(c);
            addReplyLongLong(c, last_numreplicas);
        } else {
            int numreplicas = replicationCountAcksByOffset(c->bpop.reploffset);

            if (numreplicas >= c->bpop.numreplicas) {
                last_offset = c->bpop.reploffset;
                last_numreplicas = numreplicas;
                unblockClient(c);
                addReplyLongLong(c, numreplicas);
            }
        }
    }
}

/* Return the slave replication offset for this instance, that is
 * the offset for which we already processed the master replication stream. */
// 返回从节点的复制偏移量
long long replicationGetSlaveOffset(void) {
    long long offset = 0;

    if (server.masterhost != NULL) {
        if (server.master) {
            offset = server.master->reploff;
        } else if (server.cached_master) {
            offset = server.cached_master->reploff;
        }
    }
    /* offset may be -1 when the master does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the master, so we return a positive
     * integer. */
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

/* Replication cron function, called 1 time per second.
 *
 * 复制周期函数，主节点和从节点共用
 */
void replicationCron(void) {
    static long long replication_cron_loops = 0;

    /* Check failover status first, to see if we need to start
     * handling the failover. */
    updateFailoverStatus();

    /* Non blocking connection timeout? */
    // 1 尝试连接到主服务器，但超时。那么就断开连接，等待下次重连
    if (server.masterhost &&
        // 处于 连接 ～  等待 PSYNC 的回复 之间，如果超时则断开
        (server.repl_state == REPL_STATE_CONNECTING ||
         slaveIsInHandshakeState()) &&
        (time(NULL) - server.repl_transfer_lastio) > server.repl_timeout) {
        serverLog(LL_WARNING, "Timeout connecting to the MASTER...");

        // 取消连接
        cancelReplicationHandshake(1);
    }

    /* Bulk transfer I/O timeout? */
    // 2 从主节点收到 RDB 文件超时，即 RDB 文件的传送已超时
    if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER &&
        (time(NULL) - server.repl_transfer_lastio) > server.repl_timeout) {
        serverLog(LL_WARNING,
                  "Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");

        // 断开连接，取消复制，并删除临时文件
        cancelReplicationHandshake(1);
    }

    /* Timed out master when we are an already connected slave? */
    // 3 从服务器曾经连接上主服务器，但现在超时了，释放连接主服务器的客户端
    if (server.masterhost && server.repl_state == REPL_STATE_CONNECTED &&
        (time(NULL) - server.master->lastinteraction) > server.repl_timeout) {
        serverLog(LL_WARNING, "MASTER timeout: no data nor PING received...");
        // 释放从库端的主服务的客户端
        freeClient(server.master);
    }

    /* Check if we should connect to a MASTER */
    // 4 复制状态处于 REPL_STATE_CONNECT，则尝试连接主服务器。并更新状态为 REPL_STATE_CONNECTING；注意主从模式初始化时从节点的复制状态为 REPL_STATE_CONNECT
    // FIXME 这个过程会有两个客户端：1）对于主节点来说会为从节点创建一个客户端 2）对于从节点来说，主节点是也是它的客户端
    if (server.repl_state == REPL_STATE_CONNECT) {
        serverLog(LL_NOTICE, "Connecting to MASTER %s:%d",
                  server.masterhost, server.masterport);
        // 连接主服务器
        // todo 会为连接套接字关联 syncWithMaster 函数
        connectWithMaster();
    }

    /* Send ACK to master from time to time.
     * todo 定期向主服务器发送 ACK 命令，上报复制进度等信息，作为心跳检测，作用如下：
     * - 检测主从节点网络状态
     * - 上报自身复制偏移量，检查复制数据是否丢失，如果主节点认为从节点数据丢失，则会按照增量复制的形式发送数据给从节点
     * - 实现主节点是否允许处理写命令的功能
     *
     * Note that we do not send periodic acks to masters that don't
     * support PSYNC and replication offsets.
     *
     * 不过如果主服务器客户端带有 REDIS_PRE_PSYNC 的话就不发送，因为带有该标识的版本为 < 2.8 的版本，这些版本不支持 ACK 命令
     */
    // 5 从服务器定时向主服务器发送 ACK 命令，上报复制进度等信息
    if (server.masterhost && server.master &&
        !(server.master->flags & CLIENT_PRE_PSYNC))
        // @see 用于 refreshGoodSlavesCount 函数
        replicationSendAck();

    /* If we have attached slaves, PING them from time to time.
     * 如果服务器有从服务器，定时向它们发送 PING 。
     *
     * So slaves can implement an explicit timeout to masters, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down.
     *
     * 这样从服务器就可以实现显式的 master 超时判断机制，即使 TCP 连接未断开也是如此。
     */
    // todo 6 如果是主服务器，那么周期性地向所有从服务器发送 PING，默认是 10s；实现心跳功能；
    listIter li;
    listNode *ln;
    robj *ping_argv[1];
    /* First, send PING according to ping_slave_period. */
    if ((replication_cron_loops % server.repl_ping_slave_period) == 0 &&
        listLength(server.slaves)) {
        /* Note that we don't send the PING if the clients are paused during
         * a Redis Cluster manual failover: the PING we send will otherwise
         * alter the replication offsets of master and slave, and will no longer
         * match the one stored into 'mf_master_offset' state. */
        int manual_failover_in_progress =
                ((server.cluster_enabled &&
                  server.cluster->mf_end) ||
                 server.failover_end_time) &&
                checkClientPauseTimeoutAndReturnIfPaused();

        // 非 manual_failover_in_progress 手动故障转移，就向所有已连接 slave 发送 PING
        if (!manual_failover_in_progress) {
            ping_argv[0] = shared.ping;
            replicationFeedSlaves(server.slaves, server.slaveseldb,
                                  ping_argv, 1);
        }
    }

    /* Second, send a newline to all the slaves in pre-synchronization
     * stage, that is, slaves waiting for the master to create the RDB file.
     *
     * Also send the a newline to all the chained slaves we have, if we lost
     * connection from our master, to keep the slaves aware that their
     * master is online. This is needed since sub-slaves only receive proxied
     * data from top-level masters, so there is no explicit pinging in order
     * to avoid altering the replication offsets. This special out of band
     * pings (newlines) can be sent, they will have no effect in the offset.
     *
     * The newline will be ignored by the slave but will refresh the
     * last interaction timer preventing a timeout. In this case we ignore the
     * ping period and refresh the connection once per second since certain
     * timeouts are set at a few seconds (example: PSYNC response). */
    // 向那些正在等待 RDB 文件的从服务器（状态为 BGSAVE_START 或 BGSAVE_END）发送 \n，
    // 这个 "\n" 会被从服务器忽略，它的作用就是用来防止主服务器因为长期不发送信息而被从服务器误判为超时
    // todo 主节点用于包活
    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        int is_presync =
                (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
                 (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
                  server.rdb_child_type != RDB_CHILD_TYPE_SOCKET));

        if (is_presync) {
            // 发送 \n ,用于主服务器和从服务器保持连接
            connWrite(slave->conn, "\n", 1);
        }
    }

    /* Disconnect timedout slaves. */
    // 7 断开超时的服务器（当前是主服务器）
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        // 遍历所有从服务器
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate == SLAVE_STATE_ONLINE) {
                // 忽略旧版本的从服务器
                if (slave->flags & CLIENT_PRE_PSYNC)
                    continue;

                // 释放超时的从服务器的客户端
                if ((server.unixtime - slave->repl_ack_time) > server.repl_timeout) {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (streaming sync): %s",
                              replicationGetSlaveName(slave));
                    freeClient(slave);
                    continue;
                }
            }
            /* We consider disconnecting only diskless replicas because disk-based replicas aren't fed
             * by the fork child so if a disk-based replica is stuck it doesn't prevent the fork child
             * from terminating. */
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END && server.rdb_child_type == RDB_CHILD_TYPE_SOCKET) {
                if (slave->repl_last_partial_write != 0 &&
                    (server.unixtime - slave->repl_last_partial_write) > server.repl_timeout) {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (full sync): %s",
                              replicationGetSlaveName(slave));
                    freeClient(slave);
                    continue;
                }
            }
        }
    }

    /* If this is a master without attached slaves and there is a replication
     * backlog active, in order to reclaim memory we can free it after some
     * (configured) time. Note that this cannot be done for slaves: slaves
     * without sub-slaves attached should still accumulate data into the
     * backlog, in order to reply to PSYNC queries if they are turned into
     * masters after a failover. */
    // 8 在没有任何从服务器的 N 秒之后，主服务会释放 backlog
    if (listLength(server.slaves) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog && server.masterhost == NULL) {
        // 当前时间 - 没有从节点的时间，得到 N s
        time_t idle = server.unixtime - server.repl_no_slaves_since;

        if (idle > server.repl_backlog_time_limit) {
            /* When we free the backlog, we always use a new
             * replication ID and clear the ID2. This is needed
             * because when there is no backlog, the master_repl_offset
             * is not updated, but we would still retain our replication
             * ID, leading to the following problem:
             *
             * 1. We are a master instance.
             * 2. Our slave is promoted to master. It's repl-id-2 will
             *    be the same as our repl-id.
             * 3. We, yet as master, receive some updates, that will not
             *    increment the master_repl_offset.
             * 4. Later we are turned into a slave, connect to the new
             *    master that will accept our PSYNC request by second
             *    replication ID, but there will be data inconsistency
             *    because we received writes. */
            changeReplicationId();
            clearReplicationId2();
            // 释放 backlog ，即复制积压缓冲区
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                      "Replication backlog freed after %d seconds "
                      "without connected replicas.",
                      (int) server.repl_backlog_time_limit);
        }
    }

    /* If AOF is disabled and we no longer have attached slaves, we can
     * free our Replication Script Cache as there is no need to propagate
     * EVALSHA at all. */
    // 在没有任何从服务器，AOF 关闭的情况下，清空 script 缓存
    // 因为已经没有传播 EVALSHA 的必要了
    if (listLength(server.slaves) == 0 &&
        server.aof_state == AOF_OFF &&
        listLength(server.repl_scriptcache_fifo) != 0) {
        replicationScriptCacheFlush();
    }

    replicationStartPendingFork();

    /* Remove the RDB file used for replication if Redis is not running
     * with any persistence. */
    removeRDBUsedToSyncReplicas();

    /* Refresh the number of slaves with lag <= min-slaves-max-lag. */
    // 9 todo 更新符合给定延迟值的从服务器的数量
    refreshGoodSlavesCount();

    replication_cron_loops++; /* Incremented with frequency 1 HZ. */
}

void replicationStartPendingFork(void) {
    /* Start a BGSAVE good for replication if we have slaves in
     * WAIT_BGSAVE_START state.
     *
     * In case of diskless replication, we make sure to wait the specified
     * number of seconds (according to configuration) so that other slaves
     * have the time to arrive before we start streaming. */
    if (!hasActiveChildProcess()) {
        time_t idle, max_idle = 0;
        int slaves_waiting = 0;
        int mincapa = -1;
        listNode *ln;
        listIter li;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                idle = server.unixtime - slave->lastinteraction;
                if (idle > max_idle) max_idle = idle;
                slaves_waiting++;
                mincapa = (mincapa == -1) ? slave->slave_capa :
                          (mincapa & slave->slave_capa);
            }
        }

        if (slaves_waiting &&
            (!server.repl_diskless_sync ||
             max_idle >= server.repl_diskless_sync_delay)) {
            /* Start the BGSAVE. The called function may start a
             * BGSAVE with socket target or disk target depending on the
             * configuration and slaves capabilities. */
            startBgsaveForReplication(mincapa);
        }
    }
}

/* Find replica at IP:PORT from replica list */
static client *findReplica(char *host, int port) {
    listIter li;
    listNode *ln;
    client *replica;

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        replica = ln->value;
        char ip[NET_IP_STR_LEN], *replicaip = replica->slave_addr;

        if (!replicaip) {
            if (connPeerToString(replica->conn, ip, sizeof(ip), NULL) == -1)
                continue;
            replicaip = ip;
        }

        if (!strcasecmp(host, replicaip) &&
            (port == replica->slave_listening_port))
            return replica;
    }

    return NULL;
}

const char *getFailoverStateString() {
    switch (server.failover_state) {
        case NO_FAILOVER:
            return "no-failover";
        case FAILOVER_IN_PROGRESS:
            return "failover-in-progress";
        case FAILOVER_WAIT_FOR_SYNC:
            return "waiting-for-sync";
        default:
            return "unknown";
    }
}

/* Resets the internal failover configuration, this needs
 * to be called after a failover either succeeds or fails
 * as it includes the client unpause. */
void clearFailoverState() {
    server.failover_end_time = 0;
    server.force_failover = 0;
    zfree(server.target_replica_host);
    server.target_replica_host = NULL;
    server.target_replica_port = 0;
    server.failover_state = NO_FAILOVER;
    unpauseClients();
}

/* Abort an ongoing failover if one is going on. */
void abortFailover(const char *err) {
    if (server.failover_state == NO_FAILOVER) return;

    if (server.target_replica_host) {
        serverLog(LL_NOTICE, "FAILOVER to %s:%d aborted: %s",
                  server.target_replica_host, server.target_replica_port, err);
    } else {
        serverLog(LL_NOTICE, "FAILOVER to any replica aborted: %s", err);
    }
    if (server.failover_state == FAILOVER_IN_PROGRESS) {
        replicationUnsetMaster();
    }
    clearFailoverState();
}

/* 
 * FAILOVER [TO <HOST> <PORT> [FORCE]] [ABORT] [TIMEOUT <timeout>]
 * 
 * This command will coordinate a failover between the master and one
 * of its replicas. The happy path contains the following steps:
 * 1) The master will initiate a client pause write, to stop replication
 * traffic.
 * 2) The master will periodically check if any of its replicas has
 * consumed the entire replication stream through acks. 
 * 3) Once any replica has caught up, the master will itself become a replica.
 * 4) The master will send a PSYNC FAILOVER request to the target replica, which
 * if accepted will cause the replica to become the new master and start a sync.
 * 
 * FAILOVER ABORT is the only way to abort a failover command, as replicaof
 * will be disabled. This may be needed if the failover is unable to progress. 
 * 
 * The optional arguments [TO <HOST> <IP>] allows designating a specific replica
 * to be failed over to.
 * 
 * FORCE flag indicates that even if the target replica is not caught up,
 * failover to it anyway. This must be specified with a timeout and a target
 * HOST and IP.
 * 
 * TIMEOUT <timeout> indicates how long should the primary wait for 
 * a replica to sync up before aborting. If not specified, the failover
 * will attempt forever and must be manually aborted.
 */
void failoverCommand(client *c) {
    if (server.cluster_enabled) {
        addReplyError(c, "FAILOVER not allowed in cluster mode. "
                         "Use CLUSTER FAILOVER command instead.");
        return;
    }

    /* Handle special case for abort */
    if ((c->argc == 2) && !strcasecmp(c->argv[1]->ptr, "abort")) {
        if (server.failover_state == NO_FAILOVER) {
            addReplyError(c, "No failover in progress.");
            return;
        }

        abortFailover("Failover manually aborted");
        addReply(c, shared.ok);
        return;
    }

    long timeout_in_ms = 0;
    int force_flag = 0;
    long port = 0;
    char *host = NULL;

    /* Parse the command for syntax and arguments. */
    for (int j = 1; j < c->argc; j++) {
        if (!strcasecmp(c->argv[j]->ptr, "timeout") && (j + 1 < c->argc) &&
            timeout_in_ms == 0) {
            if (getLongFromObjectOrReply(c, c->argv[j + 1],
                                         &timeout_in_ms, NULL) != C_OK)
                return;
            if (timeout_in_ms <= 0) {
                addReplyError(c, "FAILOVER timeout must be greater than 0");
                return;
            }
            j++;
        } else if (!strcasecmp(c->argv[j]->ptr, "to") && (j + 2 < c->argc) &&
                   !host) {
            if (getLongFromObjectOrReply(c, c->argv[j + 2], &port, NULL) != C_OK)
                return;
            host = c->argv[j + 1]->ptr;
            j += 2;
        } else if (!strcasecmp(c->argv[j]->ptr, "force") && !force_flag) {
            force_flag = 1;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    if (server.failover_state != NO_FAILOVER) {
        addReplyError(c, "FAILOVER already in progress.");
        return;
    }

    if (server.masterhost) {
        addReplyError(c, "FAILOVER is not valid when server is a replica.");
        return;
    }

    if (listLength(server.slaves) == 0) {
        addReplyError(c, "FAILOVER requires connected replicas.");
        return;
    }

    if (force_flag && (!timeout_in_ms || !host)) {
        addReplyError(c, "FAILOVER with force option requires both a timeout "
                         "and target HOST and IP.");
        return;
    }

    /* If a replica address was provided, validate that it is connected. */
    if (host) {
        client *replica = findReplica(host, port);

        if (replica == NULL) {
            addReplyError(c, "FAILOVER target HOST and PORT is not "
                             "a replica.");
            return;
        }

        /* Check if requested replica is online */
        if (replica->replstate != SLAVE_STATE_ONLINE) {
            addReplyError(c, "FAILOVER target replica is not online.");
            return;
        }

        server.target_replica_host = zstrdup(host);
        server.target_replica_port = port;
        serverLog(LL_NOTICE, "FAILOVER requested to %s:%ld.", host, port);
    } else {
        serverLog(LL_NOTICE, "FAILOVER requested to any replica.");
    }

    mstime_t now = mstime();
    if (timeout_in_ms) {
        server.failover_end_time = now + timeout_in_ms;
    }

    server.force_failover = force_flag;
    server.failover_state = FAILOVER_WAIT_FOR_SYNC;
    /* Cluster failover will unpause eventually */
    pauseClients(LLONG_MAX, CLIENT_PAUSE_WRITE);
    addReply(c, shared.ok);
}

/* Failover cron function, checks coordinated failover state. 
 *
 * Implementation note: The current implementation calls replicationSetMaster()
 * to start the failover request, this has some unintended side effects if the
 * failover doesn't work like blocked clients will be unblocked and replicas will
 * be disconnected. This could be optimized further.
 */
void updateFailoverStatus(void) {
    if (server.failover_state != FAILOVER_WAIT_FOR_SYNC) return;
    mstime_t now = server.mstime;

    /* Check if failover operation has timed out */
    if (server.failover_end_time && server.failover_end_time <= now) {
        if (server.force_failover) {
            serverLog(LL_NOTICE,
                      "FAILOVER to %s:%d time out exceeded, failing over.",
                      server.target_replica_host, server.target_replica_port);
            server.failover_state = FAILOVER_IN_PROGRESS;
            /* If timeout has expired force a failover if requested. */
            replicationSetMaster(server.target_replica_host,
                                 server.target_replica_port);
            return;
        } else {
            /* Force was not requested, so timeout. */
            abortFailover("Replica never caught up before timeout");
            return;
        }
    }

    /* Check to see if the replica has caught up so failover can start */
    client *replica = NULL;
    if (server.target_replica_host) {
        replica = findReplica(server.target_replica_host,
                              server.target_replica_port);
    } else {
        listIter li;
        listNode *ln;

        listRewind(server.slaves, &li);
        /* Find any replica that has matched our repl_offset */
        while ((ln = listNext(&li))) {
            replica = ln->value;
            if (replica->repl_ack_off == server.master_repl_offset) {
                char ip[NET_IP_STR_LEN], *replicaaddr = replica->slave_addr;

                if (!replicaaddr) {
                    if (connPeerToString(replica->conn, ip, sizeof(ip), NULL) == -1)
                        continue;
                    replicaaddr = ip;
                }

                /* We are now failing over to this specific node */
                server.target_replica_host = zstrdup(replicaaddr);
                server.target_replica_port = replica->slave_listening_port;
                break;
            }
        }
    }

    /* We've found a replica that is caught up */
    if (replica && (replica->repl_ack_off == server.master_repl_offset)) {
        server.failover_state = FAILOVER_IN_PROGRESS;
        serverLog(LL_NOTICE,
                  "Failover target %s:%d is synced, failing over.",
                  server.target_replica_host, server.target_replica_port);
        /* Designated replica is caught up, failover to it. */
        replicationSetMaster(server.target_replica_host,
                             server.target_replica_port);
    }
}
