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

/* ================================ MULTI/EXEC ============================== */

/* Client state initialization for MULTI/EXEC
 *
 * 初始化客户端的事务状态
 */
void initClientMultiState(client *c) {
    // 命令队列
    c->mstate.commands = NULL;

    // 命令队列长度
    c->mstate.count = 0;

    c->mstate.cmd_flags = 0;
    c->mstate.cmd_inv_flags = 0;
}

/* Release all the resources associated with MULTI/EXEC state
 *
 * 释放所有事务状态相关的资源
 */
void freeClientMultiState(client *c) {
    int j;

    // 遍历事务队列
    for (j = 0; j < c->mstate.count; j++) {
        int i;
        multiCmd *mc = c->mstate.commands + j;

        // 释放所有命令参数
        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);

        // 释放参数数组本身
        zfree(mc->argv);
    }

    // 释放事务队列
    zfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue
 *
 * 将一个新命令添加到事务队列中
 */
void queueMultiCommand(client *c) {
    multiCmd *mc;
    int j;

    /* No sense to waste memory if the transaction is already aborted.
     * this is useful in case client sends these in a pipeline, or doesn't
     * bother to read previous responses and didn't notice the multi was already
     * aborted. */
    // 客户端监视的 key 被其它客户端修改，则不再入队
    if (c->flags & CLIENT_DIRTY_EXEC)
        return;

    // 为新数组元素分配空间
    c->mstate.commands = zrealloc(c->mstate.commands,
                                  sizeof(multiCmd) * (c->mstate.count + 1));

    // 指向新元素
    mc = c->mstate.commands + c->mstate.count;

    // 设置命令、命令参数数量、以及命令的参数
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    mc->argv = zmalloc(sizeof(robj *) * c->argc);
    memcpy(mc->argv, c->argv, sizeof(robj *) * c->argc);
    for (j = 0; j < c->argc; j++)
        incrRefCount(mc->argv[j]);

    // 事务命令数量增一
    c->mstate.count++;
    c->mstate.cmd_flags |= c->cmd->flags;
    c->mstate.cmd_inv_flags |= ~c->cmd->flags;
}

/*
 * discard 操作
 */
void discardTransaction(client *c) {
    // 重置事务状态
    // 释放所有事务状态相关的资源
    freeClientMultiState(c);
    // 初始化客户端的事务状态
    initClientMultiState(c);

    // 重置客户端状态标志
    c->flags &= ~(CLIENT_MULTI | CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC);

    // 取消对所有键的监视
    unwatchAllKeys(c);
}

/* Flag the transaction as DIRTY_EXEC so that EXEC will fail.
 *
 * 将客户端的状态设置为 DIRTY_EXEC，让之后的 EXEC 命令失败
 *
 * Should be called every time there is an error while queueing a command.
 *
 * 每次在入队命令出错时调用
 */
void flagTransaction(client *c) {
    if (c->flags & CLIENT_MULTI)
        c->flags |= CLIENT_DIRTY_EXEC;
}

/*
 * multi 命令对应的函数
 */
void multiCommand(client *c) {
    // 不支持在事务中嵌套事务
    if (c->flags & CLIENT_MULTI) {
        addReplyError(c, "MULTI calls can not be nested");
        return;
    }

    // 开启事务，即设置客户端的状态标志为
    c->flags |= CLIENT_MULTI;

    addReply(c, shared.ok);
}

/*
 * discard 命令对应的函数
 */
void discardCommand(client *c) {
    // 不能在客户端未进行事务状态之前使用
    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c, "DISCARD without MULTI");
        return;
    }

    // discard 逻辑
    discardTransaction(c);

    addReply(c, shared.ok);
}

void beforePropagateMulti() {
    /* Propagating MULTI */
    serverAssert(!server.propagate_in_transaction);
    server.propagate_in_transaction = 1;
}

void afterPropagateExec() {
    /* Propagating EXEC */
    serverAssert(server.propagate_in_transaction == 1);
    server.propagate_in_transaction = 0;
}

/* Send a MULTI command to all the slaves and AOF file. Check the execCommand
 * implementation for more information.
 *
 * 向所有附属节点和 AOF 文件传播 MULTI 命令
 */
void execCommandPropagateMulti(int dbid) {
    beforePropagateMulti();

    // 命令传播
    propagate(server.multiCommand, dbid, &shared.multi, 1,
              PROPAGATE_AOF | PROPAGATE_REPL);
}

/*
 * 向所有附属节点和 AOF 文件传播 MULTI 命令
 */
void execCommandPropagateExec(int dbid) {
    propagate(server.execCommand, dbid, &shared.exec, 1,
              PROPAGATE_AOF | PROPAGATE_REPL);
    afterPropagateExec();
}

/* Aborts a transaction, with a specific error message.
 * The transaction is always aborted with -EXECABORT so that the client knows
 * the server exited the multi state, but the actual reason for the abort is
 * included too.
 * Note: 'error' may or may not end with \r\n. see addReplyErrorFormat. */
void execCommandAbort(client *c, sds error) {
    discardTransaction(c);

    if (error[0] == '-') error++;
    addReplyErrorFormat(c, "-EXECABORT Transaction discarded because of: %s", error);

    /* Send EXEC to clients waiting data from MONITOR. We did send a MULTI
     * already, and didn't send any of the queued commands, now we'll just send
     * EXEC so it is clear that the transaction is over. */
    replicationFeedMonitors(c, server.monitors, c->db->id, c->argv, c->argc);
}

/*
 * exec 命令对应的函数
 */
void execCommand(client *c) {
    int j;
    robj **orig_argv;
    int orig_argc;
    struct redisCommand *orig_cmd;
    int was_master = server.masterhost == NULL;

    // 当前客户端没有执行事务将会报错
    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c, "EXEC without MULTI");
        return;
    }

    /* Check if we need to abort the EXEC because:
     * 检查是否需要阻止事务执行，因为：
     *
     * 1) Some WATCHed key was touched.
     *    存在被客户端监视的键被修改
     *
     * 2) There was a previous error while queueing commands.
     *    命令在入队时发生错误
     *
     * A failed EXEC in the first case returns a multi bulk nil object
     * (technically it is not an error but a special behavior), while
     * in the second an EXECABORT error is returned. */
    if (c->flags & (CLIENT_DIRTY_CAS | CLIENT_DIRTY_EXEC)) {
        addReply(c, c->flags & CLIENT_DIRTY_EXEC ? shared.execaborterr :
                    shared.nullarray[c->resp]);

        // 执行 discard 逻辑，即取消事务
        discardTransaction(c);
        return;
    }

    // 保存当前客户端的状态标记
    uint64_t old_flags = c->flags;

    /* we do not want to allow blocking commands inside multi */
    // 我们不希望在multi中允许阻塞命令
    c->flags |= CLIENT_DENY_BLOCKING;

    /* Exec all the queued commands */
    // 已经可以保证安全性了，取消客户端对所有键的监视
    // 将当前客户端的 watched_keys 链表回收
    // 从当前客户端操作的数据库的 watched_keys 字典中移除当前客户端
    unwatchAllKeys(c); /* Unwatch ASAP otherwise we'll waste CPU cycles */

    server.in_exec = 1;

    // 因为事务中的命令在执行时可能会修改命令和命令的参数，所以为了正确地传播命令，需要现备份这些命令和参数
    orig_argv = c->argv;
    orig_argc = c->argc;
    orig_cmd = c->cmd;

    addReplyArrayLen(c, c->mstate.count);

    // 执行事务队列中的命令
    for (j = 0; j < c->mstate.count; j++) {

        // 因为 Redis 的命令必须在客户端的上下文中执行，所以需要将事务队列中的命令、命令参数等设置到客户端
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->cmd = c->mstate.commands[j].cmd;

        /* ACL permissions are also checked at the time of execution in case
         * they were changed after the commands were queued.
         *
         * ACL权限还在执行时检查，以防在命令排队后更改。
         */
        int acl_errpos;
        int acl_retval = ACLCheckAllPerm(c, &acl_errpos);
        if (acl_retval != ACL_OK) {
            char *reason;
            switch (acl_retval) {
                case ACL_DENIED_CMD:
                    reason = "no permission to execute the command or subcommand";
                    break;
                case ACL_DENIED_KEY:
                    reason = "no permission to touch the specified keys";
                    break;
                case ACL_DENIED_CHANNEL:
                    reason = "no permission to access one of the channels used "
                             "as arguments";
                    break;
                default:
                    reason = "no permission";
                    break;
            }
            addACLLogEntry(c, acl_retval, acl_errpos, NULL);
            addReplyErrorFormat(c,
                                "-NOPERM ACLs rules changed between the moment the "
                                "transaction was accumulated and the EXEC call. "
                                "This command is no longer allowed for the "
                                "following reason: %s", reason);
        } else {
            // 执行命令
            call(c, server.loading ? CMD_CALL_NONE : CMD_CALL_FULL);
            serverAssert((c->flags & CLIENT_BLOCKED) == 0);
        }

        /* Commands may alter argc/argv, restore mstate. */
        /*
         * 因为命令执行后命令、命令参数可能会被改变，比如 SPOP 会被改写为 SREM
         * 所以这里需要更新事务队列中的命令和参数，确保附属节点和 AOF 的数据一致性
         */
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].cmd = c->cmd;
    }

    // restore old DENY_BLOCKING value
    if (!(old_flags & CLIENT_DENY_BLOCKING))
        c->flags &= ~CLIENT_DENY_BLOCKING;

    // 还原命令
    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;

    // 清理事务状态
    discardTransaction(c);

    /* Make sure the EXEC command will be propagated as well if MULTI
     * was already propagated.
     *
     * 如果已经传播了 MULTI，也要确保传播 EXEC 命令
     */
    if (server.propagate_in_transaction) {
        int is_master = server.masterhost == NULL;
        server.dirty++;
        /* If inside the MULTI/EXEC block this instance was suddenly
         * switched from master to slave (using the SLAVEOF command), the
         * initial MULTI was propagated into the replication backlog, but the
         * rest was not. We need to make sure to at least terminate the
         * backlog with the final EXEC. */
        if (server.repl_backlog && was_master && !is_master) {
            char *execcmd = "*1\r\n$4\r\nEXEC\r\n";
            feedReplicationBacklog(execcmd, strlen(execcmd));
        }
        afterPropagateExec();
    }

    server.in_exec = 0;
}

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 * 这个实现为每个数据库都设置了一个将 key 映射为 list 的字典。
 * list 中保存了所有监视这个 key 的客户端。这样就可以在这个 key 被修改时，方便地对所有监视这个 key 的客户端进行处理。
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called.
 *
 * 另外，每个客户端都会保存一个带有所有被监视键的列表。这样可以方便地对所有被监视键进行 UNWATCH
 *
 */


/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB
 *
 * 在监视一个键时：
 * 我们既需要保存被监视的键，还需要保存该键所在的数据库
 */
typedef struct watchedKey {
    // 被监视的键对象
    robj *key;

    // 键所在的数据库
    redisDb *db;
} watchedKey;

/* Watch for the specified key
 *
 * 客户端监视给定的键
 */
void watchForKey(client *c, robj *key) {
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    /* Check if we are already watching for this key */
    // 1 检查 key 是否已经保存在 watched_keys 链表中，如果已经存在，则直接返回
    listRewind(c->watched_keys, &li);
    while ((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key, wk->key))
            return; /* Key already watched */
    }


    /* This key is not already watched in this DB. Let's add it */
    // 2 检查 key 是否存在于数据库的 watched_keys 字典中
    clients = dictFetchValue(c->db->watched_keys, key);
    // 2.1 如果不存在则，添加它
    if (!clients) {
        // 值是一个链表
        clients = listCreate();

        // 关联键值对到字典
        dictAdd(c->db->watched_keys, key, clients);
        incrRefCount(key);
    }
    // 将客户端添加到链表的末尾
    listAddNodeTail(clients, c);
    // 2.2 例子如下
    // 以下是一个 key 不存在于 watched_keys 字典的例子：
    // before :
    // {
    //  'key-1' : [c1, c2, c3],
    //  'key-2' : [c1, c2],
    // }
    // after c-10086 WATCH key-1 and key-3:
    // {
    //  'key-1' : [c1, c2, c3, c-10086],
    //  'key-2' : [c1, c2],
    //  'key-3' : [c-10086]
    // }

    /* Add the new key to the list of keys watched by this client */
    // 3 构建 watchedKey 结构并添加到客户端的 watched_key 链表的末尾
    // 3.1
    wk = zmalloc(sizeof(*wk));
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    listAddNodeTail(c->watched_keys, wk);

    // 3.2 例子
    // 以下是一个添加 watchedKey 结构到客户端的 watched_key 链表末尾的例子
    // before:
    // [
    //  {
    //   'key': 'key-1',
    //   'db' : 0
    //  }
    // ]
    // after client watch key-123321 in db 0:
    // [
    //  {
    //   'key': 'key-1',
    //   'db' : 0
    //  }
    //  ,
    //  {
    //   'key': 'key-123321',
    //   'db': 0
    //  }
    // ]
}

/* Unwatch all the keys watched by this client. To clean the EXEC dirty
 * flag is up to the caller.
 *
 * 取消客户端对所有键的监视
 *
 * 清除客户端事务状态的任务由调用者执行
 */
void unwatchAllKeys(client *c) {
    listIter li;
    listNode *ln;

    // 1 没有键被监视，直接返回
    if (listLength(c->watched_keys) == 0) return;

    // 2 遍历链表中所有被客户端监视的键
    listRewind(c->watched_keys, &li);
    while ((ln = listNext(&li))) {
        list *clients;
        watchedKey *wk;

        /* Lookup the watched key -> clients list and remove the client
         * from the list */
        // 2.1 获取当前遍历的链表节点对应的值，即 watchedKey
        wk = listNodeValue(ln);

        // 2.2 从数据库的 watched_keys 字典中取出监视 wk->key 这个键的所有客户端
        clients = dictFetchValue(wk->db->watched_keys, wk->key);
        serverAssertWithInfo(c, NULL, clients != NULL);

        // 2.2.1 删除客户端链表中当前客户端节点
        listDelNode(clients, listSearchKey(clients, c));

        /* Kill the entry at all if this was the only client */
        // 2.2.2 如果客户端链表已经被清空，那么删除这个键
        if (listLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);


        /* Remove this watched key from the client->watched list */
        // 2.3 从当前客户端的 watched_keys 链表中移除 key 节点
        listDelNode(c->watched_keys, ln);

        decrRefCount(wk->key);
        zfree(wk);
    }
}

/* "Touch" a key, so that if this key is being WATCHed by some client the
 * next EXEC will fail.
 *
 * "触碰" key，如果该 key 正在被某个/某些客户端监视着，那么这个/这些客户端在执行 EXEC 时，事件将失败
 */
void touchWatchedKey(redisDb *db, robj *key) {
    list *clients;
    listIter li;
    listNode *ln;

    // 当前数据库的 watched_leys 字典为空，没有任何键被监视
    if (dictSize(db->watched_keys) == 0) return;

    // 获取所有监视这个键的客户端
    clients = dictFetchValue(db->watched_keys, key);
    if (!clients) return;

    /* Mark all the clients watching this key as CLIENT_DIRTY_CAS */
    /* Check if we are already watching for this key
     *
     * 遍历所有客户端，打开它们的 CLIENT_DIRTY_CAS
     */
    listRewind(clients, &li);
    while ((ln = listNext(&li))) {
        client *c = listNodeValue(ln);

        c->flags |= CLIENT_DIRTY_CAS;
    }
}

/* Set CLIENT_DIRTY_CAS to all clients of DB when DB is dirty.
 * It may happen in the following situations:
 * FLUSHDB, FLUSHALL, SWAPDB
 *
 * 当一个数据库被 FLUSHDB 或 FLUSHALL 时，数据库内的所有 key 都应该被"触碰"
 *
 * replaced_with: for SWAPDB, the WATCH should be invalidated if
 * the key exists in either of them, and skipped only if it
 * doesn't exist in both.
 */
void touchAllWatchedKeysInDb(redisDb *emptied, redisDb *replaced_with) {
    listIter li;
    listNode *ln;
    dictEntry *de;

    if (dictSize(emptied->watched_keys) == 0) return;

    dictIterator *di = dictGetSafeIterator(emptied->watched_keys);
    while ((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);
        list *clients = dictGetVal(de);
        if (!clients) continue;
        listRewind(clients, &li);
        while ((ln = listNext(&li))) {
            client *c = listNodeValue(ln);
            if (dictFind(emptied->dict, key->ptr)) {
                c->flags |= CLIENT_DIRTY_CAS;
            } else if (replaced_with && dictFind(replaced_with->dict, key->ptr)) {
                c->flags |= CLIENT_DIRTY_CAS;
            }
        }
    }
    dictReleaseIterator(di);
}

/*
 * watch 命令对应的函数
 */
void watchCommand(client *c) {
    int j;

    // 不能在事务开始后执行
    if (c->flags & CLIENT_MULTI) {
        addReplyError(c, "WATCH inside MULTI is not allowed");
        return;
    }

    // 监视输入的任意个键
    for (j = 1; j < c->argc; j++)
        watchForKey(c, c->argv[j]);

    addReply(c, shared.ok);
}

/*
 * unwatch 命令对应的函数
 */
void unwatchCommand(client *c) {
    // 取消客户端对所有键的监视
    unwatchAllKeys(c);

    // 重置状态
    c->flags &= (~CLIENT_DIRTY_CAS);
    addReply(c, shared.ok);
}
