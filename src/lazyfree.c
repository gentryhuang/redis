#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static redisAtomic size_t lazyfree_objects = 0;
static redisAtomic size_t lazyfreed_objects = 0;

/* Release objects from the lazyfree thread. It's just decrRefCount()
 * updating the count of objects to release. */
void lazyfreeFreeObject(void *args[]) {
    robj *o = (robj *) args[0];
    decrRefCount(o);
    atomicDecr(lazyfree_objects, 1);
    atomicIncr(lazyfreed_objects, 1);
}

/* Release a database from the lazyfree thread. The 'db' pointer is the
 * database which was substituted with a fresh one in the main thread
 * when the database was logically deleted. */
void lazyfreeFreeDatabase(void *args[]) {
    dict *ht1 = (dict *) args[0];
    dict *ht2 = (dict *) args[1];

    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    dictRelease(ht2);
    atomicDecr(lazyfree_objects, numkeys);
    atomicIncr(lazyfreed_objects, numkeys);
}

/* Release the skiplist mapping Redis Cluster keys to slots in the
 * lazyfree thread. */
void lazyfreeFreeSlotsMap(void *args[]) {
    rax *rt = args[0];
    size_t len = rt->numele;
    raxFree(rt);
    atomicDecr(lazyfree_objects, len);
    atomicIncr(lazyfreed_objects, len);
}

/* Release the key tracking table. */
void lazyFreeTrackingTable(void *args[]) {
    rax *rt = args[0];
    size_t len = rt->numele;
    freeTrackingRadixTree(rt);
    atomicDecr(lazyfree_objects, len);
    atomicIncr(lazyfreed_objects, len);
}

void lazyFreeLuaScripts(void *args[]) {
    dict *lua_scripts = args[0];
    long long len = dictSize(lua_scripts);
    dictRelease(lua_scripts);
    atomicDecr(lazyfree_objects, len);
    atomicIncr(lazyfreed_objects, len);
}

/* Return the number of currently pending objects to free. */
size_t lazyfreeGetPendingObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfree_objects, aux);
    return aux;
}

/* Return the number of objects that have been freed. */
size_t lazyfreeGetFreedObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfreed_objects, aux);
    return aux;
}

/* Return the amount of work needed in order to free an object.
 *
 * 返回释放一个对象所需的工作量
 *
 * The return value is not always the actual number of allocations the
 * object is composed of, but a number proportional to it.
 *
 * 返回值并不总是对象所组成的实际分配的数量，而是与之成比例的数字。
 *
 * For strings the function always returns 1.
 *
 * 对于字符串，总是返回 1
 *
 * For aggregated objects represented by hash tables or other data structures
 * the function just returns the number of elements the object is composed of.
 *
 * 对于由哈希表或其他数据结构表示的聚合对象，函数只返回对象所组成的元素的数量。
 *
 * Objects composed of single allocations are always reported as having a
 * single item even if they are actually logical composed of multiple
 * elements.
 *
 * 由单个分配组成的对象总是报告为具有单个项，即使它们实际上是由多个元素在逻辑上组成的。
 *
 * For lists the function returns the number of elements in the quicklist
 * representing the list.
 *
 * 对于列表，该函数返回表示列表的快速列表中的元素数。
 */
size_t lazyfreeGetFreeEffort(robj *key, robj *obj) {
    // 对象类型是 list，返回快速列表中元素个数
    if (obj->type == OBJ_LIST) {
        quicklist *ql = obj->ptr;
        return ql->len;

        // 对象类型是 set && 编码类型是 hashtable,返回哈希表中元素个数
    } else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = obj->ptr;
        return dictSize(ht);

        // 对象类型是 zset && 编码类型是 skiplist ，返回跳表的长度
    } else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = obj->ptr;
        return zs->zsl->length;

        // 对象类型是 hash && 编码类型是 hashtable ，返回哈希表中元素的个数
    } else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = obj->ptr;
        return dictSize(ht);

        // 对象类型是 stream 类型
    } else if (obj->type == OBJ_STREAM) {
        size_t effort = 0;
        stream *s = obj->ptr;

        /* Make a best effort estimate to maintain constant runtime. Every macro
         * node in the Stream is one allocation. */
        effort += s->rax->numnodes;

        /* Every consumer group is an allocation and so are the entries in its
         * PEL. We use size of the first group's PEL as an estimate for all
         * others. */
        if (s->cgroups && raxSize(s->cgroups)) {
            raxIterator ri;
            streamCG *cg;
            raxStart(&ri, s->cgroups);
            raxSeek(&ri, "^", NULL, 0);
            /* There must be at least one group so the following should always
             * work. */
            serverAssert(raxNext(&ri));
            cg = ri.data;
            effort += raxSize(s->cgroups) * (1 + raxSize(cg->pel));
            raxStop(&ri);
        }
        return effort;

        // 对象类型是 module
    } else if (obj->type == OBJ_MODULE) {
        moduleValue *mv = obj->ptr;
        moduleType *mt = mv->type;
        if (mt->free_effort != NULL) {
            size_t effort = mt->free_effort(key, mv->value);
            /* If the module's free_effort returns 0, it will use asynchronous free
             memory by default */
            return effort == 0 ? ULONG_MAX : effort;
        } else {
            return 1;
        }

        // 其他的都是单一分配（如：字符串总是返回）
    } else {
        return 1; /* Everything else is a single allocation. */
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB.
 * If there are enough allocations to free the value object may be put into
 * a lazy free list instead of being freed synchronously. The lazy free list
 * will be reclaimed in a different bio.c thread.*/
// 启用后台删除的阈值
// 对象中的元素大于该阈值时才真正丢给后台线程去删除，如果对象中包含的元素太少就没有必要丢给后台线程，因为线程同步也要一定的消耗。
#define LAZYFREE_THRESHOLD 64

/*
 * 异步删除逻辑：
 * 1. 清除待删除 key 的过期时间
 * 2. 将待删除的对象从数据字典中移除
 * 3. 判断删除 key 对应的 val 大小（太小就每必要异步删除了），如果足够大则加到后台线程任务队列中 （todo bigkey 处理）
 * 4. 清理数据字典的条目信息
 */
int dbAsyncDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary.
     *
     * 从 expires 中直接删除 key。
     * 清除待删除key的过期时间（从过期字典中删除一个键对象不会释放键的 sds，因为它是与主字典共享的。
     */
    if (dictSize(db->expires) > 0) dictDelete(db->expires, key->ptr);

    /* If the value is composed of a few allocations, to free in a lazy way
     * is actually just slower... So under a certain limit we just free
     * the object synchronously.
     *
     * 把要删除的对象从数据库字典摘除，但不进行实际 free 操作
     */
    dictEntry *de = dictUnlink(db->dict, key->ptr);
    if (de) {
        // 获取节点的值
        robj *val = dictGetVal(de);

        /* Tells the module that the key has been unlinked from the database. */
        moduleNotifyKeyUnlink(key, val);

        // todo 获取val对象所包含的元素个数（评估 free 当前 key 的代价）
        size_t free_effort = lazyfreeGetFreeEffort(key, val);

        /* If releasing the object is too much work, do it in the background
         * by adding the object to the lazy free list.
         * Note that if the object is shared, to reclaim it now it is not
         * possible. This rarely happens, however sometimes the implementation
         * of parts of the Redis core may call incrRefCount() to protect
         * objects, and then call dbDelete(). In this case we'll fall
         * through and reach the dictFreeUnlinkedEntry() call, that will be
         * equivalent to just calling decrRefCount(). */
        /**
         * 如果 free 当前 key cost > 64，则将它放到 lazy free 的 list ，使用 bio 子线程进行实际 free 操作。不通过主线程运行。
         * 特别说明：String 类型返回的 free_effort 始终是 1，因此不会执行异步删除逻辑
         */
        if (free_effort > LAZYFREE_THRESHOLD && val->refcount == 1) {

            //原子操作给待处理的 lazyfree_objects 加1，以备 info 命令查看有多少对象待后台线程删除
            atomicIncr(lazyfree_objects, 1);

            //此时真正把对象val加入到后台线程的任务列表中
            bioCreateLazyFreeJob(lazyfreeFreeObject, 1, val);

            //把条目里的val指针设置为NULL，防止删除数据库字典条目时重复删除val对象
            dictSetVal(db->dict, de, NULL);
        }
    }

    /* Release the key-val pair, or just the key if we set the val
     * field to NULL in order to lazy free it later.
     *
     * 释放键值对 或 val 为 NULL时只释放 key（由上面逻辑可知，异步删除是针对 val 的）
     */
    if (de) {
        //删除数据库字典条目，释放资源
        dictFreeUnlinkedEntry(db->dict, de);

        if (server.cluster_enabled) slotToKeyDel(key->ptr);
        return 1;
    } else {
        return 0;
    }
}

/* Free an object, if the object is huge enough, free it in async way. */
void freeObjAsync(robj *key, robj *obj) {
    size_t free_effort = lazyfreeGetFreeEffort(key, obj);
    if (free_effort > LAZYFREE_THRESHOLD && obj->refcount == 1) {
        atomicIncr(lazyfree_objects, 1);
        bioCreateLazyFreeJob(lazyfreeFreeObject, 1, obj);
    } else {
        decrRefCount(obj);
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing. */
void emptyDbAsync(redisDb *db) {
    dict *oldht1 = db->dict, *oldht2 = db->expires;
    db->dict = dictCreate(&dbDictType, NULL);
    db->expires = dictCreate(&dbExpiresDictType, NULL);
    atomicIncr(lazyfree_objects, dictSize(oldht1));
    bioCreateLazyFreeJob(lazyfreeFreeDatabase, 2, oldht1, oldht2);
}

/* Release the radix tree mapping Redis Cluster keys to slots asynchronously. */
void freeSlotsToKeysMapAsync(rax *rt) {
    atomicIncr(lazyfree_objects, rt->numele);
    bioCreateLazyFreeJob(lazyfreeFreeSlotsMap, 1, rt);
}

/* Free an object, if the object is huge enough, free it in async way. */
void freeTrackingRadixTreeAsync(rax *tracking) {
    atomicIncr(lazyfree_objects, tracking->numele);
    bioCreateLazyFreeJob(lazyFreeTrackingTable, 1, tracking);
}

/* Free lua_scripts dict, if the dict is huge enough, free it in async way. */
void freeLuaScriptsAsync(dict *lua_scripts) {
    if (dictSize(lua_scripts) > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects, dictSize(lua_scripts));
        bioCreateLazyFreeJob(lazyFreeLuaScripts, 1, lua_scripts);
    } else {
        dictRelease(lua_scripts);
    }
}
