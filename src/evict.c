/* Maxmemory directive handling (LRU eviction and other policies).
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "bio.h"
#include "atomicvar.h"
#include <math.h>

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across performEvictions() calls.
 *
 * Entries inside the eviction pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */

/*
 * 待淘汰的集合大小
 */
#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255

/*
 * 保存待淘汰的候选键值对。
 * @see evictionPoolAlloc
 */
struct evictionPoolEntry {
    // 空闲时间
    // LRU 算法 - 用于记录待淘汰 key 的空闲时间
    // LFU 算法 - 用于记录待淘汰 key 的访问频率
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */

    // 键
    sds key;                    /* Key name. */

    // 键名的缓存 SDS 对象。
    sds cached;                 /* Cached SDS object for key name. */

    // 键所在的 DB
    int dbid;                   /* Key DB number. */
};

static struct evictionPoolEntry *EvictionPoolLRU;

/* ----------------------------------------------------------------------------
 * Implementation of eviction, aging and LRU
 * --------------------------------------------------------------------------*/

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures.
 *
 * 获得以秒为单位的 LRU 时钟
 */
unsigned int getLRUClock(void) {
    return (mstime() / LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This function is used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a system call. */
unsigned int LRU_CLOCK(void) {
    unsigned int lruclock;
    // 为了均衡性能和精度，设计了以下两个分支。
    // 1 Redis 以固定频率调用 getLRUClock 函数，使用系统调用获取全局时钟值，然后将该时钟值赋值给全局变量 server.lruclock。当要获取时钟时，直接从全局变量中获取就行，节省了系统调用的开销。
    // 2 针对频率设置过大，可能会导致非常不精确，此时直接调用系统函数获取 LRU 时间

    if (1000 / server.hz <= LRU_CLOCK_RESOLUTION) {
        atomicGet(server.lruclock, lruclock);
    } else {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm.
 * 计算给定的对象 o 的空闲时间
 */
unsigned long long estimateObjectIdleTime(robj *o) {

    // 获取全局 LRU 时钟
    unsigned long long lruclock = LRU_CLOCK();

    // 计算空闲时间
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) *
               LRU_CLOCK_RESOLUTION;
    }
}

/* LRU approximation algorithm
 *
 * Redis uses an approximation of the LRU algorithm that runs in constant
 * memory. Every time there is a key to expire, we sample N keys (with
 * N very small, usually in around 5) to populate a pool of best keys to
 * evict of M keys (the pool size is defined by EVPOOL_SIZE).
 *
 * The N keys sampled are added in the pool of good keys to expire (the one
 * with an old access time) if they are better than one of the current keys
 * in the pool.
 *
 * After the pool is populated, the best key we have in the pool is expired.
 * However note that we don't remove keys from the pool when they are deleted
 * so the pool may contain keys that no longer exist.
 *
 * When we try to evict a key, and all the entries in the pool don't exist
 * we populate it again. This time we'll be sure that the pool has at least
 * one key that can be evicted, if there is at least one key that can be
 * evicted in the whole database. */

/* Create a new eviction pool.
 *
 * 初始化待淘汰集合
 */
void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;

    // 默认待淘汰集合的大小为 16，也就是可以保存 16 个待淘汰的候选键值对。
    ep = zmalloc(sizeof(*ep) * EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL, EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictionPoolLRU = ep;
}

/* This is an helper function for performEvictions(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time bigger than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * 这是 performEvictions() 的辅助函数
 *
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right.
 *
 * 我们按升序插入键，因此空闲时间较短的键位于左侧，空闲时间较长的键位于右侧。
 *
 */

void evictionPoolPopulate(int dbid, dict *sampledict, dict *keydict, struct evictionPoolEntry *pool) {
    int j, k, count;

    // 采样后的集合，大小为maxmemory_samples，默认值为 5
    dictEntry *samples[server.maxmemory_samples];

    // 调用 dictGetSomeKeys 函数，从待采样的哈希表中随机获取一定数量的 key
    count = dictGetSomeKeys(sampledict, samples, server.maxmemory_samples);

    // 根据实际采样到的键值对数量 count ，执行一个循环，判断每个选出的键值对，是否可以放入到待淘汰键的集合
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        // 取出采样到的键值对
        de = samples[j];
        key = dictGetKey(de);

        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object. */
        // 如果非 ttl 淘汰策略
        if (server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL) {
            if (sampledict != keydict) de = dictFind(keydict, key);
            // 值对象
            o = dictGetVal(de);
        }

        /* Calculate the idle time according to the policy. This is called
         * idle just because the code initially handled LRU, but is in fact
         * just a score where an higher score means better candidate. */
        // 如果是 LRU 淘汰策略
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LRU) {
            // 调用 estimateObjectIdleTime 函数，计算在采样集合中的每个键值对的空闲时间
            idle = estimateObjectIdleTime(o);

            // 如果是 LFU 淘汰策略
        } else if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            /* When we use an LRU policy, we sort the keys by idle time
             * so that we expire keys starting from greater idle time.
             * 当我们使用 LRU 策略时，我们按空闲时间对key进行排序，以便我们从更大的空闲时间开始使key过期。
             *
             * However when the policy is an LFU one, we have a frequency
             * estimation, and we want to evict keys with lower frequency
             * first. So inside the pool we put objects using the inverted
             * frequency subtracting the actual frequency to the maximum
             * frequency of 255.
             * 然而，当策略是 LFU 策略时，我们有一个频率估计，我们希望首先驱逐频率较低的键。
             * 因此，在池中，我们使用反转频率减去实际频率到最大频率 255 放置对象。
             *
             */
            idle = 255 - LFUDecrAndReturn(o);

            // 如果是 ttl 淘汰策略
        } else if (server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
            /* In this case the sooner the expire the better. */
            idle = ULLONG_MAX - (long) dictGetVal(de);
        } else {
            serverPanic("Unknown eviction policy in evictionPoolPopulate()");
        }

        /* Insert the element inside the pool.
         * 将元素插入池中。
         *
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time.
         *
         * 首先，找到空闲时间小于我们空闲时间的第一个空桶或第一个填充桶。
         *
         */
        // 遍历待淘汰的候选键值对集合。在遍历过程中，它会尝试把采样的每一个键值对插入 EvictionPoolLRU 数组。
        k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle)
            k++;

        /*
         * 采样的键值对插入 evictionPoolEntry 数组，取决于以下两个条件之一：
         *
         * 1. 它能在数组中找到一个尚未插入键值对的空位；
         * 2. 它能在数组中找到一个空闲时间小于采样键值对空闲时间的键值对。
         *
         * 这两个条件有一个成立的话，evictionPoolPopulate 函数就可以把采样键值对插入 EvictionPoolLRU 数组。
         * 等所有采样键值对都处理完后，evictionPoolPopulate 函数就完成对待淘汰候选键值对集合的更新了。
         */
        if (k == 0 && pool[EVPOOL_SIZE - 1].key != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
        } else {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            if (pool[EVPOOL_SIZE - 1].key == NULL) {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                sds cached = pool[EVPOOL_SIZE - 1].cached;
                memmove(pool + k + 1, pool + k,
                        sizeof(pool[0]) * (EVPOOL_SIZE - k - 1));
                pool[k].cached = cached;
            } else {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool, pool + 1, sizeof(pool[0]) * k);
                pool[k].cached = cached;
            }
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        int klen = sdslen(key);
        if (klen > EVPOOL_CACHED_SDS_SIZE) {
            pool[k].key = sdsdup(key);
        } else {
            memcpy(pool[k].cached, key, klen + 1);
            sdssetlen(pool[k].cached, klen);
            pool[k].key = pool[k].cached;
        }
        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }
}

/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.
 * 最经常使用
 *
 * We have 24 total bits of space in each object in order to implement
 * an LFU (Least Frequently Used) eviction policy, since we re-use the
 * LRU field for this purpose.
 *
 * We split the 24 bits into two fields:
 *
 *          16 bits      8 bits
 *     +----------------+--------+
 *     + Last decr time | LOG_C  |
 *     +----------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "decrement time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter is halved if it has an high value, or just decremented if it
 * has a low value.
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at COUNTER_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of COUNTER_INIT_VAL
 * when incrementing the key, so that keys starting at COUNTER_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access.
 *
 * During decrement, the value of the logarithmic counter is halved if
 * its current value is greater than two times the COUNTER_INIT_VAL, otherwise
 * it is just decremented by one.
 * --------------------------------------------------------------------------*/

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation.
 *
 * 以分钟为单位返回当前时间，仅取最低有效 16 位。
 *
 * 注意，取的是缓存值。
 *
 */
unsigned long LFUGetTimeInMinutes(void) {
    return (server.unixtime / 60) & 65535;
}

/* Given an object last access time, compute the minimum number of minutes
 * that elapsed since the last access. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
unsigned long LFUTimeElapsed(unsigned long ldt) {
    unsigned long now = LFUGetTimeInMinutes();
    if (now >= ldt) return now - ldt;
    return 65535 - ldt + now;
}

/* Logarithmically increment a counter. The greater is the current counter value
 * the less likely is that it gets really implemented. Saturate it at 255.
 *
 * 以对数方式递增计算器。当前计数器的值越大，它真正实现的可能性就越小。使它饱和到 255。
 * 这属于非线性递增的方式
 *
 * 使用了这种计算规则后，我们可以通过设置不同的 lfu_log_factor 配置项，来控制计数器值增加的速度，避免 counter 值很快达到 255。
 */
uint8_t LFULogIncr(uint8_t counter) {
    // 如果计算器的值达到 255 ，那么不能再增了
    if (counter == 255) return 255;

    // 获取 (0,1) 范围内的随机数
    double r = (double) rand() / RAND_MAX;

    // 计数器当前值
    // 计数器的初始值默认是 5 （LFU_INIT_VAL 常量设置），而不是 0，这样可以避免数据刚被写入缓存，就因为访问次数少而被立即淘汰。
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;

    // 用计数器当前的值乘以配置项 lfu_log_factor（默认 10） 再加 1，再取其倒数
    double p = 1.0 / (baseval * server.lfu_log_factor + 1);

    // p > r 时，计算器才会 +1
    // counter 递增的概率取决于3个因素
    // 1 counter 值越大，递增概率越低
    // 2 lfu-log-factor 设置越大，递增概率越低
    // 3 随机值越大，递增概率越低
    if (r < p) counter++;

    return counter;
}

/* If the object decrement time is reached decrement the LFU counter but
 * do not update LFU fields of the object, we update the access time
 * and counter in an explicit way when the object is really accessed.
 * And we will times halve the counter according to the times of
 * elapsed time than server.lfu_decay_time.
 * Return the object frequency counter.
 *
 * This function is used in order to scan the dataset for the best object
 * to fit: as we check for the candidate, we incrementally decrement the
 * counter of the scanned objects if needed.
 *
 * 在一些场景下，有些数据在短时间内被大量访问后就不会再被访问了。此时，再按照访问次数来筛选的话，就不是特别好。
 * 为此，Redis 在实现 LFU 策略时，还设计了一个 counter 值的衰减机制。
 *
 *
 *
 */
unsigned long LFUDecrAndReturn(robj *o) {
    // 1 取高 16 位，获取最近一次访问时间戳
    unsigned long ldt = o->lru >> 8;

    // 2 获取访问次数
    unsigned long counter = o->lru & 255;

    // 3 计算当前时间和数据最近一次访问时间的差值，并把该差值换算成以分钟为单位
    // 然后，把差值除以 衰减因子，得到的结果就是 counter 要衰减的值
    // 即：N 分钟 / lfu_decay_time = 衰减值 (注意：递减因子越大，衰减效果越弱)
    unsigned long num_periods = server.lfu_decay_time ? LFUTimeElapsed(ldt) / server.lfu_decay_time : 0;

    // 4 衰退访问次数
    if (num_periods)
        // 访问次数递减 num_periods
        counter = (num_periods > counter) ? 0 : counter - num_periods;

    return counter;
}

/* We don't want to count AOF buffers and slaves output buffers as
 * used memory: the eviction should use mostly data size. This function
 * returns the sum of AOF and slaves buffer. */
size_t freeMemoryGetNotCountedMemory(void) {
    size_t overhead = 0;
    int slaves = listLength(server.slaves);

    if (slaves) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = listNodeValue(ln);
            overhead += getClientOutputBufferMemoryUsage(slave);
        }
    }
    if (server.aof_state != AOF_OFF) {
        overhead += sdsalloc(server.aof_buf) + aofRewriteBufferSize();
    }
    return overhead;
}

/* Get the memory status from the point of view of the maxmemory directive:
 * if the memory used is under the maxmemory setting then C_OK is returned.
 * Otherwise, if we are over the memory limit, the function returns
 * C_ERR.
 *
 * The function may return additional info via reference, only if the
 * pointers to the respective arguments is not NULL. Certain fields are
 * populated only when C_ERR is returned:
 *
 *  'total'     total amount of bytes used.
 *              (Populated both for C_ERR and C_OK)
 *
 *  'logical'   the amount of memory used minus the slaves/AOF buffers.
 *              (Populated when C_ERR is returned)
 *
 *  'tofree'    the amount of memory that should be released
 *              in order to return back into the memory limits.
 *              (Populated when C_ERR is returned)
 *
 *  'level'     this usually ranges from 0 to 1, and reports the amount of
 *              memory currently used. May be > 1 if we are over the memory
 *              limit.
 *              (Populated both for C_ERR and C_OK)
 */
// 计算使用的内存量以及需要释放的内存量
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level) {
    size_t mem_reported, mem_used, mem_tofree;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    // 检查是否超过了内存使用限制
    mem_reported = zmalloc_used_memory();
    if (total) *total = mem_reported;

    /* We may return ASAP if there is no need to compute the level. */
    // 已用内存未超过 maxmeomory
    int return_ok_asap = !server.maxmemory || mem_reported <= server.maxmemory;
    if (return_ok_asap && !level) return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    // 将用于主从复制的复制缓冲区大小和 AOF 缓冲区大小，从已使用内存量中扣除
    mem_used = mem_reported;
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;

    /* Compute the ratio of memory usage. */
    if (level) {
        if (!server.maxmemory) {
            *level = 0;
        } else {
            *level = (float) mem_used / (float) server.maxmemory;
        }
    }

    if (return_ok_asap) return C_OK;

    /* Check if we are still over the memory limit. */
    if (mem_used <= server.maxmemory) return C_OK;

    /* Compute how much memory we need to free. */
    // 计算需要释放的内存量
    mem_tofree = mem_used - server.maxmemory;

    if (logical) *logical = mem_used;
    if (tofree) *tofree = mem_tofree;

    return C_ERR;
}

/* Return 1 if used memory is more than maxmemory after allocating more memory,
 * return 0 if not. Redis may reject user's requests or evict some keys if used
 * memory exceeds maxmemory, especially, when we allocate huge memory at once. */
int overMaxmemoryAfterAlloc(size_t moremem) {
    if (!server.maxmemory) return 0; /* No limit. */

    /* Check quickly. */
    size_t mem_used = zmalloc_used_memory();
    if (mem_used + moremem <= server.maxmemory) return 0;

    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;
    return mem_used + moremem > server.maxmemory;
}

/* The evictionTimeProc is started when "maxmemory" has been breached and
 * could not immediately be resolved.  This will spin the event loop with short
 * eviction cycles until the "maxmemory" condition has resolved or there are no
 * more evictable items.  */
static int isEvictionProcRunning = 0;

/*
 * 释放内存的时间事件
 */
static int evictionTimeProc(
        struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    if (performEvictions() == EVICT_RUNNING) return 0;  /* keep evicting */

    /* For EVICT_OK - things are good, no need to keep evicting.
     * For EVICT_FAIL - there is nothing left to evict.  */
    isEvictionProcRunning = 0;
    return AE_NOMORE;
}

/* Check if it's safe to perform evictions.
 *   Returns 1 if evictions can be performed
 *   Returns 0 if eviction processing should be skipped
 */
static int isSafeToPerformEvictions(void) {
    /* - There must be no script in timeout condition.
     * - Nor we are loading data right now.  */
    if (server.lua_timedout || server.loading) return 0;

    /* By default replicas should ignore maxmemory
     * and just be masters exact copies. */
    if (server.masterhost && server.repl_slave_ignore_maxmemory) return 0;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 0;

    return 1;
}

/* Algorithm for converting tenacity (0-100) to a time limit.
 *
 * 将韧性 (0-100) 转换为时间限制的算法。
 *
 * 目的是控制大量淘汰内存空间阻塞客户端的时间。
 */
static unsigned long evictionTimeLimitUs() {
    serverAssert(server.maxmemory_eviction_tenacity >= 0);
    serverAssert(server.maxmemory_eviction_tenacity <= 100);

    if (server.maxmemory_eviction_tenacity <= 10) {
        /* A linear progression from 0..500us */
        return 50uL * server.maxmemory_eviction_tenacity;
    }

    if (server.maxmemory_eviction_tenacity < 100) {
        /* A 15% geometric progression, resulting in a limit of ~2 min at tenacity==99  */
        return (unsigned long) (500.0 * pow(1.15, server.maxmemory_eviction_tenacity - 10.0));
    }

    return ULONG_MAX;   /* No limit to eviction time */
}

/* Check that memory usage is within the current "maxmemory" limit.  If over
 * "maxmemory", attempt to free memory by evicting data (if it's safe to do so).
 *
 * 检查内存使用量是否在当前的 maxmemory 限制内，如果超过 maxmemory 则尝试通过淘汰数据来释放内存。
 *
 * It's possible for Redis to suddenly be significantly over the "maxmemory"
 * setting.  This can happen if there is a large allocation (like a hash table
 * resize) or even if the "maxmemory" setting is manually adjusted.  Because of
 * this, it's important to evict for a managed period of time - otherwise Redis
 * would become unresponsive while evicting.
 *
 * The goal of this function is to improve the memory situation - not to
 * immediately resolve it.  In the case that some items have been evicted but
 * the "maxmemory" limit has not been achieved, an aeTimeProc will be started
 * which will continue to evict items until memory limits are achieved or
 * nothing more is evictable.
 *
 * This should be called before execution of commands.  If EVICT_FAIL is
 * returned, commands which will result in increased memory usage should be
 * rejected.
 *
 * 如果返回 EVICT_FAIL，则应拒绝会导致内存使用量增加的命令
 *
 * Returns:
 *   EVICT_OK       - memory is OK or it's not possible to perform evictions now
 *   EVICT_RUNNING  - memory is over the limit, but eviction is still processing
 *   EVICT_FAIL     - memory is over the limit, and there's nothing to evict
 * */
int performEvictions(void) {
    if (!isSafeToPerformEvictions()) return EVICT_OK;

    int keys_freed = 0;
    size_t mem_reported, mem_tofree;
    long long mem_freed; /* May be negative */
    mstime_t latency, eviction_latency;
    long long delta;
    int slaves = listLength(server.slaves);
    int result = EVICT_FAIL;

    // 计算待释放的内存空间
    if (getMaxmemoryState(&mem_reported, NULL, &mem_tofree, NULL) == C_OK)
        return EVICT_OK;

    // 如果需要释放内存，但内存淘汰策略又禁止，那边返回 EVICT_FAIL
    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION)
        return EVICT_FAIL;  /* We need to free memory, but policy forbids. */

    // 转换时间限制算法
    unsigned long eviction_time_limit_us = evictionTimeLimitUs();

    mem_freed = 0;

    latencyStartMonitor(latency);

    monotime evictionTimer;
    elapsedStart(&evictionTimer);

    // 当前 Server 使用的内存量，的确已经超出了 maxmemory 的上限，执行循环来淘汰数据释放内存
    while (mem_freed < (long long) mem_tofree) {
        int j, k, i;
        static unsigned int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;

        // 如果是 LRU 、LFU、ttl 淘汰策略
        if (server.maxmemory_policy & (MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU) ||
            server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {

            // 待淘汰键集合
            struct evictionPoolEntry *pool = EvictionPoolLRU;

            while (bestkey == NULL) {
                unsigned long total_keys = 0, keys;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB. */
                // 对 Redis Server 上的每个数据库都执行，默认最多挑选出 16 个待淘汰的键
                for (i = 0; i < server.dbnum; i++) {
                    db = server.db + i;

                    // 根据淘汰策略，决定使用全局哈希表还是设置了过期时间的key的哈希表
                    dict = (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ?
                           db->dict : db->expires;

                    // 元素数量
                    if ((keys = dictSize(dict)) != 0) {
                        // 更新待淘汰的候选键集合
                        // 随机采样键值对，并插入到待淘汰集合 EvictionPoolLRU 中
                        evictionPoolPopulate(i, dict, db->dict, pool);
                        total_keys += keys;
                    }
                }

                if (!total_keys) break; /* No keys to evict. */

                /* Go backward from best to worst element to evict. */
                /*
                 * 因为 evictionPoolPopulate 函数已经更新了 EvictionPoolLRU 数组，而且这个数组里面的 key，是按照空闲时间从小到大排好序了。
                 *
                 * 遍历一次 EvictionPoolLRU 数组，从数组的最后一个 key 开始选择，如果选到的 key 不是空值，那么就把它作为最终淘汰的 key。
                 */
                // 从数组最后一个 key 开始查找
                for (k = EVPOOL_SIZE - 1; k >= 0; k--) {
                    // 当前 key 为空值，则查找下一个 key
                    if (pool[k].key == NULL) continue;
                    bestdbid = pool[k].dbid;

                    // 从全局哈希表或是expire哈希表中，获取当前key对应的键值对
                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        de = dictFind(server.db[pool[k].dbid].dict,
                                      pool[k].key);
                    } else {
                        de = dictFind(server.db[pool[k].dbid].expires,
                                      pool[k].key);
                    }

                    /* Remove the entry from the pool. */
                    // 将当前key从EvictionPoolLRU数组删除
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    //如果当前key对应的键值对不为空，选择当前key为被淘汰的key
                    if (de) {
                        bestkey = dictGetKey(de);
                        break;

                        // 否则，继续查找下一个 key
                    } else {
                        /* Ghost... Iterate again. */
                    }
                }
            }
        }

            /* volatile-random and allkeys-random policy */
            // 如果是 random 淘汰策略
        else if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM) {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            // 所有的 db 中随机选择
            for (i = 0; i < server.dbnum; i++) {
                j = (++next_db) % server.dbnum;
                db = server.db + j;
                dict = (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ?
                       db->dict : db->expires;
                if (dictSize(dict) != 0) {
                    de = dictGetRandomKey(dict);
                    bestkey = dictGetKey(de);
                    bestdbid = j;
                    break;
                }
            }
        }

        /* Finally remove the selected key. */
        // 选到了被淘汰的 key ，根据 Redis Server 的惰性删除配置，来执行同步删除或异步删除
        if (bestkey) {
            db = server.db + bestdbid;

            // 创建字符串对象
            robj *keyobj = createStringObject(bestkey, sdslen(bestkey));

            //将删除key的信息传递给从库和AOF文件
            propagateExpire(db, keyobj, server.lazyfree_lazy_eviction);
            /* We compute the amount of memory freed by db*Delete() alone.
             * It is possible that actually the memory needed to propagate
             * the DEL in AOF and replication link is greater than the one
             * we are freeing removing the key, but we can't account for
             * that otherwise we would never exit the loop.
             *
             * Same for CSC invalidation messages generated by signalModifiedKey.
             *
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */

            // 删除前，计算当前使用的内存量
            delta = (long long) zmalloc_used_memory();
            latencyStartMonitor(eviction_latency);

            // 执行异步删除
            if (server.lazyfree_lazy_eviction)
                dbAsyncDelete(db, keyobj);

            // 执行同步删除
            else
                dbSyncDelete(db, keyobj);


            latencyEndMonitor(eviction_latency);
            latencyAddSampleIfNeeded("eviction-del", eviction_latency);

            // 删除后，计算此时的内存使用量，并计算删除操作导致的内存使用量差值。
            // 这个差值就是通过删除操作而被释放的内存量
            delta -= (long long) zmalloc_used_memory();

            // 更新已释放内存大小
            mem_freed += delta;


            server.stat_evictedkeys++;
            signalModifiedKey(NULL, db, keyobj);
            notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted",
                                keyobj, db->id);
            decrRefCount(keyobj);
            keys_freed++;

            // 每删除 16 个 key 后，统计下当前内存使用量
            if (keys_freed % 16 == 0) {
                /* When the memory to free starts to be big enough, we may
                 * start spending so much time here that is impossible to
                 * deliver data to the replicas fast enough, so we force the
                 * transmission here inside the loop. */
                if (slaves) flushSlavesOutputBuffers();

                /* Normally our stop condition is the ability to release
                 * a fixed, pre-computed amount of memory. However when we
                 * are deleting objects in another thread, it's better to
                 * check, from time to time, if we already reached our target
                 * memory, since the "mem_freed" amount is computed only
                 * across the dbAsyncDelete() call, while the thread can
                 * release the memory all the time. */
                // 如果是惰性释放，计算当前内存使用量是否不超过最大内存量，符合条件就停止淘汰数据流程。
                if (server.lazyfree_lazy_eviction) {
                    if (getMaxmemoryState(NULL, NULL, NULL, NULL) == C_OK) {
                        break;
                    }
                }

                /* After some time, exit the loop early - even if memory limit
                 * hasn't been reached.  If we suddenly need to free a lot of
                 * memory, don't want to spend too much time here.
                 *
                 * 即使没有达到内存限制，但超过限制时间了，那就新增时间事件，然后结束循环。
                 *
                 */
                if (elapsedUs(evictionTimer) > eviction_time_limit_us) {
                    // We still need to free memory - start eviction timer proc
                    if (!isEvictionProcRunning) {
                        isEvictionProcRunning = 1;

                        // 新增释放内存的时间事件
                        aeCreateTimeEvent(server.el, 0,
                                          evictionTimeProc, NULL, NULL);
                    }
                    break;
                }
            }
        } else {
            goto cant_free; /* nothing to free... */
        }
    }
    /* at this point, the memory is OK, or we have reached the time limit */
    result = (isEvictionProcRunning) ? EVICT_RUNNING : EVICT_OK;

    cant_free:
    if (result == EVICT_FAIL) {
        /* At this point, we have run out of evictable items.  It's possible
         * that some items are being freed in the lazyfree thread.  Perform a
         * short wait here if such jobs exist, but don't wait long.  */
        if (bioPendingJobsOfType(BIO_LAZY_FREE)) {
            usleep(eviction_time_limit_us);
            if (getMaxmemoryState(NULL, NULL, NULL, NULL) == C_OK) {
                result = EVICT_OK;
            }
        }
    }

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle", latency);
    return result;
}

