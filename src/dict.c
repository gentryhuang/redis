/* Hash Tables Implementation.
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict.h"
#include "zmalloc.h"
#include "redisassert.h"

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * 通过 dictEnableResize() 和 dictDisableResize() 两个函数，程序可以手动地允许或阻止哈希表进行 rehash ，
 * 这在 Redis 使用子进程进行保存操作时，可以有效地利用 copy-on-write 机制。
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: a hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio.
 *
 * 需要注意的是，并非所有 rehash 都会被 dictDisableResize 阻止：
 * 如果已使用节点的数量和字典大小之间的比率，大于字典强制 rehash 比率 dict_force_resize_ratio ，那么 rehash 仍然会（强制）进行。
 *
 */

// 指示字典是否启用 rehash 的标识
static int dict_can_resize = 1;

// 负载因子：used /size
// 强制刷新的阈值
static unsigned int dict_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);

static unsigned long _dictNextPower(unsigned long size);

static long _dictKeyIndex(dict *ht, const void *key, uint64_t hash, dictEntry **existing);

static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

static uint8_t dict_hash_function_seed[16];

void dictSetHashFunctionSeed(uint8_t *seed) {
    memcpy(dict_hash_function_seed, seed, sizeof(dict_hash_function_seed));
}

uint8_t *dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t dictGenHashFunction(const void *key, int len) {
    return siphash(key, len, dict_hash_function_seed);
}

uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len) {
    return siphash_nocase(buf, len, dict_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

/**
 * 重置（或初始化）给定哈希表的各项属性值
 * @param ht
 */
static void _dictReset(dictht *ht) {
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

/* Create a new hash table
 *
 * 创建一个新的字典
 *
 * T = O(1)
 */
dict *dictCreate(dictType *type,
                 void *privDataPtr) {

    // 分配字典内存
    dict *d = zmalloc(sizeof(*d));

    // 初始化字典的哈希表
    _dictInit(d, type, privDataPtr);

    return d;
}

/* Initialize the hash table */
/**
 * 初始化哈希表
 *
 * @param d
 * @param type
 * @param privDataPtr
 * @return
 */
int _dictInit(dict *d, dictType *type,
              void *privDataPtr) {

    // 初始化两个哈希表的各项属性，但暂时还不分配内存给哈希表数组
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);

    // 设置类型特定函数
    d->type = type;

    // 设置私有数据
    d->privdata = privDataPtr;

    // 设置哈希表 rehash 状态
    d->rehashidx = -1;

    // 默认暂停 rehash
    d->pauserehash = 0;

    return DICT_OK;
}

/* Resize the table to the minimal size that contains all the elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1 */
/**
 * 调整字典所包含元素的最小大小
 * @param d
 * @return
 */
int dictResize(dict *d) {
    unsigned long minimal;

    // 不能在关闭 rehash 或者正在 rehash 的时候调用
    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;

    // 基于当前 ht[0] 存储的元素个数，进行缩容
    minimal = d->ht[0].used;
    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;

    // 调整字典的大小
    return dictExpand(d, minimal);
}

/* Expand or create the hash table,
 * when malloc_failed is non-NULL, it'll avoid panic if malloc fails (in which case it'll be set to 1).
 * Returns DICT_OK if expand was performed, and DICT_ERR if skipped. */
/**
 * 创建一个哈希表，并根据字典的情况进行分别处理：
 * 1. 如果字典的 0 号哈希表为空，那么将新哈希表设置为 0 号哈希表
 * 2. 如果字典的 0 号哈希表非空，那么将新哈希表设置为 1 号哈希表，
 *    并打开字典的 rehash 标识，使得程序可以开始对字典进行 rehash
 *
 * @param d
 * @param size
 * @param malloc_failed
 * @return
 */
int _dictExpand(dict *d, unsigned long size, int *malloc_failed) {
    if (malloc_failed) *malloc_failed = 0;


    // 1 不能在字典正在 rehash 时进行 &  size 的值也不能小于 0 号哈希表的当前已使用节点
    /* the size is invalid if it is smaller than the number of
     * elements already inside the hash table */
    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    // 新哈希表
    dictht n; /* the new hash table */

    // 2 根据 size 参数，计算哈希表目标大小
    unsigned long realsize = _dictNextPower(size);

    // 3 计算的哈希表目标大小 = 当前字典 ht[0] 的大小，忽略本次扩展
    /* Rehashing to the same table size is not useful. */
    if (realsize == d->ht[0].size) return DICT_ERR;

    // 4 为新哈希表分配空间
    /* Allocate the new hash table and initialize all pointers to NULL */
    n.size = realsize;
    n.sizemask = realsize - 1;
    if (malloc_failed) {
        n.table = ztrycalloc(realsize * sizeof(dictEntry *));
        *malloc_failed = n.table == NULL;
        if (*malloc_failed)
            return DICT_ERR;
    } else
        n.table = zcalloc(realsize * sizeof(dictEntry *));

    n.used = 0;

    /* Is this the first initialization? If so it's not really a rehashing
     * we just set the first hash table so that it can accept keys. */
    // 5 如果 0 号哈希表为空，那么这是一次初始化：
    // 程序将新哈希表赋给 0 号哈希表的指针，然后字典就可以开始处理键值对了。
    if (d->ht[0].table == NULL) {
        d->ht[0] = n;
        return DICT_OK;
    }

    /* Prepare a second hash table for incremental rehashing */
    // 6 如果 0 号哈希表非空，那么这是一次 rehash ，程序将新哈希表设置为 1 号哈希表，
    // 并将字典的 rehash 标识打开，让程序可以开始对字典进行 rehash
    d->ht[1] = n;
    d->rehashidx = 0;
    return DICT_OK;
}

/**
 * return DICT_ERR if expand was not performed
 *
 * Redis 通过该函数实现扩容/缩容
 *
 * @param d 要扩容的 Hash 表
 * @param size 要扩到的容量
 * @return
 */
int dictExpand(dict *d, unsigned long size) {
    return _dictExpand(d, size, NULL);
}

/* return DICT_ERR if expand failed due to memory allocation failure */
int dictTryExpand(dict *d, unsigned long size) {
    int malloc_failed;
    _dictExpand(d, size, &malloc_failed);
    return malloc_failed ? DICT_ERR : DICT_OK;
}

/* Performs N steps of incremental rehashing. Returns 1 if there are still
 * keys to move from the old to the new hash table, otherwise 0 is returned.
 *
 * Note that a rehashing step consists in moving a bucket (that may have more
 * than one key as we use chaining) from the old to the new hash table, however
 * since part of the hash table may be composed of empty spaces, it is not
 * guaranteed that this function will rehash even a single bucket, since it
 * will visit at max N*10 empty buckets in total, otherwise the amount of
 * work it does would be unbound and the function may block for a long time.
 *
 * 执行的 bucket 迁移个数，主要由传入的循环次数变量 n 来决定。但凡 Redis 要进行 rehash 操作，最终都会调用 dictRehash 函数。
 */
int dictRehash(dict *d, int n) {

    // 1 限制访问的最大空桶数，避免了对 Redis 性能的影响
    // 即 如果 rehash 过程中已经跳过了 empty_visits 数量的空 bucket，那么本次 dictRehash 的执行就会直接返回了，
    // 而不会再查找 bucket。这样设计的目的，也是为了避免本次 rehash 的执行一直无法结束，影响正常的请求处理。
    int empty_visits = n * 10; /* Max number of empty buckets to visit. */

    // 2 只可以在 rehash 进行中时执行
    if (!dictIsRehashing(d)) return 0;

    // 3 主循环，根据要拷贝的bucket数量n，循环n次后停止或ht[0]中的数据迁移完停止
    // 将 ht[0] 中的数据分批次 n 拷贝到 ht[1]
    while (n-- && d->ht[0].used != 0) {
        dictEntry *de, *nextde;

        /* Note that rehashidx can't overflow as we are sure there are more
         * elements because ht[0].used != 0 */
        assert(d->ht[0].size > (unsigned long) d->rehashidx);

        // 如果当前要迁移的 bucket 中没有元素
        while (d->ht[0].table[d->rehashidx] == NULL) {
            // 递增 rehashidx 的值
            d->rehashidx++;

            // 限制访问空桶的次数
            if (--empty_visits == 0) return 1;
        }

        // 获得 0 号哈希表中的第 d->rehashidx 项
        de = d->ht[0].table[d->rehashidx];

        // 如果 rehashidx 指向的 bucket 不为空，则将桶中的所有键从旧的hash HT移到新的hash HT
        while (de) {

            uint64_t h;

            // 获得同一个 bucket 中下一个哈希项
            nextde = de->next;

            // 根据扩容后的哈希表ht[1]大小，计算当前哈希项在扩容后哈希表中的bucket位置
            h = dictHashKey(d, de->key) & d->ht[1].sizemask;

            // 将当前哈希项添加到扩容后的哈希表ht[1]中
            de->next = d->ht[1].table[h];
            d->ht[1].table[h] = de;

            // 减少当前哈希表的哈希项个数
            d->ht[0].used--;

            // 增加扩容后哈希表的哈希项个数
            d->ht[1].used++;

            // 指向下一个哈希项
            de = nextde;
        }

        // 如果当前bucket中已经没有哈希项了，将该bucket置为NULL
        d->ht[0].table[d->rehashidx] = NULL;

        // 将rehash加1，下一次将迁移下一个bucket中的元素
        d->rehashidx++;
    }


    // 4 判断 ht[0] 的数据是否迁移完成
    if (d->ht[0].used == 0) {
        // ht[0] 迁移完成后，释放 ht[0] 内存空间
        zfree(d->ht[0].table);

        // 让 ht[0]指向ht[1] ，以便接受正常的请求
        d->ht[0] = d->ht[1];

        // 重置ht[1]的大小为0
        _dictReset(&d->ht[1]);

        // 设置全局哈希表的 rehashidx 标识为-1，表示rehash结束
        d->rehashidx = -1;

        // 返回0，表示ht[0]中所有元素都迁移完
        return 0;
    }

    // 返回1，表示ht[0]中仍然有元素没有迁移完
    return 1;
}

long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (((long long) tv.tv_sec) * 1000) + (tv.tv_usec / 1000);
}

/* Rehash in ms+"delta" milliseconds. The value of "delta" is larger 
 * than 0, and is smaller than 1 in most cases. The exact upper bound 
 * depends on the running time of dictRehash(d,100).
 *
 *  在给定毫秒数内，以 100 步为单位进行 rehash，对字典进行 rehash 。
 *  该方法由 Redis 主动触发，每隔 100ms 执行一次迁移操作。
 *  具体：
 *  1 以 100 个桶为基本单位迁移数据，并限制如果一次操作耗时超时 1ms 就结束本次任务，待下次再次触发迁移
 *  2 定时 rehash 只会迁移全局哈希表中的数据，不会定时迁移 Hash/Set/Sorted Set 下的哈希表的数据，这些哈希表只会在操作数据时做实时的渐进式 rehash。
 *    全局的hash表在server.db里面的dict，这里db有16个，也就是我们平时说的redis默认有16个db，每一个db里面都有全局dict。
 */
int dictRehashMilliseconds(dict *d, int ms) {
    if (d->pauserehash > 0) return 0;

    long long start = timeInMilliseconds();
    int rehashes = 0;

    // 以 100 个桶为基本单位迁移数据
    while (dictRehash(d, 100)) {
        rehashes += 100;

        // 限制每次操作耗时超过 1ms 就结束本地任务，待下次再次触发迁移
        if (timeInMilliseconds() - start > ms) break;
    }
    return rehashes;
}

/* This function performs just a step of rehashing, and only if hashing has
 * not been paused for our hash table. When we have iterators in the
 * middle of a rehashing we can't mess with the two hash tables otherwise
 * some element can be missed or duplicated.
 *
 * This function is called by common lookup or update operations in the
 * dictionary so that the hash table automatically migrates from H1 to H2
 * while it is actively used.
 *
 * 实现了每次只对一个 bucket 执行 rehash
 * 注意：当字典在进行 rehash 的过程中，此时进行增、删、查请求时（rehash 和基本操作都是主线程处理的），请求操作的哈希表：
 * - 新增 dictAddRaw ，ht[1]
 * - 删除 dictGenericDelete ，先从 ht[0]找，没找到再从 ht[1]
 * - 查询 dictFind ，先从 ht[0]找，没找到再从 ht[1]
 */
static void _dictRehashStep(dict *d) {
    // 给 dictRehash 传入的循环次数参数为1，表明每迁移完一个bucket ，就执行正常操作
    if (d->pauserehash == 0) dictRehash(d, 1);
}

/* Add an element to the target hash table */
/**
 * 往 Hash 表中添加一个键值对
 *
 * @param d 字典
 * @param key key
 * @param val value
 * @return
 */
int dictAdd(dict *d, void *key, void *val) {

    // 尝试添加键到字典，并返回包含了这个键的新的哈希项
    dictEntry *entry = dictAddRaw(d, key, NULL);

    // 如果 key 存在则添加失败
    if (!entry) return DICT_ERR;

    // 键不存在，设置节点的值
    dictSetVal(d, entry, val);

    // 返回添加成功
    return DICT_OK;
}

/* Low level add or find:
 * This function adds the entry but instead of setting a value returns the
 * dictEntry structure to the user, that will make sure to fill the value
 * field as they wish.
 *
 * This function is also directly exposed to the user API to be called
 * mainly in order to store non-pointers inside the hash value, example:
 *
 * entry = dictAddRaw(dict,mykey,NULL);
 * if (entry != NULL) dictSetSignedIntegerVal(entry,1000);
 *
 * Return values:
 *
 * If key already exists NULL is returned, and "*existing" is populated
 * with the existing entry if existing is not NULL.
 *
 * If key was added, the hash entry is returned to be manipulated by the caller.
 */
/**
 * 往 Redis 中增加键值对，这里只是尝试创建键对应的哈希项
 *
 * @param d
 * @param key
 * @param existing
 * @return
 */
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing) {
    long index;
    dictEntry *entry;
    dictht *ht;

    // 1 如果字典正在 rehash ，则执行 rehash 操作
    // 默认每次迁移一个 Bucket
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 2 获取新元素的索引，如果 key 已经存在，则返回 -1。返回给上层 NULL
    /* Get the index of the new element, or -1 if the element already exists. */
    if ((index = _dictKeyIndex(d, key, dictHashKey(d, key), existing)) == -1)
        return NULL;

    /* Allocate the memory and store the new entry.
     * Insert the element in top, with the assumption that in a database
     * system it is more likely that recently added entries are accessed
     * more frequently. */
    // 3 判断是否在扩容，在扩容的时候，新增键值对选择 1 号 Hash 表
    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];

    // 4 为新节点分配空间
    entry = zmalloc(sizeof(*entry));

    // 5 todo 链表头插法
    entry->next = ht->table[index];
    ht->table[index] = entry;

    // 更新哈希表已有元素数量
    ht->used++;

    // 6 设置新节点的键
    /* Set the hash entry fields. */
    dictSetKey(d, entry, key);

    // 7 返回新的哈希节点，也就是哈希项
    return entry;
}

/*
 * 添加或覆盖
 * Add or Overwrite:
 * Add an element, discarding the old value if the key already exists.
 * Return 1 if the key was added from scratch, 0 if there was already an
 * element with such key and dictReplace() just performed a value update
 * operation. */
int dictReplace(dict *d, void *key, void *val) {
    dictEntry *entry, *existing, auxentry;

    // 尝试添加 key 对应的哈希项
    /* Try to add the element. If the key does not exists dictAdd will succeed. */
    entry = dictAddRaw(d, key, &existing);

    // 如果 key 对应的哈希项不存在则会新建一个，这里再设置值即可
    if (entry) {
        dictSetVal(d, entry, val);
        return 1;
    }

    // 执行到这里，说明 key 对应的哈希项已经存在，这里直接覆盖即可
    /* Set the new value and free the old one. Note that it is important
     * to do that in this order, as the value may just be exactly the same
     * as the previous one. In this context, think to reference counting,
     * you want to increment (set), and then decrement (free), and not the
     * reverse. */

    // 保存包含原有值的哈希项
    auxentry = *existing;

    // 重置新的值
    dictSetVal(d, existing, val);

    // 释放旧值
    dictFreeVal(d, &auxentry);

    return 0;
}

/* 添加或查找 key 对应的哈希项
 *
 * Add or Find:
 * dictAddOrFind() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information. */
dictEntry *dictAddOrFind(dict *d, void *key) {
    // 定义哈希项
    dictEntry *entry, *existing;

    // 往 Redis 中增加键值对，并返回键对应哈希项
    entry = dictAddRaw(d, key, &existing);

    // 如果 key 已经存在，则返回 existing ，不存在返回新创建的 entry
    return entry ? entry : existing;
}

/* Search and remove an element. This is an helper function for
 * dictDelete() and dictUnlink(), please check the top comment
 * of those functions.
 *
 * 查找并删除包含给定键的节点。参数 nofree 决定是否调用键和值的释放函数，0 表示调用，1 表示不调用。
 *
 * 找到并成功删除返回 DICT_OK ，没找到则返回 DICT_ERR
 *
 * T = O(1)
 */
static dictEntry *dictGenericDelete(dict *d, const void *key, int nofree) {
    uint64_t h, idx;
    dictEntry *he, *prevHe;
    int table;

    // 字典为空
    if (d->ht[0].used == 0 && d->ht[1].used == 0) return NULL;

    // 进行单步 rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 计算 hash 值
    h = dictHashKey(d, key);

    // 删除逻辑可能会涉及到两个 Hash 表
    for (table = 0; table <= 1; table++) {
        // 计算索引值
        idx = h & d->ht[table].sizemask;

        // 指向该索引上的链表
        he = d->ht[table].table[idx];
        prevHe = NULL;

        // 遍历链表上的所有节点
        while (he) {
            // 查找目标节点
            if (key == he->key || dictCompareKeys(d, key, he->key)) {
                /* Unlink the element from the list */
                // 从链表中删除
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht[table].table[idx] = he->next;

                // 释放调用键和值的释放函数？
                if (!nofree) {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                    // 释放节点本身
                    zfree(he);
                }

                // 更新已使用节点数量
                d->ht[table].used--;
                return he;
            }

            prevHe = he;
            he = he->next;
        }

        // 如果执行到这里，说明在 0 号哈希表中找不到给定键
        // 那么根据字典是否正在进行 rehash ，决定要不要查找 1 号哈希表
        if (!dictIsRehashing(d)) break;
    }
    return NULL; /* not found */
}

/* Remove an element, returning DICT_OK on success or DICT_ERR if the
 * element was not found. */
int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht, key, 0) ? DICT_OK : DICT_ERR;
}

/* Remove an element from the table, but without actually releasing
 * the key, value and dictionary entry. The dictionary entry is returned
 * if the element was found (and unlinked from the table), and the user
 * should later call `dictFreeUnlinkedEntry()` with it in order to release it.
 * Otherwise if the key is not found, NULL is returned.
 *
 * This function is useful when we want to remove something from the hash
 * table but want to use its value before actually deleting the entry.
 * Without this function the pattern would require two lookups:
 *
 *  entry = dictFind(...);
 *  // Do something with entry
 *  dictDelete(dictionary,entry);
 *
 * Thanks to this function it is possible to avoid this, and use
 * instead:
 *
 * entry = dictUnlink(dictionary,entry);
 * // Do something with entry
 * dictFreeUnlinkedEntry(entry); // <- This does not need to lookup again.
 */
dictEntry *dictUnlink(dict *ht, const void *key) {
    return dictGenericDelete(ht, key, 1);
}

/* You need to call this function to really free the entry after a call
 * to dictUnlink(). It's safe to call this function with 'he' = NULL. */
void dictFreeUnlinkedEntry(dict *d, dictEntry *he) {
    if (he == NULL) return;
    dictFreeKey(d, he);
    dictFreeVal(d, he);
    zfree(he);
}


/**
 * 删除哈希表上的所有节点，并重置哈希表的各项属性
 *
 * T = O(N)
 *
 * @param d
 * @param ht
 * @param callback
 * @return
 */
/* Destroy an entire dictionary */
int _dictClear(dict *d, dictht *ht, void(callback)(void *)) {
    unsigned long i;

    /* Free all the elements */
    // 遍历整个哈希表
    for (i = 0; i < ht->size && ht->used > 0; i++) {
        dictEntry *he, *nextHe;
        if (callback && (i & 65535) == 0) callback(d->privdata);

        // 获取索引 i 位置的节点结构 dictEntry
        if ((he = ht->table[i]) == NULL) continue;

        // 遍历当前节点的整个链表
        while (he) {
            nextHe = he->next;
            // 删除键
            dictFreeKey(d, he);
            // 删除值
            dictFreeVal(d, he);
            // 释放节点
            zfree(he);

            // 更新哈希表中元素个数
            ht->used--;

            // 处理下个节点
            he = nextHe;
        }
    }

    /* Free the table and the allocated cache structure */
    // 释放哈希表结构
    zfree(ht->table);

    /* Re-initialize the table */
    // 重置哈希表属性
    _dictReset(ht);

    return DICT_OK; /* never fails */
}

/* Clear & Release the hash table
 *
 * 删除并释放整个字典
 *
 * T = O(N)
 *
 */
void dictRelease(dict *d) {
    // 删除并清空两个哈希表
    _dictClear(d, &d->ht[0], NULL);
    _dictClear(d, &d->ht[1], NULL);
    zfree(d);
}

/*
 * 返回字典中包含 key 的节点。如果找到节点则返回；否则返回 NULL
 *
 * T = O(1)
 */
dictEntry *dictFind(dict *d, const void *key) {
    dictEntry *he;
    uint64_t h, idx, table;

    // 字典（的哈希表）为空
    if (dictSize(d) == 0) return NULL; /* dict is empty */

    // 如果字典正在 rehash，则进行单步 rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 计算给定键的哈希值
    h = dictHashKey(d, key);

    // 在字典的哈希表中查找这个键； T = O(1)
    // 注意，可能字典的两个哈希表都会遍历
    for (table = 0; table <= 1; table++) {

        // 计算索引值
        idx = h & d->ht[table].sizemask;

        // 遍历给定索引上的链表，查找 key
        he = d->ht[table].table[idx];
        while (he) {
            // 比较两个键
            if (key == he->key || dictCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }

        // 走到这里说明当前哈希表中没有找到指定的键的节点
        // 如果程序是遍历完 0 号 哈希表，那么会进一步检查字典是否在进行 rehash，如果是在进行 rehash ，那么会尝试继续从 1 号哈希表中查找；否则直接返回 NULL
        if (!dictIsRehashing(d)) return NULL;
    }
    return NULL;
}

/**
 * 获取包含给定键的节点的值
 *
 * @param d
 * @param key
 * @return
 */
void *dictFetchValue(dict *d, const void *key) {
    dictEntry *he;

    // 根据 key 获取对应的哈希项
    he = dictFind(d, key);

    // 获取哈希项对应的值
    return he ? dictGetVal(he) : NULL;
}

/* A fingerprint is a 64 bit number that represents the state of the dictionary
 * at a given time, it's just a few dict properties xored together.
 * When an unsafe iterator is initialized, we get the dict fingerprint, and check
 * the fingerprint again when the iterator is released.
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictionary while iterating. */
long long dictFingerprint(dict *d) {
    long long integers[6], hash = 0;
    int j;

    integers[0] = (long) d->ht[0].table;
    integers[1] = d->ht[0].size;
    integers[2] = d->ht[0].used;
    integers[3] = (long) d->ht[1].table;
    integers[4] = d->ht[1].size;
    integers[5] = d->ht[1].used;

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    for (j = 0; j < 6; j++) {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

dictIterator *dictGetIterator(dict *d) {
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;
    return iter;
}

dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);

    i->safe = 1;
    return i;
}

dictEntry *dictNext(dictIterator *iter) {
    while (1) {
        if (iter->entry == NULL) {
            dictht *ht = &iter->d->ht[iter->table];
            if (iter->index == -1 && iter->table == 0) {
                if (iter->safe)
                    dictPauseRehashing(iter->d);
                else
                    iter->fingerprint = dictFingerprint(iter->d);
            }
            iter->index++;
            if (iter->index >= (long) ht->size) {
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                } else {
                    break;
                }
            }
            iter->entry = ht->table[iter->index];
        } else {
            iter->entry = iter->nextEntry;
        }
        if (iter->entry) {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }
    return NULL;
}

void dictReleaseIterator(dictIterator *iter) {
    if (!(iter->index == -1 && iter->table == 0)) {
        if (iter->safe)
            dictResumeRehashing(iter->d);
        else
            assert(iter->fingerprint == dictFingerprint(iter->d));
    }
    zfree(iter);
}

/* Return a random entry from the hash table. Useful to
 * implement randomized algorithms */
/**
 * 随机返回字典中任意一个节点，如果字典为空，返回 NULL
 * @param d
 * @return
 */
dictEntry *dictGetRandomKey(dict *d) {
    dictEntry *he, *orighe;
    unsigned long h;
    int listlen, listele;

    // 字典为空
    if (dictSize(d) == 0) return NULL;

    // 进行单步 rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 如果正在 rehash ，那么将 1 号哈希表也作为随机查找的目标
    if (dictIsRehashing(d)) {
        do {
            /* We are sure there are no elements in indexes from 0
             * to rehashidx-1 */
            h = d->rehashidx + (randomULong() % (dictSlots(d) - d->rehashidx));
            he = (h >= d->ht[0].size) ? d->ht[1].table[h - d->ht[0].size] :
                 d->ht[0].table[h];
        } while (he == NULL);

        // 否则，只从 0 号哈希表中查找节点
    } else {
        do {
            h = randomULong() & d->ht[0].sizemask;
            he = d->ht[0].table[h];
        } while (he == NULL);
    }

    // 执行到这里，he 已经指向一个非空的节点链表
    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    listlen = 0;
    orighe = he;

    // 计算节点数量
    while (he) {
        he = he->next;
        listlen++;
    }

    // 取模，得出随机节点的索引
    listele = random() % listlen;
    he = orighe;

    // 按索引查找节点
    while (listele--) he = he->next;
    return he;
}

/* This function samples the dictionary to return a few keys from random
 * locations.
 *
 * It does not guarantee to return all the keys specified in 'count', nor
 * it does guarantee to return non-duplicated elements, however it will make
 * some effort to do both things.
 *
 * Returned pointers to hash table entries are stored into 'des' that
 * points to an array of dictEntry pointers. The array must have room for
 * at least 'count' elements, that is the argument we pass to the function
 * to tell how many random elements we need.
 *
 * The function returns the number of items stored into 'des', that may
 * be less than 'count' if the hash table has less than 'count' elements
 * inside, or if not enough elements were found in a reasonable amount of
 * steps.
 *
 * Note that this function is not suitable when you need a good distribution
 * of the returned items, but only when you need to "sample" a given number
 * of continuous elements to run some kind of algorithm or to produce
 * statistics. However the function is much faster than dictGetRandomKey()
 * at producing N elements. */
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count) {
    unsigned long j; /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    if (dictSize(d) < count) count = dictSize(d);
    maxsteps = count * 10;

    /* Try to do a rehashing work proportional to 'count'. */
    for (j = 0; j < count; j++) {
        if (dictIsRehashing(d))
            _dictRehashStep(d);
        else
            break;
    }

    tables = dictIsRehashing(d) ? 2 : 1;
    maxsizemask = d->ht[0].sizemask;
    if (tables > 1 && maxsizemask < d->ht[1].sizemask)
        maxsizemask = d->ht[1].sizemask;

    /* Pick a random point inside the larger table. */
    unsigned long i = randomULong() & maxsizemask;
    unsigned long emptylen = 0; /* Continuous empty entries so far. */
    while (stored < count && maxsteps--) {
        for (j = 0; j < tables; j++) {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            if (tables == 2 && j == 0 && i < (unsigned long) d->rehashidx) {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                if (i >= d->ht[1].size)
                    i = d->rehashidx;
                else
                    continue;
            }
            if (i >= d->ht[j].size) continue; /* Out of range for this table. */
            dictEntry *he = d->ht[j].table[i];

            /* Count contiguous empty buckets, and jump to other
             * locations if they reach 'count' (with a minimum of 5). */
            if (he == NULL) {
                emptylen++;
                if (emptylen >= 5 && emptylen > count) {
                    i = randomULong() & maxsizemask;
                    emptylen = 0;
                }
            } else {
                emptylen = 0;
                while (he) {
                    /* Collect all the elements of the buckets found non
                     * empty while iterating. */
                    *des = he;
                    des++;
                    he = he->next;
                    stored++;
                    if (stored == count) return stored;
                }
            }
        }
        i = (i + 1) & maxsizemask;
    }
    return stored;
}

/* This is like dictGetRandomKey() from the POV of the API, but will do more
 * work to ensure a better distribution of the returned element.
 *
 * This function improves the distribution because the dictGetRandomKey()
 * problem is that it selects a random bucket, then it selects a random
 * element from the chain in the bucket. However elements being in different
 * chain lengths will have different probabilities of being reported. With
 * this function instead what we do is to consider a "linear" range of the table
 * that may be constituted of N buckets with chains of different lengths
 * appearing one after the other. Then we report a random element in the range.
 * In this way we smooth away the problem of different chain lengths. */
#define GETFAIR_NUM_ENTRIES 15

dictEntry *dictGetFairRandomKey(dict *d) {
    dictEntry *entries[GETFAIR_NUM_ENTRIES];
    unsigned int count = dictGetSomeKeys(d, entries, GETFAIR_NUM_ENTRIES);
    /* Note that dictGetSomeKeys() may return zero elements in an unlucky
     * run() even if there are actually elements inside the hash table. So
     * when we get zero, we call the true dictGetRandomKey() that will always
     * yield the element if the hash table has at least one. */
    if (count == 0) return dictGetRandomKey(d);
    unsigned int idx = rand() % count;
    return entries[idx];
}

/* Function to reverse bits. Algorithm from:
 * http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static unsigned long rev(unsigned long v) {
    unsigned long s = CHAR_BIT * sizeof(v); // bit size; must be power of 2
    unsigned long mask = ~0UL;
    while ((s >>= 1) > 0) {
        mask ^= (mask << s);
        v = ((v >> s) & mask) | ((v << s) & ~mask);
    }
    return v;
}

/* dictScan() is used to iterate over the elements of a dictionary.
 *
 * dictScan() 函数用于迭代给定字典中的元素。
 *
 * Iterating works the following way:
 * 迭代按以下方式执行：
 *
 * 1) Initially you call the function using a cursor (v) value of 0.
 *     一开始，你使用 0 作为游标来调用函数
 * 2) The function performs one step of the iteration, and returns the
 *    new cursor value you must use in the next call.
 *    函数执行一步迭代操作，并返回一个下次迭代时使用的新游标。
 * 3) When the returned cursor is 0, the iteration is complete.
 *    当函数返回的游标为 0 时，迭代完成。
 *
 * The function guarantees all elements present in the
 * dictionary get returned between the start and end of the iteration.
 * However it is possible some elements get returned multiple times.
 *
 * 函数保证，在迭代从开始到结束期间，一直存在于字典的元素肯定会被迭代到，
 * 但一个元素可能会被返回多次。
 *
 * For every element returned, the callback argument 'fn' is
 * called with 'privdata' as first argument and the dictionary entry
 * 'de' as second argument.
 * 每当一个元素被返回时，回调函数 fn 就会被执行，fn 函数的第一个参数是 privdata ，而第二个参数则是字典节点 de 。
 *
 *
 * HOW IT WORKS.
 * 工作原理，采用高位进位加法
 *
 * The iteration algorithm was designed by Pieter Noordhuis.
 * The main idea is to increment a cursor starting from the higher order
 * bits. That is, instead of incrementing the cursor normally, the bits
 * of the cursor are reversed, then the cursor is incremented, and finally
 * the bits are reversed again.
 *
 * 算法的主要思路是在二进制高位上对游标进行加法计算，也即是说，不是按正常的办法来对游标进行加法计算，
 * 而是首先将游标的二进制位翻转（reverse）过来，然后对翻转后的值进行加法计算，最后再次对加法计算之后的结果进行翻转。
 *
 *
 * This strategy is needed because the hash table may be resized between
 * iteration calls.
 * 这一策略是必要的，因为在一次完整的迭代过程中，哈希表的大小有可能在两次迭代之间发生改变。
 *
 * dict.c hash tables are always power of two in size, and they
 * use chaining, so the position of an element in a given table is given
 * by computing the bitwise AND between Hash(key) and SIZE-1
 * (where SIZE-1 is always the mask that is equivalent to taking the rest
 *  of the division between the Hash of the key and SIZE).
 *
 * For example if the current hash table size is 16, the mask is
 * (in binary) 1111. The position of a key in the hash table will always be
 * the last four bits of the hash output, and so forth.
 *
 * WHAT HAPPENS IF THE TABLE CHANGES IN SIZE?
 *
 * If the hash table grows, elements can go anywhere in one multiple of
 * the old bucket: for example let's say we already iterated with
 * a 4 bit cursor 1100 (the mask is 1111 because hash table size = 16).
 *
 * If the hash table will be resized to 64 elements, then the new mask will
 * be 111111. The new buckets you obtain by substituting in ??1100
 * with either 0 or 1 can be targeted only by keys we already visited
 * when scanning the bucket 1100 in the smaller hash table.
 *
 * By iterating the higher bits first, because of the inverted counter, the
 * cursor does not need to restart if the table size gets bigger. It will
 * continue iterating using cursors without '1100' at the end, and also
 * without any other combination of the final 4 bits already explored.
 *
 * Similarly when the table size shrinks over time, for example going from
 * 16 to 8, if a combination of the lower three bits (the mask for size 8
 * is 111) were already completely explored, it would not be visited again
 * because we are sure we tried, for example, both 0111 and 1111 (all the
 * variations of the higher bit) so we don't need to test it again.
 *
 * WAIT... YOU HAVE *TWO* TABLES DURING REHASHING!
 *
 * Yes, this is true, but we always iterate the smaller table first, then
 * we test all the expansions of the current cursor into the larger
 * table. For example if the current cursor is 101 and we also have a
 * larger table of size 16, we also test (0)101 and (1)101 inside the larger
 * table. This reduces the problem back to having only one table, where
 * the larger one, if it exists, is just an expansion of the smaller one.
 *
 * LIMITATIONS
 *
 * This iterator is completely stateless, and this is a huge advantage,
 * including no additional memory used.
 *
 * The disadvantages resulting from this design are:
 *
 * 1) It is possible we return elements more than once. However this is usually
 *    easy to deal with in the application level.
 * 2) The iterator must return multiple elements per call, as it needs to always
 *    return all the keys chained in a given bucket, and all the expansions, so
 *    we are sure we don't miss keys moving during rehashing.
 * 3) The reverse cursor is somewhat hard to understand at first, but this
 *    comment is supposed to help.
 */
unsigned long dictScan(dict *d,
                       unsigned long v, // 游标
                       dictScanFunction *fn,
                       dictScanBucketFunction *bucketfn,
                       void *privdata) {
    dictht *t0, *t1;
    const dictEntry *de, *next;
    unsigned long m0, m1;

    // 跳过空字典
    if (dictSize(d) == 0) return 0;

    /* This is needed in case the scan callback tries to do dictFind or alike. */
    dictPauseRehashing(d);

    // 迭代只有一个哈希表的字典
    if (!dictIsRehashing(d)) {
        // 指向哈希表
        t0 = &(d->ht[0]);
        // 记录哈希表的 mask
        m0 = t0->sizemask;

        /* Emit entries at cursor */
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);

        // 指向游标 v 对应的哈希桶
        de = t0->table[v & m0];

        // 遍历桶中的所有节点
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /** todo 高位进位加法操作游标 */
        /* Set unmasked bits so incrementing the reversed cursor operates on the masked bits */
        v |= ~m0;
        /* Increment the reverse cursor */
        // 增加反向游标
        v = rev(v);
        v++;
        v = rev(v);

        // 迭代两个哈希表
    } else {

        // 指向两个哈希表
        t0 = &d->ht[0];
        t1 = &d->ht[1];

        /* Make sure t0 is the smaller and t1 is the bigger table */
        // 确保 t0 比 t1 要小
        if (t0->size > t1->size) {
            t0 = &d->ht[1];
            t1 = &d->ht[0];
        }

        // 记录掩码
        m0 = t0->sizemask;
        m1 = t1->sizemask;

        /* Emit entries at cursor */
        if (bucketfn) bucketfn(privdata, &t0->table[v & m0]);

        // 指向桶，并迭代桶中的所有节点
        de = t0->table[v & m0];
        while (de) {
            next = de->next;
            fn(privdata, de);
            de = next;
        }

        /* Iterate over indices in larger table that are the expansion
         * of the index pointed to by the cursor in the smaller table */
        do {
            /* Emit entries at cursor */
            if (bucketfn) bucketfn(privdata, &t1->table[v & m1]);
            // 指向桶，并迭代桶中的所有节点
            de = t1->table[v & m1];
            while (de) {
                next = de->next;
                fn(privdata, de);
                de = next;
            }

            /* Increment the reverse cursor not covered by the smaller mask.*/
            v |= ~m1;
            v = rev(v);
            v++;
            v = rev(v);

            /* Continue while bits covered by mask difference is non-zero */
        } while (v & (m0 ^ m1));
    }

    dictResumeRehashing(d);

    return v;
}

/* ------------------------- private functions ------------------------------ */

/* Because we may need to allocate huge memory chunk at once when dict
 * expands, we will check this allocation is allowed or not if the dict
 * type has expandAllowed member function. */
static int dictTypeExpandAllowed(dict *d) {
    if (d->type->expandAllowed == NULL) return 1;
    return d->type->expandAllowed(
            _dictNextPower(d->ht[0].used + 1) * sizeof(dictEntry *),
            (double) d->ht[0].used / d->ht[0].size);
}

/**
 * 根据需要，初始化字典（的哈希表），或者对字典（的现有哈希表）进行扩展
 * @param d
 * @return
 */
static int _dictExpandIfNeeded(dict *d) {

    // 增量 rehash 已经在进行中，直接返回即可
    /* Incremental rehashing already in progress. Return. */
    if (dictIsRehashing(d)) return DICT_OK;

    // 如果 Hash 表为空，将 Hash 表扩为初始大小
    if (d->ht[0].size == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets.
     *
     * 1 如果Hash表承载的元素个数超过其当前大小，并且允许进行扩容
     * 2 Hash表承载的元素个数已是当前大小的5倍，不管是否允许，都要扩容
     */
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used / d->ht[0].size > dict_force_resize_ratio) &&
        dictTypeExpandAllowed(d)) {

        // 扩容
        // 注意，这里大小虽然只是 +1 ，但是内部函数会找到比该值大的最小 2^N 的值
        return dictExpand(d, d->ht[0].used + 1);
    }
    return DICT_OK;
}

/**
 * 计算第一个大于等于 size 的 2 的 N 次方，用作哈希表的值
 *
 * @param size
 * @return
 */
static unsigned long _dictNextPower(unsigned long size) {
    // Hash 表的初始大小 ，是 4
    unsigned long i = DICT_HT_INITIAL_SIZE;

    // 如果要扩容的大小已经超过最大值，则返回最大值+1
    if (size >= LONG_MAX) return LONG_MAX + 1LU;

    // 扩容大小没有超过最大值
    while (1) {
        if (i >= size)
            return i;
        // 每一步扩容都在现有大小基础上乘以2
        i *= 2;
    }
}

/* Returns the index of a free slot that can be populated with
 * a hash entry for the given 'key'.
 * If the key already exists, -1 is returned
 * and the optional output parameter may be filled.
 *
 * 返回可以将 key 插入到哈希表的索引位置
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table.
 */
/**
 * 返回可以将 key 插入到哈希表的索引位置
 *
 * @param d 字典
 * @param key key
 * @param hash key 对应的 hash
 * @param existing 如果 key 已经存在于字典中，就把 key 对应哈希项赋值给该参数
 * @return
 */
static long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing) {
    unsigned long idx, table;
    dictEntry *he;

    // 如果 key 重复是否将对应的 Hash 项赋值给 existing
    if (existing) *existing = NULL;

    /** 是否扩展 Hash 表 */
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;

    // 遍历字典中的两个 Hash 表
    for (table = 0; table <= 1; table++) {

        // 计算索引值
        idx = hash & d->ht[table].sizemask;

        // 计算 key 对应的哈希项
        /* Search if this slot does not already contain the given key */
        he = d->ht[table].table[idx];

        // 查找 key 是否存在
        while (he) {

            // 如果字典中已经存在当前 key ，则返回 -1
            if (key == he->key || dictCompareKeys(d, key, he->key)) {
                // 允许赋值操作
                if (existing) *existing = he;
                return -1;
            }
            he = he->next;
        }

        // 执行到这里，说明 0 号哈希表中所有节点都不包含 key 。如果这时 rehash 正在进行，那么继续对 1 号哈希表查找
        if (!dictIsRehashing(d)) break;
    }

    // 返回 key 所在的索引
    return idx;
}

/**
 * 清空字典上的所有哈希表，并重置字典属性
 * @param d
 * @param callback
 */
void dictEmpty(dict *d, void(callback)(void *)) {
    // 删除两个 Hash 表上的所有哈希项
    _dictClear(d, &d->ht[0], callback);
    _dictClear(d, &d->ht[1], callback);

    // 重置属性
    d->rehashidx = -1;
    d->pauserehash = 0;
}

// 设置允许扩容
void dictEnableResize(void) {
    dict_can_resize = 1;
}

// 禁止扩容
void dictDisableResize(void) {
    dict_can_resize = 0;
}

uint64_t dictGetHash(dict *d, const void *key) {
    return dictHashKey(d, key);
}

/* Finds the dictEntry reference by using pointer and pre-calculated hash.
 * oldkey is a dead pointer and should not be accessed.
 * the hash value should be provided using dictGetHash.
 * no string / key comparison is performed.
 * return value is the reference to the dictEntry if found, or NULL if not found. */
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash) {
    dictEntry *he, **heref;
    unsigned long idx, table;

    if (dictSize(d) == 0) return NULL; /* dict is empty */
    for (table = 0; table <= 1; table++) {
        idx = hash & d->ht[table].sizemask;
        heref = &d->ht[table].table[idx];
        he = *heref;
        while (he) {
            if (oldptr == he->key)
                return heref;
            heref = &he->next;
            he = *heref;
        }
        if (!dictIsRehashing(d)) return NULL;
    }
    return NULL;
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICT_STATS_VECTLEN 50

size_t _dictGetStatsHt(char *buf, size_t bufsize, dictht *ht, int tableid) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];
    size_t l = 0;

    if (ht->used == 0) {
        return snprintf(buf, bufsize,
                        "No stats available for empty dictionaries\n");
    }

    /* Compute stats. */
    for (i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;
    for (i = 0; i < ht->size; i++) {
        dictEntry *he;

        if (ht->table[i] == NULL) {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = ht->table[i];
        while (he) {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN - 1)]++;
        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;
    }

    /* Generate human readable stats. */
    l += snprintf(buf + l, bufsize - l,
                  "Hash table %d stats (%s):\n"
                  " table size: %lu\n"
                  " number of elements: %lu\n"
                  " different slots: %lu\n"
                  " max chain length: %lu\n"
                  " avg chain length (counted): %.02f\n"
                  " avg chain length (computed): %.02f\n"
                  " Chain length distribution:\n",
                  tableid, (tableid == 0) ? "main hash table" : "rehashing target",
                  ht->size, ht->used, slots, maxchainlen,
                  (float) totchainlen / slots, (float) ht->used / slots);

    for (i = 0; i < DICT_STATS_VECTLEN - 1; i++) {
        if (clvector[i] == 0) continue;
        if (l >= bufsize) break;
        l += snprintf(buf + l, bufsize - l,
                      "   %s%ld: %ld (%.02f%%)\n",
                      (i == DICT_STATS_VECTLEN - 1) ? ">= " : "",
                      i, clvector[i], ((float) clvector[i] / ht->size) * 100);
    }

    /* Unlike snprintf(), return the number of characters actually written. */
    if (bufsize) buf[bufsize - 1] = '\0';
    return strlen(buf);
}

void dictGetStats(char *buf, size_t bufsize, dict *d) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictGetStatsHt(buf, bufsize, &d->ht[0], 0);
    buf += l;
    bufsize -= l;
    if (dictIsRehashing(d) && bufsize > 0) {
        _dictGetStatsHt(buf, bufsize, &d->ht[1], 1);
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize - 1] = '\0';
}

/* ------------------------------- Benchmark ---------------------------------*/

#ifdef REDIS_TEST

uint64_t hashCallback(const void *key) {
    return dictGenHashFunction((unsigned char*)key, strlen((char*)key));
}

int compareCallback(void *privdata, const void *key1, const void *key2) {
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = strlen((char*)key1);
    l2 = strlen((char*)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallback(void *privdata, void *val) {
    DICT_NOTUSED(privdata);

    zfree(val);
}

char *stringFromLongLong(long long value) {
    char buf[32];
    int len;
    char *s;

    len = sprintf(buf,"%lld",value);
    s = zmalloc(len+1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

dictType BenchmarkDictType = {
    hashCallback,
    NULL,
    NULL,
    compareCallback,
    freeCallback,
    NULL,
    NULL
};

#define start_benchmark() start = timeInMilliseconds()
#define end_benchmark(msg) do { \
    elapsed = timeInMilliseconds()-start; \
    printf(msg ": %ld items in %lld ms\n", count, elapsed); \
} while(0)

/* ./redis-server test dict [<count> | --accurate] */
int dictTest(int argc, char **argv, int accurate) {
    long j;
    long long start, elapsed;
    dict *dict = dictCreate(&BenchmarkDictType,NULL);
    long count = 0;

    if (argc == 4) {
        if (accurate) {
            count = 5000000;
        } else {
            count = strtol(argv[3],NULL,10);
        }
    } else {
        count = 5000;
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        int retval = dictAdd(dict,stringFromLongLong(j),(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictSize(dict) == count);

    /* Wait for rehashing. */
    while (dictIsRehashing(dict)) {
        dictRehashMilliseconds(dict,100);
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(rand() % count);
        dictEntry *de = dictFind(dict,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        dictEntry *de = dictGetRandomKey(dict);
        assert(de != NULL);
    }
    end_benchmark("Accessing random keys");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(rand() % count);
        key[0] = 'X';
        dictEntry *de = dictFind(dict,key);
        assert(de == NULL);
        zfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLong(j);
        int retval = dictDelete(dict,key);
        assert(retval == DICT_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictAdd(dict,key,(void*)j);
        assert(retval == DICT_OK);
    }
    end_benchmark("Removing and adding");
    dictRelease(dict);
    return 0;
}
#endif
