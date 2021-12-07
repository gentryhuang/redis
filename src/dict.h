/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
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

#ifndef __DICT_H
#define __DICT_H

#include "mt19937-64.h"
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * 字典的操作状态：
 * DICT_OK: 操作成功
 * DICT_ERR: 操作失败或出错
 */
#define DICT_OK 0
#define DICT_ERR 1

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void) V)

/**
 * 哈希表节点结构。哈希表每一项是一个 dictEntry 的结构体
 */
typedef struct dictEntry {
    // 键
    void *key;

    /**
     * 值
     * 特别说明：
     * 1 值是一个联合体 v 定义的，这个联合体 v 中包含了指向实际值的指针 *val，还包含了无符号的 64 位整数，有符号的 64 位整数，以及 double 类型的数
     * 2 将值定义为一个联合体的目的是，为了节省内存。因为当值为整数或双精度浮点数时，由于其本身就是 64 位，就可以不用指针指向了，而是可以直接存在键值对的结构体中，这样就避免了再用一个指针，从而节省了内存空间。
     */
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;

    // 指向下个哈希表节点
    struct dictEntry *next;

} dictEntry;

/**
 * 字典类型，包含了一些特定函数
 */
typedef struct dictType {
    // 计算哈希值函数
    uint64_t (*hashFunction)(const void *key);

    // 复制键的函数
    void *(*keyDup)(void *privdata, const void *key);

    // 复制值的函数
    void *(*valDup)(void *privdata, const void *obj);

    // 键比较函数
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);

    // 键销毁函数
    void (*keyDestructor)(void *privdata, void *key);

    // 值销毁函数
    void (*valDestructor)(void *privdata, void *obj);

    // 扩展函数
    int (*expandAllowed)(size_t moreMem, double usedRatio);
} dictType;

/* This is our hash table structure. Every dictionary has two of this as we
 * implement incremental rehashing, for the old to the new table. */
/**
 * 哈希表
 */
typedef struct dictht {

    // 哈希表数组，数组中的每个元素是一个指向哈希项（dictEntry）的指针
    dictEntry **table;

    // 哈希表大小
    unsigned long size;

    // 哈希表大小掩码，用于计算索引值
    // 一般是数组长度-1
    unsigned long sizemask;

    // 哈希表已有元素数量
    unsigned long used;
} dictht;

/**
 * 字典
 * 每个字典包含两个哈希表，从而实现渐进式 rehash
 */
typedef struct dict {
    // 字典类型特定函数
    dictType *type;

    // 私有数据
    void *privdata;

    // 哈希表（2个），交替使用，用于 rehash 操作
    dictht ht[2];

    // Hash 表是否在进行 rehash 的标识，-1 表示没有进行rehash
    // 其它值表示迁移时对应的桶索引，即 表示的是当前 rehash 在对哪个 bucket 做数据迁移
    long rehashidx; /* rehashing not in progress if rehashidx == -1 */

    // 暂停 rehash，当该值 > 0 时，rehash 被暂停。
    int16_t pauserehash; /* If >0 rehashing is paused (<0 indicates coding error) */
} dict;

/* If safe is set to 1 this is a safe iterator, that means, you can call
 * dictAdd, dictFind, and other functions against the dictionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictNext()
 * should be called while iterating. */
/**
 * 字典迭代器
 */
typedef struct dictIterator {
    // 被迭代的字典
    dict *d;

    // 迭代器当前所指向的哈希表索引位置
    long index;

    // table: 正在被迭代的哈希表编号，0 或 1 (因为一个字典有两个哈希表)
    // safe: 标识这个迭代器是否安全
    int table, safe;

    // entry: 当前迭代到的哈希表节点指针
    // nextEntry: 当前迭代的哈希表节点的下一个节点指针
    dictEntry *entry, *nextEntry;

    /* unsafe iterator fingerprint for misuse detection. */
    long long fingerprint;
} dictIterator;

typedef void (dictScanFunction)(void *privdata, const dictEntry *de);

typedef void (dictScanBucketFunction)(void *privdata, dictEntry **bucketref);

/* This is the initial size of every hash table */
// 哈希表初始化大小
#define DICT_HT_INITIAL_SIZE     4

/* ------------------------------- Macros ------------------------------------*/
// 释放给定字典节点的值
#define dictFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d)->privdata, (entry)->v.val)

// 设置给定字典节点的值
#define dictSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        (entry)->v.val = (d)->type->valDup((d)->privdata, _val_); \
    else \
        (entry)->v.val = (_val_); \
} while(0)

#define dictSetSignedIntegerVal(entry, _val_) \
    do { (entry)->v.s64 = _val_; } while(0)

#define dictSetUnsignedIntegerVal(entry, _val_) \
    do { (entry)->v.u64 = _val_; } while(0)

#define dictSetDoubleVal(entry, _val_) \
    do { (entry)->v.d = _val_; } while(0)

#define dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d)->privdata, (entry)->key)

#define dictSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        (entry)->key = (d)->type->keyDup((d)->privdata, _key_); \
    else \
        (entry)->key = (_key_); \
} while(0)

// 比较两个键
#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d)->privdata, key1, key2) : \
        (key1) == (key2))

// 计算给定键的哈希值
#define dictHashKey(d, key) (d)->type->hashFunction(key)

// 返回获取给定节点的键
#define dictGetKey(he) ((he)->key)

// 返回给定节点的值
#define dictGetVal(he) ((he)->v.val)

// 返回给定节点的有符合整数值
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetDoubleVal(he) ((he)->v.d)

// 返回给定字典的大小
#define dictSlots(d) ((d)->ht[0].size+(d)->ht[1].size)

// 返回字典中节点数量
#define dictSize(d) ((d)->ht[0].used+(d)->ht[1].used)

// 查看字典是否正在 rehash
#define dictIsRehashing(d) ((d)->rehashidx != -1)
#define dictPauseRehashing(d) (d)->pauserehash++
#define dictResumeRehashing(d) (d)->pauserehash--

/* If our unsigned long type can store a 64 bit number, use a 64 bit PRNG. */
#if ULONG_MAX >= 0xffffffffffffffff
#define randomULong() ((unsigned long) genrand64_int64())
#else
#define randomULong() random()
#endif

/* API */
dict *dictCreate(dictType *type, void *privDataPtr);

int dictExpand(dict *d, unsigned long size);

int dictTryExpand(dict *d, unsigned long size);

int dictAdd(dict *d, void *key, void *val);

dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing);

dictEntry *dictAddOrFind(dict *d, void *key);

int dictReplace(dict *d, void *key, void *val);

int dictDelete(dict *d, const void *key);

dictEntry *dictUnlink(dict *ht, const void *key);

void dictFreeUnlinkedEntry(dict *d, dictEntry *he);

void dictRelease(dict *d);

dictEntry *dictFind(dict *d, const void *key);

void *dictFetchValue(dict *d, const void *key);

int dictResize(dict *d);

dictIterator *dictGetIterator(dict *d);

dictIterator *dictGetSafeIterator(dict *d);

dictEntry *dictNext(dictIterator *iter);

void dictReleaseIterator(dictIterator *iter);

dictEntry *dictGetRandomKey(dict *d);

dictEntry *dictGetFairRandomKey(dict *d);

unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count);

void dictGetStats(char *buf, size_t bufsize, dict *d);

uint64_t dictGenHashFunction(const void *key, int len);

uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len);

void dictEmpty(dict *d, void(callback)(void *));

void dictEnableResize(void);

void dictDisableResize(void);

/**
 * 执行键拷贝
 * @param d 全局 Hash 表
 * @param n 需要进行拷贝的桶数量
 * @return
 */
int dictRehash(dict *d, int n);

int dictRehashMilliseconds(dict *d, int ms);

void dictSetHashFunctionSeed(uint8_t *seed);

uint8_t *dictGetHashFunctionSeed(void);

unsigned long
dictScan(dict *d, unsigned long v, dictScanFunction *fn, dictScanBucketFunction *bucketfn, void *privdata);

uint64_t dictGetHash(dict *d, const void *key);

dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash);

/* Hash table types */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#ifdef REDIS_TEST
int dictTest(int argc, char *argv[], int accurate);
#endif

#endif /* __DICT_H */
