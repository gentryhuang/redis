/* quicklist.h - A generic doubly linked quicklist implementation
 *
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this quicklist of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this quicklist of conditions and the following disclaimer in the
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

#include <stdint.h> // for UINTPTR_MAX

#ifndef __QUICKLIST_H__
#define __QUICKLIST_H__

/* Node, quicklist, and Iterator are the only data structures used currently. */

/* quicklistNode is a 32 byte struct describing a ziplist for a quicklist.
 * quicklistNode 是一个32字节的结构，描述快速列表的 ziplist。
 *
 * We use bit fields keep the quicklistNode at 32 bytes.
 * count: 16 bits, max 65536 (max zl bytes is 65k, so max count actually < 32k).
 * encoding: 2 bits, RAW=1, LZF=2.
 * container: 2 bits, NONE=1, ZIPLIST=2.
 * recompress: 1 bit, bool, true if node is temporary decompressed for usage.
 * attempted_compress: 1 bit, boolean, used for verifying during testing.
 * extra: 10 bits, free for future use; pads out the remainder of 32 bits */
/*
 * quicklist 链表的节点结构
 */
typedef struct quicklistNode {
    /*
     * 前驱指针
     */
    struct quicklistNode *prev;
    /*
     * 后驱指针
     */
    struct quicklistNode *next;
    /*
     * quicklistNode 指向 ziplist
     *
     * 注意，不设置压缩数据参数时指向一个ziplist结构，设置压缩数据参数指向quicklistLZF结构
     */
    unsigned char *zl;
    /*
     * ziplist 的字节大小
     */
    unsigned int sz;             /* ziplist size in bytes */
    /*
     * ziplist 中的元素个数
     */
    unsigned int count: 16;     /* count of items in ziplist */
    /*
     * 编码格式
     *
     * 表示是否采用LZF压缩算法压缩quicklist节点，1表示不采用，2表示采用
     */
    unsigned int encoding: 2;   /* RAW==1 or LZF==2 */
    /*
     * 存储方式， 默认使用 ziplist 存储
     *
     * 表示一个quicklistNode节点是否采用ziplist结构保存数据，2表示采用，1表示不采用，默认是2
     */
    unsigned int container: 2;  /* NONE==1 or ZIPLIST==2 */
    /*
     * 数据是否被压缩
     */
    unsigned int recompress: 1; /* was this node previous compressed? */
    /*
     *  数据能否被压缩，测试时使用
     */
    unsigned int attempted_compress: 1; /* node can't compress; too small */
    /*
     * 预留的 bit 位
     */
    unsigned int extra: 10; /* more bits to steal for future usage */
} quicklistNode;

/* quicklistLZF is a 4+N byte struct holding 'sz' followed by 'compressed'.
 * 'sz' is byte length of 'compressed' field.
 * 'compressed' is LZF data with total (compressed) length 'sz'
 * NOTE: uncompressed length is stored in quicklistNode->sz.
 * When quicklistNode->zl is compressed, node->zl points to a quicklistLZF
 *
 * 压缩的 ziplist
 *
 * Redis 为了节省内存空间，会将 quicklist 的节点用 LZF 压缩后存储
 */
typedef struct quicklistLZF {
    /*
     * 表示被 LZF 算法压缩后的 ziplist 的大小
     */
    unsigned int sz; /* LZF size in bytes*/

    /*
     * 保存压缩后的 ziplist 的数组
     */
    char compressed[];
} quicklistLZF;

/* Bookmarks are padded with realloc at the end of of the quicklist struct.
 * They should only be used for very big lists if thousands of nodes were the
 * excess memory usage is negligible, and there's a real need to iterate on them
 * in portions.
 * When not used, they don't add any memory overhead, but when used and then
 * deleted, some overhead remains (to avoid resonance).
 * The number of bookmarks used should be kept to minimum since it also adds
 * overhead on node deletion (searching for a bookmark to update). */
typedef struct quicklistBookmark {
    quicklistNode *node;
    char *name;
} quicklistBookmark;

#if UINTPTR_MAX == 0xffffffff
/* 32-bit */
#   define QL_FILL_BITS 14
#   define QL_COMP_BITS 14
#   define QL_BM_BITS 4
#elif UINTPTR_MAX == 0xffffffffffffffff
/* 64-bit */
#   define QL_FILL_BITS 16
#   define QL_COMP_BITS 16
#   define QL_BM_BITS 4 /* we can encode more, but we rather limit the user
                           since they cause performance degradation. */
#else
#   error unknown arch bits count
#endif

/* quicklist is a 40 byte struct (on 64-bit systems) describing a quicklist.
 * 'count' is the number of total entries.
 * 'len' is the number of quicklist nodes.
 * 'compress' is: 0 if compression disabled, otherwise it's the number
 *                of quicklistNodes to leave uncompressed at ends of quicklist.
 * 'fill' is the user-requested (or default) fill factor.
 * 'bookmakrs are an optional feature that is used by realloc this struct,
 *      so that they don't consume memory when not used. */
/*
 * quicklist 作为一个链表结构
 *
 * 本质上就是一个双向链表
 */
typedef struct quicklist {
    /*
     * quicklist 的链表头
     */
    quicklistNode *head;
    /*
     * quicklist 的链表尾
     */
    quicklistNode *tail;
    /*
     * 所有 ziplist 中的总元素个数
     */
    unsigned long count;        /* total count of all entries in all ziplists */
    /*
     * quicklist 中节点个数
     */
    unsigned long len;          /* number of quicklistNodes */
    /*
     *  16 位，每个 quicknode 节点的最大容量，配置文件设定
     */
    int fill: QL_FILL_BITS;              /* fill factor for individual nodes */
    /*
     * 16 位，quicklist的压缩深度，0表示所有节点都不压缩，否则就表示从两端开始有多少个节点不压缩
     */
    unsigned int compress: QL_COMP_BITS; /* depth of end nodes not to compress;0=off */
    /*
     * 4 位，bookmarks数组的大小，bookmarks是一个可选字段，用来quicklist重新分配内存空间时使用，不使用时不占用空间
     */
    unsigned int bookmark_count: QL_BM_BITS;
    quicklistBookmark bookmarks[];
} quicklist;

/*
 * quicklist 的迭代器结构
 */
typedef struct quicklistIter {
    /*
     * 指向所属的quicklist的指针
     */
    const quicklist *quicklist;
    /*
     * 指向当前迭代的quicklist节点的指针
     */
    quicklistNode *current;
    /*
     * 指向当前quicklist节点中迭代的ziplist
     */
    unsigned char *zi;
    /*
     * 在当前ziplist结构中的偏移量
     */
    long offset; /* offset in current ziplist */
    /*
     * 迭代方向
     */
    int direction;
} quicklistIter;

/*
 * quicklist 的节点的管理器
 */
typedef struct quicklistEntry {
    /*
     * 指向所属的quicklist的指针
     */
    const quicklist *quicklist;
    /*
     * 指向所属的quicklistNode节点的指针
     */
    quicklistNode *node;
    /*
     * 指向当前ziplist结构的指针
     */
    unsigned char *zi;
    /*
     * 指向当前ziplist结构的字符串vlaue成员
     */
    unsigned char *value;
    /*
     * 指向当前ziplist结构的整数value成员
     */
    long long longval;
    /*
     * 保存当前ziplist结构的字节数大小
     */
    unsigned int sz;
    /*
     * 保存相对ziplist的偏移量
     */
    int offset;
} quicklistEntry;

#define QUICKLIST_HEAD 0
#define QUICKLIST_TAIL -1

/* quicklist node encodings quicklist 节点编码*/

// 没有被压缩
#define QUICKLIST_NODE_ENCODING_RAW 1
// 被 LZF 算法压缩
#define QUICKLIST_NODE_ENCODING_LZF 2

/* quicklist compression disable */
#define QUICKLIST_NOCOMPRESS 0

/* quicklist container formats
 *
 * 表示一个quicklistNode节点是否采用ziplist结构保存数据，2表示采用，1表示不采用，默认是 2
 */
#define QUICKLIST_NODE_CONTAINER_NONE 1 // quicklist节点直接保存对象
#define QUICKLIST_NODE_CONTAINER_ZIPLIST 2 // quicklist节点采用ziplist保存对象

// 测试quicklist节点是否被压缩，返回1 表示被压缩，否则返回0
#define quicklistNodeIsCompressed(node)                                        \
    ((node)->encoding == QUICKLIST_NODE_ENCODING_LZF)

/* Prototypes */
/*
 * 创建快速列表
 */
quicklist *quicklistCreate(void);

/*
 * 创建快速列表，并指定是否压缩等参数
 */
quicklist *quicklistNew(int fill, int compress);

/*
 * 为指定的 quicklist 设置 compress
 */
void quicklistSetCompressDepth(quicklist *quicklist, int depth);

/*
 * 为指定的 quicklist 设置 fill
 */
void quicklistSetFill(quicklist *quicklist, int fill);

/*
 * 为指定的 quicklist 设置 fill 和 compress
 */
void quicklistSetOptions(quicklist *quicklist, int fill, int depth);

/*
 * 释放 quicklist
 */
void quicklistRelease(quicklist *quicklist);

/*
 * 头插
 */
int quicklistPushHead(quicklist *quicklist, void *value, const size_t sz);

/*
 * 尾插
 */
int quicklistPushTail(quicklist *quicklist, void *value, const size_t sz);

/*
 * 将元素插入到指定的位置，where 要么是 头，要么是 尾
 */
void quicklistPush(quicklist *quicklist, void *value, const size_t sz,
                   int where);

/*
 * 在 quicklist 的尾节点后插入一个存放 zl 的新 quicklistNode
 */
void quicklistAppendZiplist(quicklist *quicklist, unsigned char *zl);

/*
 * 将传入的 ziplist 中的值，依次加入到 quicklist
 */
quicklist *quicklistAppendValuesFromZiplist(quicklist *quicklist,
                                            unsigned char *zl);

/*
 * 根据特性参数创建一个新的 quicklist ，并将传入的 ziplist 中的值依次加入到该 quicklist
 */
quicklist *quicklistCreateFromZiplist(int fill, int compress,
                                      unsigned char *zl);
/*
 * 在 quicklist 中，将元素插入到 node 中填充的 quiclistNode 后面
 */
void quicklistInsertAfter(quicklist *quicklist, quicklistEntry *node,
                          void *value, const size_t sz);

/*
 * 在 quicklist 中，将元素插入到 node 中填充的 quiclistNode 前面
 */
void quicklistInsertBefore(quicklist *quicklist,
                           quicklistEntry *node,
                           void *value,
                           const size_t sz);

/*
 * 删除 entry 中 quicklistNode
 */
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry);

/*
 * 数据替换
 */
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data,
                            int sz);
/*
 * 范围删除
 */
int quicklistDelRange(quicklist *quicklist, const long start, const long stop);

/*
 * 迭代器
 */
quicklistIter *quicklistGetIterator(const quicklist *quicklist, int direction);

/*
 *  从指定位置开始的迭代器
 */
quicklistIter *quicklistGetIteratorAtIdx(const quicklist *quicklist,
                                         int direction, const long long idx);

int quicklistNext(quicklistIter *iter, quicklistEntry *node);

void quicklistReleaseIterator(quicklistIter *iter);

quicklist *quicklistDup(quicklist *orig);

/*
 * 查找下标为idx的entry，返回1 表示找到，0表示没找到
 */
int quicklistIndex(const quicklist *quicklist, const long long index,
                   quicklistEntry *entry);

void quicklistRewind(quicklist *quicklist, quicklistIter *li);

void quicklistRewindTail(quicklist *quicklist, quicklistIter *li);

void quicklistRotate(quicklist *quicklist);

int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data,
                       unsigned int *sz, long long *sval,
                       void *(*saver)(unsigned char *data, unsigned int sz));

/*
 * 获取元素
 */
int quicklistPop(quicklist *quicklist, int where, unsigned char **data,
                 unsigned int *sz, long long *slong);

unsigned long quicklistCount(const quicklist *ql);

int quicklistCompare(unsigned char *p1, unsigned char *p2, int p2_len);

size_t quicklistGetLzf(const quicklistNode *node, void **data);

/* bookmarks */
int quicklistBookmarkCreate(quicklist **ql_ref, const char *name, quicklistNode *node);

int quicklistBookmarkDelete(quicklist *ql, const char *name);

quicklistNode *quicklistBookmarkFind(quicklist *ql, const char *name);

void quicklistBookmarksClear(quicklist *ql);

#ifdef REDIS_TEST
int quicklistTest(int argc, char *argv[], int accurate);
#endif

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __QUICKLIST_H__ */
