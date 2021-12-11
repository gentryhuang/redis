/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/**
 * 在 ziplist.h 中看不到压缩列表的结构体定义。因为压缩列表本身就是一块连续的内存空间，它通过使用不同的编码来保存数据。
 */

#ifndef _ZIPLIST_H
#define _ZIPLIST_H

/**
 * 标志压缩列表头
 */
#define ZIPLIST_HEAD 0
/**
 * 标志压缩列表尾
 */
#define ZIPLIST_TAIL 1

/* Each entry in the ziplist is either a string or an integer.
 *
 * ziplist中的每个条目都是字符串或整数
 */
typedef struct {
    /* When string is used, it is provided with the length (slen). */
    // 当条目存储的是 string 时，表示 string 的值
    unsigned char *sval;

    // 当条目存储的是 string 时，表示 string 的长度
    unsigned int slen;


    /* When integer is used, 'sval' is NULL, and lval holds the value. */
    // 当条目存储的是整数时，表示整数的值
    long long lval;
} ziplistEntry;

/**
 * 创建并返回一个空的 压缩列表
 * @return
 */
unsigned char *ziplistNew(void);


/**
 * 创建一个包含给定值的新节点，并将这个新节点添加到压缩列表的表头或表尾
 * @param zl 压缩列表
 * @param s 给定值
 * @param slen 给定值大小
 * @param where 添加到压缩列表的位置
 * @return
 */
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where);

/**
 * 返回压缩列表给定索引上的节点
 * @param zl 压缩列表
 * @param index 索引
 * @return
 */
unsigned char *ziplistIndex(unsigned char *zl, int index);

/**
 * 返回给定节点的下一个节点
 * @param zl 压缩列表
 * @param p 给定节点
 * @return
 */
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p);

/**
 * 返回给定节点的前一个节点
 * @param zl 压缩列表
 * @param p 给定节点
 * @return
 */
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p);

/**
 * 获取给定节点保存的值
 * @param p
 * @param sval
 * @param slen
 * @param lval
 * @return
 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sval, unsigned int *slen, long long *lval);

/**
 * 将给定值插入到给给定节点之后
 * @param zl 压缩列表
 * @param p 给定节点
 * @param s 给值值
 * @param slen 给定值大小
 * @return
 */
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);

/**
 * 从压缩列表中删除给定的节点
 * @param zl 压缩列表
 * @param p 给定的节点
 * @return
 */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p);

/**
 * 删除压缩列表在给定索引上的连续多个节点
 * @param zl
 * @param index
 * @param num
 * @return
 */
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num);

unsigned char *ziplistReplace(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);


/**
 * 在压缩列表中查找并返回包含了给定值的节点
 * @param zl
 * @param p
 * @param vstr
 * @param vlen
 * @param skip
 * @return
 */
unsigned char *
ziplistFind(unsigned char *zl, unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip);

/**
 * 返回压缩列表目前包含的节点数量
 *
 * 注意，当节点数量 < 65535 时可以直接从压缩列表的 zllen 中取出，时间复杂度 O(1)；节点数量 >= 65535 ，需要进行遍历
 * @param zl
 * @return
 */
unsigned int ziplistLen(unsigned char *zl);

/**
 * 返回压缩列表目前占用的内存字节数
 * @param zl
 * @return
 */
size_t ziplistBlobLen(unsigned char *zl);


unsigned char *ziplistMerge(unsigned char **first, unsigned char **second);

unsigned int ziplistCompare(unsigned char *p, unsigned char *s, unsigned int slen);


void ziplistRepr(unsigned char *zl);

typedef int (*ziplistValidateEntryCB)(unsigned char *p, void *userdata);

int ziplistValidateIntegrity(unsigned char *zl, size_t size, int deep,
                             ziplistValidateEntryCB entry_cb, void *cb_userdata);

void ziplistRandomPair(unsigned char *zl, unsigned long total_count, ziplistEntry *key, ziplistEntry *val);

void ziplistRandomPairs(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals);

unsigned int ziplistRandomPairsUnique(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals);

#ifdef REDIS_TEST
int ziplistTest(int argc, char *argv[], int accurate);
#endif

#endif /* _ZIPLIST_H */
