/* SDSLib 2.0 -- A C dynamic strings library
 *
 * Copyright (c) 2006-2015, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2015, Oran Agra
 * Copyright (c) 2015, Redis Labs, Inc
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

#ifndef __SDS_H
#define __SDS_H

/*
 * 最大预分配长度
 * 1M = 1024KB = 1024 * 1024Byte
 */
#define SDS_MAX_PREALLOC (1024*1024)

extern const char *SDS_NOINIT;

#include <sys/types.h>
#include <stdarg.h>
#include <stdint.h>

/*
 * 类型别名，用于指向 sdshdr 的 buf 属性。约定：在 Redis 中出现 sds 表示的是 SDS 结构体中的 char buf[]
 *
 * 1 todo 这里指的是 sdshdr 中的 buf[] 字符数组，而非整个 SDS 结构体
 * 2 todo 要使用 SDS 结构体，对 sds 进行地址减运算即可得到指向 SDS 结构体的指针
 *
 * todo 说明：SDS 本质还是字符数组，只是在字符数组基础上增加了额外的元数据。字符串数据存放在 SDS 结构体的 buf[] 中，表示 buf[] 状态的数据是 SDS 中的元数据。
 *
 * 优秀设计：
 *
 * SDS 有一个很优秀的设计是对外和char*保持一致，这样外部就可以像使用 char* 一样来使用 SDS 了。
 * 同时，在使用 SDS 相关函数操作时又可以发挥 SDS 的特性，因为 sds 和 sds的Header 可以通过偏移量相互获取到，知道任意一个就能知道另一个。
 * - 获取 SDS 结构体的元数据大小：  int hdrlen = sizeof(struct sdshdrN)
 * - 根据 sds 获取 sds的Header:  void *sh = sds - hdrlen
 * - 根据 sds的Header 获取 sds:  sds = (char*)sh + hdrlen
 */
typedef char *sds;

/*
 * 说明：
 * 1 sdshdrN: 其中的 N 表示当前的 SDS 可以存储的字节数为 2^N
 * 2 sdshdrN 中的 flags 用于标志是哪种 sds
 * 3 相比于 C 语言中的字符串实现，SDS 这种字符串的实现方式，会提升字符串的操作效率，并且可以用来保存二进制数据。
 * 4 因为 Redis 是使用 C 语言开发的，所以为了保证能尽量复用 C 标准库中的字符串操作函数，Redis 保留了使用字符数组来保存实际的数据。
 * 5 SDS 结构中有一个元数据 flags，表示的是 SDS 类型。事实上，SDS 一共设计了 5 种类型，分别是 sdshdr5、sdshdr8、sdshdr16、sdshdr32 和 sdshdr64。
 *   这 5 种类型的主要区别就在于，它们数据结构中的字符数组现有长度 len 和分配空间长度 alloc，这两个元数据的数据类型不同。sdshdr5 这一类型 Redis 已经不再使用了.
 * 6 SDS 的实现需要考虑操作高效、能保存任意二进制数据，以及节省内存的需求。
 */
/* Note: sdshdr5 is never used, we just access the flags byte directly.
 * However is here to document the layout of type 5 SDS strings. */
struct __attribute__ ((__packed__)) sdshdr5 {
    unsigned char flags; /* 3 lsb of type, and 5 msb of string length */
    char buf[];
};

/**
 * 结构说明（sds 对应的 Header，是去掉 buf[] 存储数据后的结构体长度）：
 * 1 uint8_t 是 8 位无符号整型，会占用 1 字节的内存空间。其它的类推。
 * 2 当字符串类型是 sdshdr8 时，它能表示的字符数组长度（包括数组最后一位\0）不会超过 256 字节（2 的 8 次方等于 256）。其它的类推
 * 3 SDS 之所以设计不同的结构头（即不同类型），是为了能灵活保存不同大小的字符串，从而有效节省内存空间。因为在保存不同大小的字符串时，
 *   结构头占用的内存空间也不一样，这样一来，在保存小字符串时，结构头占用空间也比较少。
 * 4 除了设计不同类型的结构头，Redis 在编程上还使用了专门的编译优化来节省内存空间。
 *   如 __attribute__ ((__packed__))的作用就是告诉编译器，在编译 sdshdr8 结构时，不要使用字节对齐的方式，而是采用紧凑的方式分配内存，这样一来，结构体实际占用多少内存空间，编译器就分配多少空间。
 *   这是因为在默认情况下，编译器会按照 8 字节对齐的方式，给变量分配内存。也就是说，即使一个变量的大小不到 8 个字节，编译器也会给它分配 8 个字节。
 * 5  __attribute__ ((__packed__)) 除了节省内存空间外，还有一个很精妙的设计就是在编译之后，sds 可以和 sds 所在的 Header 进行相互查找，这是 SDS 设计的灵魂。
 */
struct __attribute__ ((__packed__)) sdshdr8 {
    // 字符数组现有长度。 8 位无符号整型，占用 1 字节的内存空间。
    // 一个字符是一个长度，一个字符占一个字节，len 用来计数多少个字符。因此，SDS 的容量大小 len 大小个字节
    uint8_t len; /* used */
    // 字符数组已经分配的长度, 不包括结构体和\0结束符
    uint8_t alloc; /* excluding the header and null terminator */
    // SDS 类型
    unsigned char flags; /* 3 lsb of type, 5 unused bits */
    // 字符数组，用来保存实际数据
    char buf[]; // todo 注意，再分配 sdshdr 时不会分配该属性的内存空间，因为大小不确定。如果大小确定就可以分配了，如 char buf[1]
};
struct __attribute__ ((__packed__)) sdshdr16 {
    uint16_t len; /* used */
    uint16_t alloc; /* excluding the header and null terminator */
    unsigned char flags; /* 3 lsb of type, 5 unused bits */
    char buf[];
};
struct __attribute__ ((__packed__)) sdshdr32 {
    uint32_t len; /* used */
    uint32_t alloc; /* excluding the header and null terminator */
    unsigned char flags; /* 3 lsb of type, 5 unused bits */
    char buf[];
};
struct __attribute__ ((__packed__)) sdshdr64 {
    uint64_t len; /* used */
    uint64_t alloc; /* excluding the header and null terminator */
    unsigned char flags; /* 3 lsb of type, 5 unused bits */
    char buf[];
};

#define SDS_TYPE_5  0
#define SDS_TYPE_8  1
#define SDS_TYPE_16 2
#define SDS_TYPE_32 3
#define SDS_TYPE_64 4
#define SDS_TYPE_MASK 7
#define SDS_TYPE_BITS 3

/**
 * 获取 sds 对应的 SDS 结构体，一键让 sds 回到指针初始位置
 */
#define SDS_HDR_VAR(T, s) struct sdshdr##T *sh = (void*)((s)-(sizeof(struct sdshdr##T)));
#define SDS_HDR(T, s) ((struct sdshdr##T *)((s)-(sizeof(struct sdshdr##T))))
#define SDS_TYPE_5_LEN(f) ((f)>>SDS_TYPE_BITS)

/*
 * 返回 sds 实际保存的字符串的长度，而这个长度记录在对应 SDS 结构体中，因此需要先通过 sds 得到指向 SDS 的指针。
 *
 * T = O(1)
 */
static inline size_t sdslen(const sds s) {
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            return SDS_TYPE_5_LEN(flags);
        case SDS_TYPE_8:
            return SDS_HDR(8, s)->len;
        case SDS_TYPE_16:
            return SDS_HDR(16, s)->len;
        case SDS_TYPE_32:
            return SDS_HDR(32, s)->len;
        case SDS_TYPE_64:
            return SDS_HDR(64, s)->len;
    }
    return 0;
}

/*
 * 返回 sds 可用的（空闲的）长度 , avail = alloc - len
 *
 * T = O(1)
 */
static inline size_t sdsavail(const sds s) {
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK) {
        case SDS_TYPE_5: {
            return 0;
        }
        case SDS_TYPE_8: {
            SDS_HDR_VAR(8, s);
            return sh->alloc - sh->len;
        }
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16, s);
            return sh->alloc - sh->len;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32, s);
            return sh->alloc - sh->len;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64, s);
            return sh->alloc - sh->len;
        }
    }
    return 0;
}

/*
 * 设置 sds 的实际长度
 *
 * T = O(1)
 */
static inline void sdssetlen(sds s, size_t newlen) {
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK) {
        case SDS_TYPE_5: {
            unsigned char *fp = ((unsigned char *) s) - 1;
            *fp = SDS_TYPE_5 | (newlen << SDS_TYPE_BITS);
        }
            break;
        case SDS_TYPE_8:
            SDS_HDR(8, s)->len = newlen;
            break;
        case SDS_TYPE_16:
            SDS_HDR(16, s)->len = newlen;
            break;
        case SDS_TYPE_32:
            SDS_HDR(32, s)->len = newlen;
            break;
        case SDS_TYPE_64:
            SDS_HDR(64, s)->len = newlen;
            break;
    }
}

/*
 * 增加 sds 的实际长度
 *
 * T = O(1)
 */
static inline void sdsinclen(sds s, size_t inc) {
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK) {
        case SDS_TYPE_5: {
            unsigned char *fp = ((unsigned char *) s) - 1;
            unsigned char newlen = SDS_TYPE_5_LEN(flags) + inc;
            *fp = SDS_TYPE_5 | (newlen << SDS_TYPE_BITS);
        }
            break;
        case SDS_TYPE_8:
            SDS_HDR(8, s)->len += inc;
            break;
        case SDS_TYPE_16:
            SDS_HDR(16, s)->len += inc;
            break;
        case SDS_TYPE_32:
            SDS_HDR(32, s)->len += inc;
            break;
        case SDS_TYPE_64:
            SDS_HDR(64, s)->len += inc;
            break;
    }
}

/*
 * 获取 sds 的实际分配长度，sdsalloc() = sdsavail() + sdslen()
 */
static inline size_t sdsalloc(const sds s) {
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            return SDS_TYPE_5_LEN(flags);
        case SDS_TYPE_8:
            return SDS_HDR(8, s)->alloc;
        case SDS_TYPE_16:
            return SDS_HDR(16, s)->alloc;
        case SDS_TYPE_32:
            return SDS_HDR(32, s)->alloc;
        case SDS_TYPE_64:
            return SDS_HDR(64, s)->alloc;
    }
    return 0;
}

/*
 * 设置 sds 的实际分配长度
 *
 * T = O(1)
 */
static inline void sdssetalloc(sds s, size_t newlen) {
    unsigned char flags = s[-1];
    switch (flags & SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            /* Nothing to do, this type has no total allocation info. */
            break;
        case SDS_TYPE_8:
            SDS_HDR(8, s)->alloc = newlen;
            break;
        case SDS_TYPE_16:
            SDS_HDR(16, s)->alloc = newlen;
            break;
        case SDS_TYPE_32:
            SDS_HDR(32, s)->alloc = newlen;
            break;
        case SDS_TYPE_64:
            SDS_HDR(64, s)->alloc = newlen;
            break;
    }
}

/*------------------------------------- 以下是函数定义 --------------------------------------*/
/**
 * 创建 sds 类型变量，即 char* 类型变量
 * @param init
 * @param initlen
 * @return
 */
sds sdsnewlen(const void *init, size_t initlen);

sds sdstrynewlen(const void *init, size_t initlen);

sds sdsnew(const char *init);

sds sdsempty(void);

sds sdsdup(const sds s);

void sdsfree(sds s);

sds sdsgrowzero(sds s, size_t len);

/**
 * 字符串追加
 * @param s 源字符串
 * @param t 目标字符串
 * @param len 要追加的长度
 * @return
 */
sds sdscatlen(sds s, const void *t, size_t len);

sds sdscat(sds s, const char *t);

sds sdscatsds(sds s, const sds t);

sds sdscpylen(sds s, const char *t, size_t len);

sds sdscpy(sds s, const char *t);

sds sdscatvprintf(sds s, const char *fmt, va_list ap);

#ifdef __GNUC__

sds sdscatprintf(sds s, const char *fmt, ...)
__attribute__((format(printf, 2, 3)));

#else
sds sdscatprintf(sds s, const char *fmt, ...);
#endif

sds sdscatfmt(sds s, char const *fmt, ...);

sds sdstrim(sds s, const char *cset);

void sdsrange(sds s, ssize_t start, ssize_t end);

void sdsupdatelen(sds s);

void sdsclear(sds s);

int sdscmp(const sds s1, const sds s2);

sds *sdssplitlen(const char *s, ssize_t len, const char *sep, int seplen, int *count);

void sdsfreesplitres(sds *tokens, int count);

void sdstolower(sds s);

void sdstoupper(sds s);

sds sdsfromlonglong(long long value);

sds sdscatrepr(sds s, const char *p, size_t len);

sds *sdssplitargs(const char *line, int *argc);

sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen);

sds sdsjoin(char **argv, int argc, char *sep);

sds sdsjoinsds(sds *argv, int argc, const char *sep, size_t seplen);

/* Callback for sdstemplate. The function gets called by sdstemplate
 * every time a variable needs to be expanded. The variable name is
 * provided as variable, and the callback is expected to return a
 * substitution value. Returning a NULL indicates an error.
 */
typedef sds (*sdstemplate_callback_t)(const sds variable, void *arg);

sds sdstemplate(const char *template, sdstemplate_callback_t cb_func, void *cb_arg);

/* Low level functions exposed to the user API */
sds sdsMakeRoomFor(sds s, size_t addlen);

void sdsIncrLen(sds s, ssize_t incr);

sds sdsRemoveFreeSpace(sds s);

size_t sdsAllocSize(sds s);

void *sdsAllocPtr(sds s);

/* Export the allocator used by SDS to the program using SDS.
 * Sometimes the program SDS is linked to, may use a different set of
 * allocators, but may want to allocate or free things that SDS will
 * respectively free or allocate. */
void *sds_malloc(size_t size);

void *sds_realloc(void *ptr, size_t size);

void sds_free(void *ptr);

#ifdef REDIS_TEST
int sdsTest(int argc, char *argv[], int accurate);
#endif

#endif
