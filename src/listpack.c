/* Listpack -- A lists of strings serialization format
 *
 * This file implements the specification you can find at:
 *
 *  https://github.com/antirez/listpack
 *
 * Copyright (c) 2017, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2020, Redis Labs, Inc
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

#include <stdint.h>
#include <limits.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "listpack.h"
#include "listpack_malloc.h"
#include "redisassert.h"

/*
 * listpack  头大小，默认 6 个字节；4 个字节记录 listpack 的总字节数，2 个字节记录 listpack 的元素数量
 *
 * listpack 头大小 32bits + 16bits
 */
#define LP_HDR_SIZE 6       /* 32 bit total len + 16 bit number of elements. */

/*
 * listpack 的最后一个字节是用来标识 listpack 的结束。
 * 和 ziplist 的结束标记一样，LP_EOF 的值也是 255
 */
#define LP_EOF 0xFF


#define LP_HDR_NUMELE_UNKNOWN UINT16_MAX

#define LP_MAX_INT_ENCODING_LEN 9
#define LP_MAX_BACKLEN_SIZE 5


#define LP_MAX_ENTRY_BACKLEN 34359738367ULL
/*
 * encoding 类型
 *  0 整数编码
 *  1 字符串编码
 */
#define LP_ENCODING_INT 0
#define LP_ENCODING_STRING 1

/* LP_ENCODING__XX_BIT_INT 和 LP_ENCODING__XX_BIT_STR ，定义了 listpack 的元素编码类型。具体来说，listpack 元素会对不同长度的整数和字符串进行编码。 */

/*
 * 1 表示元素的实际数据是一个 7 bit 的无符号整数。
 * 2 该编码类型定义值为 0 ，所以编码类型的值也相应为 0 ，占 1 个 bit
 * -------
 * 1 编码类型和元素实际数据共用 1 字节，这个字节的最高位为 0 ，表示编码类型；后续的 7 位用来存储 7 bit 的无符号整数，即整数值存储在后续的 7 位中。
 */
#define LP_ENCODING_7BIT_UINT 0 // 2 进制：00000000
#define LP_ENCODING_7BIT_UINT_MASK 0x80  // 16 进制： 0x80 -> 2 进制： 10000000
#define LP_ENCODING_IS_7BIT_UINT(byte) (((byte)&LP_ENCODING_7BIT_UINT_MASK)==LP_ENCODING_7BIT_UINT)


/*
 * 1 表示元素的实际数据是一个 13 bit 的整数
 * 2 该编码类型定义值为 1100 0000 ，所以这个二进制值中的后 5 位和后续的 8 位，共 13 位用来存储 13bit 的整数。
 * --------
 * 1 编码类型和元素实际数据共用 2 字节，对应二进制值中的前 3 位 100 ，用来表示当前的编码类型；后续的 13 位用来存储 13bit 整数。
 */
#define LP_ENCODING_13BIT_INT 0xC0 // 16 进制： 0xC0 -> 2 进制： 11000000
#define LP_ENCODING_13BIT_INT_MASK 0xE0  // 16 进制： 0xE0 -> 2 进制： 11100000
#define LP_ENCODING_IS_13BIT_INT(byte) (((byte)&LP_ENCODING_13BIT_INT_MASK)==LP_ENCODING_13BIT_INT)

#define LP_ENCODING_16BIT_INT 0xF1 // 16 进制： 0xF1 -> 2 进制： 11110001
#define LP_ENCODING_16BIT_INT_MASK 0xFF // 16 进制： 0xFF -> 2 进制： 11111111
#define LP_ENCODING_IS_16BIT_INT(byte) (((byte)&LP_ENCODING_16BIT_INT_MASK)==LP_ENCODING_16BIT_INT)

#define LP_ENCODING_24BIT_INT 0xF2  // 16 进制： 0xF2 -> 2 进制： 11110010
#define LP_ENCODING_24BIT_INT_MASK 0xFF  // 16 进制： 0xFF -> 2 进制： 11111111
#define LP_ENCODING_IS_24BIT_INT(byte) (((byte)&LP_ENCODING_24BIT_INT_MASK)==LP_ENCODING_24BIT_INT)

#define LP_ENCODING_32BIT_INT 0xF3  // 16 进制： 0xF3 -> 2 进制： 11110011
#define LP_ENCODING_32BIT_INT_MASK 0xFF  // 16 进制： 0xFF -> 2 进制： 11111111
#define LP_ENCODING_IS_32BIT_INT(byte) (((byte)&LP_ENCODING_32BIT_INT_MASK)==LP_ENCODING_32BIT_INT)

#define LP_ENCODING_64BIT_INT 0xF4 // 16 进制： 0xF4 -> 2 进制： 11110100
#define LP_ENCODING_64BIT_INT_MASK 0xFF  // 16 进制： 0xFF -> 2 进制： 11111111
#define LP_ENCODING_IS_64BIT_INT(byte) (((byte)&LP_ENCODING_64BIT_INT_MASK)==LP_ENCODING_64BIT_INT)


/*
 * 对于字符串来说，一共有三种类型，如下。
 *
 * 整数编码类型名称中 BIT 前面的数字表示的是整数的长度。字符串编码类似，字符串编码类型名称中 BIT 前的数字，表示的就是字符串的长度。
 */

/*
 * 编码类型为 LP_ENCODING_6BIT_STR 时，编码类型占 1 字节。
 *
 * 1 该类型的宏定义值是 10000000 ，前 2 位是用来标识编码类型本身，后 6 位保存的是字符串长度，注意不是字符串内容。这是和整数编码最大的区别。
 * 2 列表项中的数据部分保存了实际的字符串。
 */
#define LP_ENCODING_6BIT_STR 0x80  // 16 进制： 0x80 -> 2 进制： 10000000
#define LP_ENCODING_6BIT_STR_MASK 0xC0 // 16 进制： 0xC0 -> 2 进制： 11000000
#define LP_ENCODING_IS_6BIT_STR(byte) (((byte)&LP_ENCODING_6BIT_STR_MASK)==LP_ENCODING_6BIT_STR)

#define LP_ENCODING_12BIT_STR 0xE0 // 16 进制： 0xE0 -> 2 进制： 11100000
#define LP_ENCODING_12BIT_STR_MASK 0xF0 // 16 进制： 0xF0 -> 2 进制： 11110000
#define LP_ENCODING_IS_12BIT_STR(byte) (((byte)&LP_ENCODING_12BIT_STR_MASK)==LP_ENCODING_12BIT_STR)

#define LP_ENCODING_32BIT_STR 0xF0 // 16 进制： 0xF0 -> 2 进制： 11110000
#define LP_ENCODING_32BIT_STR_MASK 0xFF  // 16 进制： 0xFF -> 2 进制： 11111111
#define LP_ENCODING_IS_32BIT_STR(byte) (((byte)&LP_ENCODING_32BIT_STR_MASK)==LP_ENCODING_32BIT_STR)


#define LP_ENCODING_6BIT_STR_LEN(p) ((p)[0] & 0x3F)
#define LP_ENCODING_12BIT_STR_LEN(p) ((((p)[0] & 0xF) << 8) | (p)[1])
#define LP_ENCODING_32BIT_STR_LEN(p) (((uint32_t)(p)[1]<<0) | \
                                      ((uint32_t)(p)[2]<<8) | \
                                      ((uint32_t)(p)[3]<<16) | \
                                      ((uint32_t)(p)[4]<<24))


/* 不难看出，total_bytes 和 size 字段都是固定大小的 **/

/*
 * 4 个字节记录 listpack 的总字节数
 */
#define lpGetTotalBytes(p)           (((uint32_t)(p)[0]<<0) | \
                                      ((uint32_t)(p)[1]<<8) | \
                                      ((uint32_t)(p)[2]<<16) | \
                                      ((uint32_t)(p)[3]<<24))

/*
 * 2 个字节记录 listpack 的元素数量
 */
#define lpGetNumElements(p)          (((uint32_t)(p)[4]<<0) | \
                                      ((uint32_t)(p)[5]<<8))

/*
 * 设置 listpack 的大小，使用 4 个字节保存
 */
#define lpSetTotalBytes(p, v) do { \
    (p)[0] = (v)&0xff; \
    (p)[1] = ((v)>>8)&0xff; \
    (p)[2] = ((v)>>16)&0xff; \
    (p)[3] = ((v)>>24)&0xff; \
} while(0)

/*
 * 设置 listpack 的元素数量，使用 2 个字节保存
 */
#define lpSetNumElements(p, v) do { \
    (p)[4] = (v)&0xff; \
    (p)[5] = ((v)>>8)&0xff; \
} while(0)

/* Validates that 'p' is not ouside the listpack.
 * All function that return a pointer to an element in the listpack will assert
 * that this element is valid, so it can be freely used.
 * Generally functions such lpNext and lpDelete assume the input pointer is
 * already validated (since it's the return value of another function). */
#define ASSERT_INTEGRITY(lp, p) do { \
    assert((p) >= (lp)+LP_HDR_SIZE && (p) < (lp)+lpGetTotalBytes((lp))); \
} while (0)

/* Similar to the above, but validates the entire element lenth rather than just
 * it's pointer. */
#define ASSERT_INTEGRITY_LEN(lp, p, len) do { \
    assert((p) >= (lp)+LP_HDR_SIZE && (p)+(len) < (lp)+lpGetTotalBytes((lp))); \
} while (0)

/* Convert a string into a signed 64 bit integer.
 * The function returns 1 if the string could be parsed into a (non-overflowing)
 * signed 64 bit int, 0 otherwise. The 'value' will be set to the parsed value
 * when the function returns success.
 *
 * Note that this function demands that the string strictly represents
 * a int64 value: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. *
 *
 * -----------------------------------------------------------------------------
 *
 * Credits: this function was adapted from the Redis source code, file
 * "utils.c", function string2ll(), and is copyright:
 *
 * Copyright(C) 2011, Pieter Noordhuis
 * Copyright(C) 2011, Salvatore Sanfilippo
 *
 * The function is released under the BSD 3-clause license.
 *
 * 尝试将 string 转为整数
 */
int lpStringToInt64(const char *s, unsigned long slen, int64_t *value) {
    const char *p = s;
    unsigned long plen = 0;
    int negative = 0;
    uint64_t v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++;
        plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0] - '0';
        p++;
        plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (UINT64_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (UINT64_MAX - (p[0] - '0'))) /* Overflow. */
            return 0;
        v += p[0] - '0';

        p++;
        plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((uint64_t) (-(INT64_MIN + 1)) + 1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > INT64_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

/* Create a new, empty listpack.
 * On success the new listpack is returned, otherwise an error is returned.
 * Pre-allocate at least `capacity` bytes of memory,
 * over-allocated memory can be shrinked by `lpShrinkToFit`.
 *
 * 创建一个指定大小的 listpack ，如果指定大小 > listpack 头大小（6字节） + 1，就使用传入的大小，否则使用 listpack 头大小（6字节） + 1
 *
 */
unsigned char *lpNew(size_t capacity) {
    // 1 分配 listpack 大小，头 + 末端
    unsigned char *lp = lp_malloc(capacity > LP_HDR_SIZE + 1 ? capacity : LP_HDR_SIZE + 1);
    if (lp == NULL) return NULL;

    /*
     * 2 设置 listpack 的大小
     *  - 4字节用来记录totalLen
     *  - 2字节用来记录元素的个数
     *  - +1的一个字节用来标识end，end也恒为0xFF
     */
    lpSetTotalBytes(lp, LP_HDR_SIZE + 1);

    // 3 设置 listpack 的元素个数，初始值为 0
    lpSetNumElements(lp, 0);

    // 4 设置 listpack 的结尾标识为 LP_EOF ，值为 255
    lp[LP_HDR_SIZE] = LP_EOF;

    return lp;
}

/* Free the specified listpack.
 *
 * 释放 listpack
 */
void lpFree(unsigned char *lp) {
    lp_free(lp);
}


/* Shrink the memory to fit.
 *
 * 缩小内存以适应。
 */
unsigned char *lpShrinkToFit(unsigned char *lp) {
    size_t size = lpGetTotalBytes(lp);
    if (size < lp_malloc_size(lp)) {
        return lp_realloc(lp, size);
    } else {
        return lp;
    }
}

/* Given an element 'ele' of size 'size', determine if the element can be
 * represented inside the listpack encoded as integer, and returns
 * LP_ENCODING_INT if so. Otherwise returns LP_ENCODING_STR if no integer
 * encoding is possible.
 *
 * If the LP_ENCODING_INT is returned, the function stores the integer encoded
 * representation of the element in the 'intenc' buffer.
 *
 * Regardless of the returned encoding, 'enclen' is populated by reference to
 * the number of bytes that the string or integer encoded element will require
 * in order to be represented.
 *
 * todo enclen 表示的是元素 ele 的 encoding 大小，具体要看是整数还是字符串。
 *
 * 1 整数： 编码类型（encoding（前几位））+ 数据长度（encoding 后几位），即编码类型字节数。
 * 2 字符串：编码类型字节数 + 字符串长度。
 *
 */
int lpEncodeGetType(unsigned char *ele, uint32_t size, unsigned char *intenc, uint64_t *enclen) {
    int64_t v;

    // 1 尝试将字符串转为整数，并将其设置到 v 中
    if (lpStringToInt64((const char *) ele, size, &v)) {

        // 将整数数据也存到编码 encoding 中
        if (v >= 0 && v <= 127) {
            /* Single byte 0-127 integer. */
            intenc[0] = v;
            *enclen = 1; // 需要 1 个字节编码
        } else if (v >= -4096 && v <= 4095) {
            /* 13 bit integer. */
            if (v < 0) v = ((int64_t) 1 << 13) + v;
            intenc[0] = (v >> 8) | LP_ENCODING_13BIT_INT;
            intenc[1] = v & 0xff;
            *enclen = 2; // 需要 2 个字节编码
        } else if (v >= -32768 && v <= 32767) {
            /* 16 bit integer. */
            if (v < 0) v = ((int64_t) 1 << 16) + v;
            intenc[0] = LP_ENCODING_16BIT_INT;
            intenc[1] = v & 0xff;
            intenc[2] = v >> 8;
            *enclen = 3;  // 需要 3 个字节编码
        } else if (v >= -8388608 && v <= 8388607) {
            /* 24 bit integer. */
            if (v < 0) v = ((int64_t) 1 << 24) + v;
            intenc[0] = LP_ENCODING_24BIT_INT;
            intenc[1] = v & 0xff;
            intenc[2] = (v >> 8) & 0xff;
            intenc[3] = v >> 16;
            *enclen = 4; // 需要 4 个字节编码
        } else if (v >= -2147483648 && v <= 2147483647) {
            /* 32 bit integer. */
            if (v < 0) v = ((int64_t) 1 << 32) + v;
            intenc[0] = LP_ENCODING_32BIT_INT;
            intenc[1] = v & 0xff;
            intenc[2] = (v >> 8) & 0xff;
            intenc[3] = (v >> 16) & 0xff;
            intenc[4] = v >> 24;
            *enclen = 5; // 需要 5 个字节编码
        } else {
            /* 64 bit integer. */
            uint64_t uv = v;
            intenc[0] = LP_ENCODING_64BIT_INT;
            intenc[1] = uv & 0xff;
            intenc[2] = (uv >> 8) & 0xff;
            intenc[3] = (uv >> 16) & 0xff;
            intenc[4] = (uv >> 24) & 0xff;
            intenc[5] = (uv >> 32) & 0xff;
            intenc[6] = (uv >> 40) & 0xff;
            intenc[7] = (uv >> 48) & 0xff;
            intenc[8] = uv >> 56;
            *enclen = 9; // 需要 9 个字节编码
        }
        return LP_ENCODING_INT;


        // 2 并非整数，而是字符串
    } else {
        // 1 -> 1 个字节
        // 需要 1 个字节编码 + 字符串大小
        if (size < 64) *enclen = 1 + size;
            // 2 -> 2 个字节
            // 需要 2 个字节编码 + 字符串大小
        else if (size < 4096) *enclen = 2 + size;
            // 5 > 5 个字节
            // 需要 5 个字节编码 + 字符串大小
        else *enclen = 5 + size;
        return LP_ENCODING_STRING;
    }
}

/* Store a reverse-encoded variable length field, representing the length
 * of the previous element of size 'l', in the target buffer 'buf'.
 *
 * 根据编码方式和实际数据长度之和，存储一个反向编码的可变长度字段到 buf 中，表示入参 l 的长度。
 *
 * The function returns the number of bytes used to encode it, from
 * 1 to 5. If 'buf' is NULL the function just returns the number of bytes
 * needed in order to encode the backlen.
 *
 * 该函数返回用于对 l 进行编码的字节数，从 1 ～ 5 。
 * 如果 buf 为 NULL ，函数只返回编码 l 所需的字节数，不会存储一个反向编码的可变长度字段。
 *
 *
 * todo listpack 元素长度的编码可以是 1、2、3、4、5
 *
 * 1 用最高位字节为 0 来当作边界
 * 2 倒序顺序，前 4 个字节 7 bit 有效，第 5 个字节 8 bit 有效
 *
 * todo 在读取时，倒序读取，直到读取到最高字节的最高 0 位
 */
unsigned long lpEncodeBacklen(unsigned char *buf, uint64_t l) {
    // 编码类型和实际数据 总长度 <= 127，entry-len 长度使用 1 字节
    if (l <= 127) { // 2^7 - 1

        if (buf) buf[0] = l;
        return 1;

        // 编码类型和实际数据的总长度大于127但小于16383，entry-len长度使用 2 字节
    } else if (l < 16383) { // 2^14 -1
        if (buf) {
            buf[0] = l >> 7;
            buf[1] = (l & 127) | 128;
        }
        return 2;

        // 编码类型和实际数据的总长度大于16383但小于2097151，entry-len长度使用 3 字节
    } else if (l < 2097151) { // 2^21 - 1
        if (buf) {
            buf[0] = l >> 14;
            buf[1] = ((l >> 7) & 127) | 128;
            buf[2] = (l & 127) | 128;
        }
        return 3;

        // 编码类型和实际数据的总长度大于2097151但小于268435455，entry-len长度为4字节
    } else if (l < 268435455) { // 2^28 - 1
        if (buf) {
            buf[0] = l >> 21;
            buf[1] = ((l >> 14) & 127) | 128;
            buf[2] = ((l >> 7) & 127) | 128;
            buf[3] = (l & 127) | 128;
        }
        return 4;

        // 否则，entry-len 长度使用 5 字节
    } else {
        if (buf) {
            buf[0] = l >> 28;
            buf[1] = ((l >> 21) & 127) | 128;
            buf[2] = ((l >> 14) & 127) | 128;
            buf[3] = ((l >> 7) & 127) | 128;
            buf[4] = (l & 127) | 128;
        }
        return 5;
    }
}

/* Decode the backlen and returns it. If the encoding looks invalid (more than
 * 5 bytes are used), UINT64_MAX is returned to report the problem.
 *
 * 解码 backlen 并返回它。如果编码看起来无效(超过5字节被使用)，UINT64_MAX被返回报告问题。
 *
 * todo 通过指向 length 最后一个字节的指针解析出此 entry 的 length ，和 lpEncodeBacklen 函数对应。
 */
uint64_t lpDecodeBacklen(unsigned char *p) {
    uint64_t val = 0;
    uint64_t shift = 0;

    do {
        /*
         * 这里之所以要与第 8 位进行 & 运算一下，等于 0 就退出，原因在下：
         *
         * 该函数中，length 的编码是逐字节倒序的，而且最高字节最高位一定会设置为 0 （length 为 5 以下），以此划分 length 的边界。
         * 这个过程其实是编码的逆运算。
         */
        // todo 或运算，将有效位的数据全部映射，最终还原长度
        val |= (uint64_t) (p[0] & 127) << shift;

        // 判断当前字节的最高位是否为 0 ，最高位为 0 表示当前字节已经是 length 最后一个字节了，结束。
        if (!(p[0] & 128)) break;

        // 低 7 位记录 length 信息
        shift += 7;

        // 逐字节倒序读取
        p--;

        // 达到 5 字节
        if (shift > 28) return UINT64_MAX;
    } while (1);

    return val;
}

/* Encode the string element pointed by 's' of size 'len' in the target
 * buffer 's'. The function should be called with 'buf' having always enough
 * space for encoding the string. This is done by calling lpEncodeGetType()
 * before calling this function.
 *
 * todo 将 s 指向的字符串这个数据编码到目标缓冲中，也就是 buf 移动指针的空间，大小为 len 。
 *
 * 在调用函数时，buf 应该总是有足够的空间来编码字符串，这个是通过先调用 lpEncodeGetType() 保证的。
 */
void lpEncodeString(unsigned char *buf, unsigned char *s, uint32_t len) {

    // 根据字符串不同的长度，写入对于的位置
    if (len < 64) {
        buf[0] = len | LP_ENCODING_6BIT_STR;
        memcpy(buf + 1, s, len);
    } else if (len < 4096) {
        buf[0] = (len >> 8) | LP_ENCODING_12BIT_STR;
        buf[1] = len & 0xff;
        memcpy(buf + 2, s, len);
    } else {
        buf[0] = LP_ENCODING_32BIT_STR;
        buf[1] = len & 0xff;
        buf[2] = (len >> 8) & 0xff;
        buf[3] = (len >> 16) & 0xff;
        buf[4] = (len >> 24) & 0xff;
        memcpy(buf + 5, s, len);
    }
}

/* Return the encoded length of the listpack element pointed by 'p'.
 * This includes the encoding byte, length bytes, and the element data itself.
 * If the element encoding is wrong then 0 is returned.
 * Note that this method may access additional bytes (in case of 12 and 32 bit
 * str), so should only be called when we know 'p' was already validated by
 * lpCurrentEncodedSizeBytes or ASSERT_INTEGRITY_LEN (possibly since 'p' is
 * a return value of another function that validated its return.
 *
 * todo 根据 p 指向的列表项第 1 个字节的值，判断当前项的编码类型，并根据编码类型计算当前项编码方式和实际数据的总长度，具体情况要分是整数还是字符串。
 *
 * 1 如果是整数类型，那么编码类型+数据大小，就是编码方式大小。因为整数没有单独存储在数据项，而是直接存储在编码方式中。
 * 2 如果是字符串类型，那么编码类型+数据大小，就是两部分的总和了。一部分是编码方式大小，另一部分是字符串长度，字符串是需要单独存储在列表项的数据部分的。
 *   整数不需要单独存储，因此编码方式大小不需要加上整数长度。
 * 3 该函数返回的是，当前项编码方式和实际数据的总长度，其中整数的话，没有实际数据长度，字符串才有
 *
 * 4 encoding 表示编码类型和数据大小的总和
 */
uint32_t lpCurrentEncodedSizeUnsafe(unsigned char *p) {
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) return 1;
    if (LP_ENCODING_IS_6BIT_STR(p[0])) return 1 + LP_ENCODING_6BIT_STR_LEN(p);
    if (LP_ENCODING_IS_13BIT_INT(p[0])) return 2;
    if (LP_ENCODING_IS_16BIT_INT(p[0])) return 3;
    if (LP_ENCODING_IS_24BIT_INT(p[0])) return 4;
    if (LP_ENCODING_IS_32BIT_INT(p[0])) return 5;
    if (LP_ENCODING_IS_64BIT_INT(p[0])) return 9;
    if (LP_ENCODING_IS_12BIT_STR(p[0])) return 2 + LP_ENCODING_12BIT_STR_LEN(p);
    if (LP_ENCODING_IS_32BIT_STR(p[0])) return 5 + LP_ENCODING_32BIT_STR_LEN(p);
    if (p[0] == LP_EOF) return 1;
    return 0;
}

/* Return bytes needed to encode the length of the listpack element pointed by 'p'.
 * This includes just the encodign byte, and the bytes needed to encode the length
 * of the element (excluding the element data itself)
 * If the element encoding is wrong then 0 is returned. */
uint32_t lpCurrentEncodedSizeBytes(unsigned char *p) {
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) return 1;
    if (LP_ENCODING_IS_6BIT_STR(p[0])) return 1;
    if (LP_ENCODING_IS_13BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_16BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_24BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_32BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_64BIT_INT(p[0])) return 1;
    if (LP_ENCODING_IS_12BIT_STR(p[0])) return 2;
    if (LP_ENCODING_IS_32BIT_STR(p[0])) return 5;
    if (p[0] == LP_EOF) return 1;
    return 0;
}

/* Skip the current entry returning the next. It is invalid to call this
 * function if the current element is the EOF element at the end of the
 * listpack, however, while this function is used to implement lpNext(),
 * it does not return NULL when the EOF element is encountered. */
unsigned char *lpSkip(unsigned char *p) {

    // 1 根据当前列表项（p指向的元素）第 1 个字节的取值，用于计算当前项的编码方式，并根据编码方式，计算当前项编码类型和实际数据的总长度。
    // todo 即 求出 p 所指列表项的 encoding 和 content 的总大小。注意，整数和字符串在 content 大小上的区别。
    unsigned long entrylen = lpCurrentEncodedSizeUnsafe(p);

    // 2 根据 encoding 和 content 的总大小，进一步计算列表项最后一部分 length
    entrylen += lpEncodeBacklen(NULL, entrylen);

    /* 执行到这里，可以知道：当前项的编码类型、实际数据和 entry-len 的总长度*/

    // 3 将当前项指针向右偏移相应的长度，从而实现查到下一个列表项的目的
    // todo 指针移动 p 指向元素的大小
    p += entrylen;

    return p;
}

/* If 'p' points to an element of the listpack, calling lpNext() will return
 * the pointer to the next element (the one on the right), or NULL if 'p'
 * already pointed to the last element of the listpack.
 *
 * 如果'p'指向列表包中的一个元素，调用lpNext()将返回指向下一个元素(右边的那个)的指针，如果'p'已经指向列表包中的最后一个元素，则返回NULL。
 *
 */
unsigned char *lpNext(unsigned char *lp, unsigned char *p) {
    assert(p);

    // 调用 lpSkip 函数，偏移指针指向下一个列表项地址
    // todo 本质上计算出 p 指向的元素大小，然后移动指针对应偏移量
    p = lpSkip(p);
    ASSERT_INTEGRITY(lp, p);

    // 如果下一个列表指尾端，则返回 NULL
    if (p[0] == LP_EOF) return NULL;
    return p;
}

/* If 'p' points to an element of the listpack, calling lpPrev() will return
 * the pointer to the previous element (the one on the left), or NULL if 'p'
 * already pointed to the first element of the listpack.
 *
 * 如果'p'指向列表包中的一个元素，调用lpPrev()将返回指向前一个元素(左边的那个)的指针，如果'p'已经指向列表包中的第一个元素，则返回NULL。
 */
unsigned char *lpPrev(unsigned char *lp, unsigned char *p) {
    assert(p);
    // 没有前一个元素
    if (p - lp == LP_HDR_SIZE) return NULL;

    // 查找最后一个元素的第一个backlen字节。
    // todo 这个是由于 listpack 列表项结构决定的
    p--; /* Seek the first backlen byte of the last element. */

    // 从右向左，逐个字节地读取当前项的 length
    // todo 核心点
    uint64_t prevlen = lpDecodeBacklen(p);

    // 计算得到的 length 本身字节大小 // todo ？？？
    prevlen += lpEncodeBacklen(NULL, prevlen);

    // 这样一来，就得到了 p 指向的列表项的前一项的总长度
    p -= prevlen - 1; /* Seek the first byte of the previous entry. */
    ASSERT_INTEGRITY(lp, p);
    return p;
}

/* Return a pointer to the first element of the listpack, or NULL if the
 * listpack has no elements.
 *
 * 返回指向 listpack 中第一个元素的指针，如果列表没有元素则返回 NULL
 *
 * lp 是 listpack 的指针
 *
 */
unsigned char *lpFirst(unsigned char *lp) {
    // 1 跳过 listpack 头部 6 个字节
    lp += LP_HDR_SIZE; /* Skip the header. */

    // 2 如果已经是 listpack 的末端结束字节，则返回 NULL
    if (lp[0] == LP_EOF) return NULL;

    return lp;
}

/* Return a pointer to the last element of the listpack, or NULL if the
 * listpack has no elements.
 *
 * 返回指向列表最后一个元素的指针，如果列表中没有元素则返回 NULL
 */
unsigned char *lpLast(unsigned char *lp) {

    // 根据 listpack 头中记录的 listpack 总长度（总字节），定位 listpack 的尾部结束标记
    // 即 定位到 EOF 起始的位置
    unsigned char *p = lp + lpGetTotalBytes(lp) - 1; /* Seek EOF element. */

    // 返回指向列表最后一个元素的指针
    return lpPrev(lp, p); /* Will return NULL if EOF is the only element. */
}

/* Return the number of elements inside the listpack. This function attempts
 * to use the cached value when within range, otherwise a full scan is
 * needed. As a side effect of calling this function, the listpack header
 * could be modified, because if the count is found to be already within
 * the 'numele' header field range, the new value is set. */
uint32_t lpLength(unsigned char *lp) {
    // 直接从头部第 5 个字节开始，获取2个字节的数据，也就是元素个数
    uint32_t numele = lpGetNumElements(lp);

    // 没有达到 65535 直接返回
    if (numele != LP_HDR_NUMELE_UNKNOWN) return numele;

    /* Too many elements inside the listpack. We need to scan in order
     * to get the total number. */

    // 达到 65535 个后，需要遍历 listpack
    uint32_t count = 0;

    // 根据 listpack 头大小获取第一个元素的指针
    unsigned char *p = lpFirst(lp);
    while (p) {
        count++;

        // 向后遍历
        p = lpNext(lp, p);
    }

    /* If the count is again within range of the header numele field,
     * set it. */
    if (count < LP_HDR_NUMELE_UNKNOWN) lpSetNumElements(lp, count);

    return count;
}

/* Return the listpack element pointed by 'p'.
 *
 * The function changes behavior depending on the passed 'intbuf' value.
 * Specifically, if 'intbuf' is NULL:
 *
 * If the element is internally encoded as an integer, the function returns
 * NULL and populates the integer value by reference in 'count'. Otherwise if
 * the element is encoded as a string a pointer to the string (pointing inside
 * the listpack itself) is returned, and 'count' is set to the length of the
 * string.
 *
 * If instead 'intbuf' points to a buffer passed by the caller, that must be
 * at least LP_INTBUF_SIZE bytes, the function always returns the element as
 * it was a string (returning the pointer to the string and setting the
 * 'count' argument to the string length by reference). However if the element
 * is encoded as an integer, the 'intbuf' buffer is used in order to store
 * the string representation.
 *
 * The user should use one or the other form depending on what the value will
 * be used for. If there is immediate usage for an integer value returned
 * by the function, than to pass a buffer (and convert it back to a number)
 * is of course useless.
 *
 * If the function is called against a badly encoded ziplist, so that there
 * is no valid way to parse it, the function returns like if there was an
 * integer encoded with value 12345678900000000 + <unrecognized byte>, this may
 * be an hint to understand that something is wrong. To crash in this case is
 * not sensible because of the different requirements of the application using
 * this lib.
 *
 * Similarly, there is no error returned since the listpack normally can be
 * assumed to be valid, so that would be a very high API cost. However a function
 * in order to check the integrity of the listpack at load time is provided,
 * check lpIsValid(). */
unsigned char *lpGet(unsigned char *p, int64_t *count, unsigned char *intbuf) {
    int64_t val;
    uint64_t uval, negstart, negmax;

    assert(p); /* assertion for valgrind (avoid NPD) */
    if (LP_ENCODING_IS_7BIT_UINT(p[0])) {
        negstart = UINT64_MAX; /* 7 bit ints are always positive. */
        negmax = 0;
        uval = p[0] & 0x7f;
    } else if (LP_ENCODING_IS_6BIT_STR(p[0])) {
        *count = LP_ENCODING_6BIT_STR_LEN(p);
        return p + 1;
    } else if (LP_ENCODING_IS_13BIT_INT(p[0])) {
        uval = ((p[0] & 0x1f) << 8) | p[1];
        negstart = (uint64_t) 1 << 12;
        negmax = 8191;
    } else if (LP_ENCODING_IS_16BIT_INT(p[0])) {
        uval = (uint64_t) p[1] |
               (uint64_t) p[2] << 8;
        negstart = (uint64_t) 1 << 15;
        negmax = UINT16_MAX;
    } else if (LP_ENCODING_IS_24BIT_INT(p[0])) {
        uval = (uint64_t) p[1] |
               (uint64_t) p[2] << 8 |
               (uint64_t) p[3] << 16;
        negstart = (uint64_t) 1 << 23;
        negmax = UINT32_MAX >> 8;
    } else if (LP_ENCODING_IS_32BIT_INT(p[0])) {
        uval = (uint64_t) p[1] |
               (uint64_t) p[2] << 8 |
               (uint64_t) p[3] << 16 |
               (uint64_t) p[4] << 24;
        negstart = (uint64_t) 1 << 31;
        negmax = UINT32_MAX;
    } else if (LP_ENCODING_IS_64BIT_INT(p[0])) {
        uval = (uint64_t) p[1] |
               (uint64_t) p[2] << 8 |
               (uint64_t) p[3] << 16 |
               (uint64_t) p[4] << 24 |
               (uint64_t) p[5] << 32 |
               (uint64_t) p[6] << 40 |
               (uint64_t) p[7] << 48 |
               (uint64_t) p[8] << 56;
        negstart = (uint64_t) 1 << 63;
        negmax = UINT64_MAX;
    } else if (LP_ENCODING_IS_12BIT_STR(p[0])) {
        *count = LP_ENCODING_12BIT_STR_LEN(p);
        return p + 2;
    } else if (LP_ENCODING_IS_32BIT_STR(p[0])) {
        *count = LP_ENCODING_32BIT_STR_LEN(p);
        return p + 5;
    } else {
        uval = 12345678900000000ULL + p[0];
        negstart = UINT64_MAX;
        negmax = 0;
    }

    /* We reach this code path only for integer encodings.
     * Convert the unsigned value to the signed one using two's complement
     * rule. */
    if (uval >= negstart) {
        /* This three steps conversion should avoid undefined behaviors
         * in the unsigned -> signed conversion. */
        uval = negmax - uval;
        val = uval;
        val = -val - 1;
    } else {
        val = uval;
    }

    /* Return the string representation of the integer or the value itself
     * depending on intbuf being NULL or not. */
    if (intbuf) {
        *count = snprintf((char *) intbuf, LP_INTBUF_SIZE, "%lld", (long long) val);
        return intbuf;
    } else {
        *count = val;
        return NULL;
    }
}

/* Insert, delete or replace the specified element 'ele' of length 'len' at
 * the specified position 'p', with 'p' being a listpack element pointer
 * obtained with lpFirst(), lpLast(), lpNext(), lpPrev() or lpSeek().
 *
 * The element is inserted before, after, or replaces the element pointed
 * by 'p' depending on the 'where' argument, that can be LP_BEFORE, LP_AFTER
 * or LP_REPLACE.
 *
 * If 'ele' is set to NULL, the function removes the element pointed by 'p'
 * instead of inserting one.
 *
 * Returns NULL on out of memory or when the listpack total length would exceed
 * the max allowed size of 2^32-1, otherwise the new pointer to the listpack
 * holding the new element is returned (and the old pointer passed is no longer
 * considered valid)
 *
 * If 'newp' is not NULL, at the end of a successful call '*newp' will be set
 * to the address of the element just added, so that it will be possible to
 * continue an interation with lpNext() and lpPrev().
 *
 * 如果'newp'不为NULL，在成功调用结束时，'*newp'将被设置为刚刚添加的元素的地址，这样就可以继续与lpNext()和lpPrev()进行交互。
 *
 * For deletion operations ('ele' set to NULL) 'newp' is set to the next
 * element, on the right of the deleted one, or to NULL if the deleted element
 * was the last one.
 *
 * lp: listpack 结构
 * ele: 插入的数据
 * size: 插入数据的长度
 * p: 这是一个指向某个元素的指针。由 lpFirst()、lpLast()、lpNext()、lpPrev() or lpSeek() 得到
 * where: 插入时是 before 还是 after ，或者替换
 * newp: 用于保存刚刚添加的元素的地址，这样就可以继续与 lpNext() 和 lpPrev() 进行交互了
 *
 */
unsigned char *
lpInsert(unsigned char *lp, unsigned char *ele, uint32_t size, unsigned char *p, int where, unsigned char **newp) {

    // 1 存储 ele 的 encoding (表示了编码类型 和 content 长度) （String 的话不会存储 content，整数会将值存入）
    unsigned char intenc[LP_MAX_INT_ENCODING_LEN];

    // 2 存储对 encoding 编码的字节数，也就是 ele 的返回解析长度 length 值
    unsigned char backlen[LP_MAX_BACKLEN_SIZE];

    // 3 用于存储已编码元素的长度
    uint64_t enclen; /* The length of the encoded element. */

    /* An element pointer set to NULL means deletion, which is conceptually
     * replacing the element with a zero-length element. So whatever we
     * get passed as 'where', set it to LP_REPLACE.
     *
     * 4 将元素指针设置为 NULL ，意味着删除，即在概念上用零长度的元素替换元素。
     * 不管原来是操作哪里，这里将它设为 LP_REPLACE 。
     */
    if (ele == NULL) where = LP_REPLACE;

    /* If we need to insert after the current element, we just jump to the
     * next element (that could be the EOF one) and handle the case of
     * inserting before. So the function will actually deal with just two
     * cases: LP_BEFORE and LP_REPLACE.
     *
     * 5 如果需要在当前元素之后插入，只需跳到下一个元素（可以是 EOF 尾端）并处理之前插入的情况。
     *
     * todo 可以看出 这个函数实际上只处理两种情况: LP_BEFORE 和 LP_REPLACE。
     */
    if (where == LP_AFTER) {
        // 跳到下一个元素
        p = lpSkip(p);

        // 在下一个元素前插入元素
        where = LP_BEFORE;

        ASSERT_INTEGRITY(lp, p);
    }

    /* Store the offset of the element 'p', so that we can obtain its
     * address again after a reallocation.
     *
     * 6 临时保存元素 p 的偏移量，这样我们可以在重新分配后再次获得它的地址。
     */
    unsigned long poff = p - lp;

    /* Calling lpEncodeGetType() results into the encoded version of the
     * element to be stored into 'intenc' in case it is representable as
     * an integer: in that case, the function returns LP_ENCODING_INT.
     *
     * 调用 lpEncodeGetType() 的结果是，如果元素表示为整数，则存储到 intenc 的是编码信息，函数返回 LP_ENCODING_INT
     *
     * Otherwise if LP_ENCODING_STR is returned, we'll have to call
     * lpEncodeString() to actually write the encoded string on place later.
     *
     * 否则，如果返回 LP_ENCODING_STR ，则必须调用 lpEncodeString() 来实际写入编码后的字符串，因为字符串不像整数一样可以直接写入编码信息中，它需要
     * 写入 listpack 列表项的 content 中。
     *
     * Whatever the returned encoding is, 'enclen' is populated with the
     * length of the encoded element.
     *
     * 无论返回的编码是什么，enclen 都会被编码后的元素的长度填充。
     */
    int enctype;
    // 7 如果要插入元素 ele 不为空
    if (ele) {
        // 1 获取元素 ele 的 encoding
        // - 可能是整数，也可能是 str ，返回值为类型，是整数的话写入到 intenc
        // - enclen 记录用于编码元素的 encoding 实际长度
        enctype = lpEncodeGetType(ele, size, intenc, &enclen);

        // 如果元素 ele 为空
    } else {
        enctype = -1;
        enclen = 0;
    }

    /* We need to also encode the backward-parsable length of the element
     * and append it to the end: this allows to traverse the listpack from
     * the end to the start.
     *
     * 还需要对 元素的反向解析长度 进行编码，并将其附加到该元素列表项的末尾。这样就可以从头到尾遍历 listpack 。
     *
     */
    // 8 对编码后的元素的长度 enclen 进行编码，得到元素的反向解析长度
    unsigned long backlen_size = ele ? lpEncodeBacklen(backlen, enclen) : 0;

    // 9 获取 lp 原来的总字节
    uint64_t old_listpack_bytes = lpGetTotalBytes(lp);

    uint32_t replaced_len = 0;

    // 10 如果是替换操作
    if (where == LP_REPLACE) {
        // 获取编码类型和实际数据总长度，即 p 指向元素的 encoding 的大小
        replaced_len = lpCurrentEncodedSizeUnsafe(p);

        // 对 replaced_len 进行编码，并返回编码后的字节数
        replaced_len += lpEncodeBacklen(NULL, replaced_len);

        // 至此，p 的数据大小就是 replaced_len (encoding 大小 + 编码 encoding 字节数)
        ASSERT_INTEGRITY_LEN(lp, p, replaced_len);
    }

    // 计算新的 listpack 的字节数
    // 原来的字节数 + 新元素的 encoding 的长度 + 元素的反向解析长度 - 删除p时需要减去的空间大小
    uint64_t new_listpack_bytes = old_listpack_bytes + enclen + backlen_size - replaced_len;

    // listpack 长度不允许超过 UINT32_MAX
    if (new_listpack_bytes > UINT32_MAX) return NULL;

    /* We now need to reallocate in order to make space or shrink the
     * allocation (in case 'when' value is LP_REPLACE and the new element is
     * smaller). However we do that before memmoving the memory to
     * make room for the new element if the final allocation will get
     * larger, or we do it after if the final allocation will get smaller. */

    // 还原 p 的指向
    unsigned char *dst = lp + poff; /* May be updated after reallocation. */

    /* Realloc before: we need more room. */
    if (new_listpack_bytes > old_listpack_bytes &&
        new_listpack_bytes > lp_malloc_size(lp)) {
        if ((lp = lp_realloc(lp, new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* Setup the listpack relocating the elements to make the exact room
     * we need to store the new one.
     *
     * 不同操作决定不同的行为
     */
    if (where == LP_BEFORE) {
        // 在 p 之前插入，则移动数据，为新插入的元素腾出位置。（todo 知道数据长度，腾出的位置就可以用来写数据）
        // 移动数据的长度：新元素 encoding 长度 + 元素的反向解析长度
        memmove(dst + enclen + backlen_size, dst, old_listpack_bytes - poff);
    } else { /* LP_REPLACE. */
        long lendiff = (enclen + backlen_size) - replaced_len;

        // 基于 dst + rep 开始移动，覆盖掉被删除的元素位置
        memmove(dst + replaced_len + lendiff,
                dst + replaced_len,
                old_listpack_bytes - poff - replaced_len);
    }

    /* Realloc after: we need to free space. */
    // 在替换的情况下，缩小
    if (new_listpack_bytes < old_listpack_bytes) {
        if ((lp = lp_realloc(lp, new_listpack_bytes)) == NULL) return NULL;
        dst = lp + poff;
    }

    /* Store the entry. */
    // 如果 newp 不为空，则将其设置为刚刚添加的元素地址 dst
    if (newp) {

        *newp = dst;
        /* In case of deletion, set 'newp' to NULL if the next element is
         * the EOF element. */
        if (!ele && dst[0] == LP_EOF) *newp = NULL;
    }

    /* 执行到这里，空间都已经处理好了，下面就可以在空间上写入数据了 */

    // 新增 ele 的情况
    if (ele) {

        // 是整数编码类型
        if (enctype == LP_ENCODING_INT) {
            // 整数的情况，复制即可
            memcpy(dst, intenc, enclen);

            // 是字符编码类型，需要将字符串数据写入到 listpack 列表项的 content 位置
            // 包括 encoding 信息的写入
        } else {
            lpEncodeString(dst, ele, size);
        }

        // 移动指针
        dst += enclen;

        // 拷贝 length，即存储元素的反向解析长度
        memcpy(dst, backlen, backlen_size);

        // 移动指针
        dst += backlen_size;
    }

    /* Update header. */
    // 更新头部信息中的元素数量
    if (where != LP_REPLACE || ele == NULL) {
        uint32_t num_elements = lpGetNumElements(lp);
        if (num_elements != LP_HDR_NUMELE_UNKNOWN) {
            if (ele)
                lpSetNumElements(lp, num_elements + 1);
            else
                lpSetNumElements(lp, num_elements - 1);
        }
    }

    lpSetTotalBytes(lp, new_listpack_bytes);

#if 0
    /* This code path is normally disabled: what it does is to force listpack
     * to return *always* a new pointer after performing some modification to
     * the listpack, even if the previous allocation was enough. This is useful
     * in order to spot bugs in code using listpacks: by doing so we can find
     * if the caller forgets to set the new pointer where the listpack reference
     * is stored, after an update. */
    unsigned char *oldlp = lp;
    lp = lp_malloc(new_listpack_bytes);
    memcpy(lp,oldlp,new_listpack_bytes);
    if (newp) {
        unsigned long offset = (*newp)-oldlp;
        *newp = lp + offset;
    }
    /* Make sure the old allocation contains garbage. */
    memset(oldlp,'A',new_listpack_bytes);
    lp_free(oldlp);
#endif

    return lp;
}

/* Append the specified element 'ele' of length 'len' at the end of the
 * listpack.
 *
 * 将指定元素 ele 的长度 size 添加到列表的末端
 *
 * It is implemented in terms of lpInsert(), so the return value is
 * the same as lpInsert().
 *
 * 底层是用 lpInsert() 实现的，因此返回值与lpInsert()相同。
 *
 */
unsigned char *lpAppend(unsigned char *lp, unsigned char *ele, uint32_t size) {
    // 1 获取 listpack 的总字节数
    uint64_t listpack_bytes = lpGetTotalBytes(lp);

    // 2 获取指向 listpack 尾端的指针
    unsigned char *eofptr = lp + listpack_bytes - 1;

    // 3 在 listpack 尾端之前插入元素
    return lpInsert(lp, ele, size, eofptr, LP_BEFORE, NULL);
}

/* Remove the element pointed by 'p', and return the resulting listpack.
 * If 'newp' is not NULL, the next element pointer (to the right of the
 * deleted one) is returned by reference. If the deleted element was the
 * last one, '*newp' is set to NULL.
 *
 * 删除 p 指向的元素
 */
unsigned char *lpDelete(unsigned char *lp, unsigned char *p, unsigned char **newp) {
    return lpInsert(lp, NULL, 0, p, LP_REPLACE, newp);
}

/* Return the total number of bytes the listpack is composed of.
 *
 * 返回 listpack 总字节数
 */
uint32_t lpBytes(unsigned char *lp) {
    return lpGetTotalBytes(lp);
}

/* Seek the specified element and returns the pointer to the seeked element.
 * Positive indexes specify the zero-based element to seek from the head to
 * the tail, negative indexes specify elements starting from the tail, where
 * -1 means the last element, -2 the penultimate and so forth. If the index
 * is out of range, NULL is returned. */
unsigned char *lpSeek(unsigned char *lp, long index) {
    int forward = 1; /* Seek forward by default. */

    /* We want to seek from left to right or the other way around
     * depending on the listpack length and the element position.
     * However if the listpack length cannot be obtained in constant time,
     * we always seek from left to right. */

    // 得到总元素数
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN) {
        // 把索引先变成正数
        if (index < 0) index = (long) numele + index;
        if (index < 0) return NULL; /* Index still < 0 means out of range. */
        if (index >= (long) numele) return NULL; /* Out of range the other side. */
        /* We want to scan right-to-left if the element we are looking for
         * is past the half of the listpack. */

        // 找个最近开始遍历
        if (index > (long) numele / 2) {
            forward = 0;
            /* Right to left scanning always expects a negative index. Convert
             * our index to negative form. */
            index -= numele;
        }
    } else {
        /* If the listpack length is unspecified, for negative indexes we
         * want to always scan right-to-left. */
        if (index < 0) forward = 0;
    }

    /* Forward and backward scanning is trivially based on lpNext()/lpPrev(). */
    if (forward) {
        unsigned char *ele = lpFirst(lp);
        while (index > 0 && ele) {
            ele = lpNext(lp, ele);
            index--;
        }
        return ele;
    } else {
        unsigned char *ele = lpLast(lp);
        while (index < -1 && ele) {
            ele = lpPrev(lp, ele);
            index++;
        }
        return ele;
    }
}

/* Validate the integrity of a single listpack entry and move to the next one.
 * The input argument 'pp' is a reference to the current record and is advanced on exit.
 * Returns 1 if valid, 0 if invalid. */
int lpValidateNext(unsigned char *lp, unsigned char **pp, size_t lpbytes) {
#define OUT_OF_RANGE(p) ( \
        (p) < lp + LP_HDR_SIZE || \
        (p) > lp + lpbytes - 1)
    unsigned char *p = *pp;
    if (!p)
        return 0;

    if (*p == LP_EOF) {
        *pp = NULL;
        return 1;
    }

    /* check that we can read the encoded size */
    uint32_t lenbytes = lpCurrentEncodedSizeBytes(p);
    if (!lenbytes)
        return 0;

    /* make sure the encoded entry length doesn't rech outside the edge of the listpack */
    if (OUT_OF_RANGE(p + lenbytes))
        return 0;

    /* get the entry length and encoded backlen. */
    unsigned long entrylen = lpCurrentEncodedSizeUnsafe(p);
    unsigned long encodedBacklen = lpEncodeBacklen(NULL, entrylen);
    entrylen += encodedBacklen;

    /* make sure the entry doesn't rech outside the edge of the listpack */
    if (OUT_OF_RANGE(p + entrylen))
        return 0;

    /* move to the next entry */
    p += entrylen;

    /* make sure the encoded length at the end patches the one at the beginning. */
    uint64_t prevlen = lpDecodeBacklen(p - 1);
    if (prevlen + encodedBacklen != entrylen)
        return 0;

    *pp = p;
    return 1;
#undef OUT_OF_RANGE
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
int lpValidateIntegrity(unsigned char *lp, size_t size, int deep) {
    /* Check that we can actually read the header. (and EOF) */
    if (size < LP_HDR_SIZE + 1)
        return 0;

    /* Check that the encoded size in the header must match the allocated size. */
    size_t bytes = lpGetTotalBytes(lp);
    if (bytes != size)
        return 0;

    /* The last byte must be the terminator. */
    if (lp[size - 1] != LP_EOF)
        return 0;

    if (!deep)
        return 1;

    /* Validate the invividual entries. */
    uint32_t count = 0;
    unsigned char *p = lpFirst(lp);
    while (p) {
        if (!lpValidateNext(lp, &p, bytes))
            return 0;
        count++;
    }

    /* Check that the count in the header is correct */
    uint32_t numele = lpGetNumElements(lp);
    if (numele != LP_HDR_NUMELE_UNKNOWN && numele != count)
        return 0;

    return 1;
}
