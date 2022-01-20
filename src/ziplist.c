/* The ziplist is a specially encoded dually linked list that is designed
 * to be very memory efficient.
 *
 * ziplist 是为了尽可能地节约内存而设计的特殊编码双端链表。
 *
 * It stores both strings and integer values,
 * where integers are encoded as actual integers instead of a series of
 * characters.
 *
 * ziplist 可以存储字符串和整数值。其中，整数值被保存为实际的整数，而不是字符数组。
 *
 * It allows push and pop operations on either side of the list
 * in O(1) time. However, because every operation requires a reallocation of
 * the memory used by the ziplist, the actual complexity is related to the
 * amount of memory used by the ziplist.
 *
 * ziplist 允许在列表的两端进行 O(1) 复杂度的 push 和 pop 操作。
 * 但是，因为这些操作都需要对整个 ziplist 进行内存重分配，所以实际的复杂度和 ziplist 占用的内存有关
 *
 * ----------------------------------------------------------------------------
 *
 * ZIPLIST OVERALL LAYOUT  ziplist 的整体布局
 * ======================
 *
 * The general layout of the ziplist is as follows:
* 以下是 ziplist 的一般布局：
 *
 * <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 *
 * NOTE: all fields are stored in little endian, if not specified otherwise.
 *
 * <uint32_t zlbytes> is an unsigned integer to hold the number of bytes that
 * the ziplist occupies, including the four bytes of the zlbytes field itself.
 * This value needs to be stored to be able to resize the entire structure
 * without the need to traverse it first.
 *
 * <zlbytes> 是一个无符号整数，保存着 ziplist 使用的内存大小。通过这个值，程序可以直接对 ziplist 的内存大小进行调整，
 * 而无须为了计算 ziplist 的内存大小而遍历整个列表
 *
 *
 * <uint32_t zltail> is the offset to the last entry in the list. This allows
 * a pop operation on the far side of the list without the need for full
 * traversal.
 *
 * <zltail> 保存着到压缩列表中最后一个节点的偏移量。
 * 这个偏移量使得对表尾的 pop 操作可以在无须遍历整个列表的情况下进行
 *
 * <uint16_t zllen> is the number of entries. When there are more than
 * 2^16-2 entries, this value is set to 2^16-1 and we need to traverse the
 * entire list to know how many items it holds.
 *
 * <zllen> 保存着压缩列表中的节点数量。当 zllen 保存的值大于 2^16-2 时，程序需要遍历整个列表才能知道列表实际包含了多少个节点
 *
 * <uint8_t zlend> is a special entry representing the end of the ziplist.
 * Is encoded as a single byte equal to 255. No other normal entry starts
 * with a byte set to the value of 255.
 *
 * <zlend> 的长度为 1 字节，值为 255 ，标识列表的末尾
 *
 * ZIPLIST ENTRIES ziplist 节点
 * ===============
 *
 * Every entry in the ziplist is prefixed by metadata that contains two pieces
 * of information. First, the length of the previous entry is stored to be
 * able to traverse the list from back to front. Second, the entry encoding is
 * provided. It represents the entry type, integer or string, and in the case
 * of strings it also represents the length of the string payload.
 * So a complete entry is stored like this:
 *
 * <prevlen> <encoding> <entry-data>
 *
 * Sometimes the encoding represents the entry itself, like for small integers
 * as we'll see later. In such a case the <entry-data> part is missing, and we
 * could have just:
 *
 * <prevlen> <encoding>
 *
 *
 * 每个 ziplist 节点的前面都带有一个 header ，这个 header 包含两部分信息：
 * 1）前置节点的长度，在程序从后向前遍历时使用。
 * 2）当前节点所保存的数据的类型和长度
 *
 *
 *
 * The length of the previous entry, <prevlen>, is encoded in the following way:
 * If this length is smaller than 254 bytes, it will only consume a single
 * byte representing the length as an unsinged 8 bit integer. When the length
 * is greater than or equal to 254, it will consume 5 bytes. The first byte is
 * set to 254 (FE) to indicate a larger value is following. The remaining 4
 * bytes take the length of the previous entry as value.
 *
 * 前置节点的长度（prevlen）规则：
 * 1）如果前置节点的长度小于 254 字节，那么程序将使用 1 个字节来保存这个长度值。
 * 2）如果前置节点的长度大于等于 254 字节，那么程序将使用 5 个字节来保存这个长度值：
 *    a) 第 1 个字节的值被设置为 254 ，标识这是一个 5 字节长的长度。
 *    b) 后 4 个字节则用于保存前置节点的实际长度。
 *
 *
 * So practically an entry is encoded in the following way:
 * <prevlen from 0 to 253> <encoding> <entry>
 * Or alternatively if the previous entry length is greater than 253 bytes
 * the following encoding is used:
 * 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 *
 * The encoding field of the entry depends on the content of the
 * entry. When the entry is a string, the first 2 bits of the encoding first
 * byte will hold the type of encoding used to store the length of the string,
 * followed by the actual length of the string. When the entry is an integer
 * the first 2 bits are both set to 1. The following 2 bits are used to specify
 * what kind of integer will be stored after this header. An overview of the
 * different types and encodings is as follows. The first byte is always enough
 * to determine the kind of entry.
 *
 * header 另一部分的内容和节点所保存的数据类型和长度（encoding）有关：
 * 1）如果节点保存的是字符串，那么 encoding 的前 2 位将保存编码字符串长度所使用的类型，之后跟着的则是字符串的实际长度
 * 说明：所谓的编码技术，就是指用不同数量的字节来表示保存的信息
 *
 * |00pppppp| - 1 byte 编码长度
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      "pppppp" represents the unsigned 6 bit length.
 *      字符串的长度小于或等于 63 字节
 *
 * |01pppppp|qqqqqqqq| - 2 bytes 编码长度
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      IMPORTANT: The 14 bit number is stored in big endian.
 *       字符串的长度小于或等于 16383 字节。
 *
 * |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes 编码长度
 *      String value with length greater than or equal to 16384 bytes.
 *      Only the 4 bytes following the first byte represents the length
 *      up to 2^32-1. The 6 lower bits of the first byte are not used and
 *      are set to zero.
 *       字符串的长度大于或等于 16384 字节。
 *
 * 2）如果节点保存的是整数值，那么 encoding 的前 2 位都被设置为 1，之后跟着的则是用于标识节点所保存的整数的类型。
 *
 *
 *
 *      IMPORTANT: The 32 bit number is stored in big endian.
 * |11000000| - 3 bytes
 *      Integer encoded as int16_t (2 bytes).
 *      节点的值为 int16_t 类型的整数，长度为 2 字节
 * |11010000| - 5 bytes
 *      Integer encoded as int32_t (4 bytes).
 *      节点的值为 int32_t 类型的整数，长度为 4 字节
 * |11100000| - 9 bytes
 *      Integer encoded as int64_t (8 bytes).
 *      节点的值为 int64_t 类型的整数，长度为 8 字节
 * |11110000| - 4 bytes
 *      Integer encoded as 24 bit signed (3 bytes).
 *      节点的值为 24 位（3 字节）长的整数。
 * |11111110| - 2 bytes
 *      Integer encoded as 8 bit signed (1 byte).
 *      节点的值为 8 位（1 字节）长的整数。
 * |1111xxxx| - (with xxxx between 0001 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 *      节点的值为介于 0 至 12 之间的无符号整数。
 *      因为 0000 和 1111 都不能使用，所以位的实际值将是 1 至 13 。
 *      程序在取得这 4 个位的值之后，还需要减去 1 ，才能计算出正确的值。
 *      比如说，如果位的值为 0001 = 1 ，那么程序返回的值将是 1 - 1 = 0 。
 *
 * |11111111| - End of ziplist special entry.
 *      ziplist 的结尾标识
 *
 * Like for the ziplist header, all the integers are represented in little
 * endian byte order, even when this code is compiled in big endian systems.
 *
 * EXAMPLES OF ACTUAL ZIPLISTS
 * ===========================
 *
 * The following is a ziplist containing the two elements representing
 * the strings "2" and "5". It is composed of 15 bytes, that we visually
 * split into sections:
 *
 *  [0f 00 00 00] [0c 00 00 00] [02 00] [00 f3] [02 f6] [ff]
 *        |             |          |       |       |     |
 *     zlbytes        zltail    entries   "2"     "5"   end
 *
 * The first 4 bytes represent the number 15, that is the number of bytes
 * the whole ziplist is composed of. The second 4 bytes are the offset
 * at which the last ziplist entry is found, that is 12, in fact the
 * last entry, that is "5", is at offset 12 inside the ziplist.
 * The next 16 bit integer represents the number of elements inside the
 * ziplist, its value is 2 since there are just two elements inside.
 * Finally "00 f3" is the first entry representing the number 2. It is
 * composed of the previous entry length, which is zero because this is
 * our first entry, and the byte F3 which corresponds to the encoding
 * |1111xxxx| with xxxx between 0001 and 1101. We need to remove the "F"
 * higher order bits 1111, and subtract 1 from the "3", so the entry value
 * is "2". The next entry has a prevlen of 02, since the first entry is
 * composed of exactly two bytes. The entry itself, F6, is encoded exactly
 * like the first entry, and 6-1 = 5, so the value of the entry is 5.
 * Finally the special entry FF signals the end of the ziplist.
 *
 * Adding another element to the above string with the value "Hello World"
 * allows us to show how the ziplist encodes small strings. We'll just show
 * the hex dump of the entry itself. Imagine the bytes as following the
 * entry that stores "5" in the ziplist above:
 *
 * [02] [0b] [48 65 6c 6c 6f 20 57 6f 72 6c 64]
 *
 * The first byte, 02, is the length of the previous entry. The next
 * byte represents the encoding in the pattern |00pppppp| that means
 * that the entry is a string of length <pppppp>, so 0B means that
 * an 11 bytes string follows. From the third byte (48) to the last (64)
 * there are just the ASCII characters for "Hello World".
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2017, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "config.h"
#include "endianconv.h"
#include "redisassert.h"

/*
 * ziplist的列表尾字节内容
 */
#define ZIP_END 255         /* Special "end of ziplist" entry. */

/*
 * 使用 5 字节编码长度的标识符
 *
 * todo 取值 1 字节时，表示上一个 entry 的长度小于 254 字节。虽然 1 字节的值能表示的数值范围是 0 到 255，
 * 但是压缩列表中 zlend 的取值默认是 255，因此，就默认用 255 表示整个压缩列表的结束，其他表示长度的地方就不
 * 能再用 255 这个值了。所以，当上一个 entry 长度小于 254 字节时，prev_len 取值为 1 字节，否则，就取值为 5 字节。
 */
#define ZIP_BIG_PREVLEN 254 /* ZIP_BIG_PREVLEN - 1 is the max number of bytes of
                               the previous entry, for the "prevlen" field prefixing
                               each entry, to be represented with just a single byte.
                               Otherwise it is represented as FE AA BB CC DD, where
                               AA BB CC DD are a 4 bytes unsigned integer
                               representing the previous entry len. */


/* Different encoding/length possibilities */
/*
 * 字符串编码和整数编码的掩码
 */
#define ZIP_STR_MASK 0xc0
#define ZIP_INT_MASK 0x30

/*
 * 字符串编码类型，即保存字符串使用的长度
 */
#define ZIP_STR_06B (0 << 6) // 长度小于等于 63（2^6 –1）字节
#define ZIP_STR_14B (1 << 6) // 长度小于等于 16383(2^14 -1)字节
#define ZIP_STR_32B (2 << 6) // 长度小于等于 4294967295(2^32 -1)字节

/*
 * 整数编码类型，即保存整数使用的长度
 */
#define ZIP_INT_16B (0xc0 | 0<<4)
#define ZIP_INT_32B (0xc0 | 1<<4)
#define ZIP_INT_64B (0xc0 | 2<<4)
#define ZIP_INT_24B (0xc0 | 3<<4)
#define ZIP_INT_8B 0xfe // 11111110

/* 4 bit integer immediate encoding |1111xxxx| with xxxx between
 * 0001 and 1101.
 *
 * 4 位整数编码的掩码和类型
 */
#define ZIP_INT_IMM_MASK 0x0f   /* Mask to extract the 4 bits value. To add one is needed to reconstruct the value. */
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */

/*
 * 24 位整数的最大值和最小值
 */
#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

/* Macro to determine if the entry is a string. String entries never start
 * with "11" as most significant bits of the first byte.
 *
 * 查看给定编码 encoding 是否字符串编码，
 * 注意，针对字符串的编码不以 11 作为第一个字节的最有效位开始
 */
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)



/*--------------------- Utility macros. ziplist 属性宏 --------------*/

/* Return total bytes a ziplist is composed of.
 * 记录整个 ziplist 所占用的内存字节数
 */
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))

/* Return the offset of the last item inside the ziplist.
 * 记录到尾节点的偏移量
 */
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))

/* Return the length of a ziplist, or UINT16_MAX if the length cannot be
 * determined without scanning the whole ziplist.
 * 记录 ziplist 包含的节点数量
 */
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))

/* The size of a ziplist header: two 32 bit integers for the total
 * bytes count and last item offset. One 16 bit integer for the number
 * of items field.
 * ziplist的列表头大小，包括2个32 bits整数和1个16bits整数，分别表示:
 * - 压缩列表的总字节数
 * - 列表最后一个元素离列表头的偏移
 * - 列表中的元素个数
 */
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))

/* Size of the "end of ziplist" entry. Just one byte.
 * ziplist的列表尾大小，包括1个8 bits整数，表示列表结束。
 */
#define ZIPLIST_END_SIZE        (sizeof(uint8_t))

/* Return the pointer to the first entry of a ziplist.
 *  指向 ziplist 第一个节点（的起始位置）的指针
 *
 *  即 ziplist 起始地址 + ziplist 头大小
 */
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)

/* Return the pointer to the last entry of a ziplist, using the
 * last entry offset inside the ziplist header.
 * 指向 ziplist 最后一个节点（的起始位置）的指针
 *
 * 即 ziplist 起始地址 + zltail 记录到尾节点的偏移量
 */
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))

/* Return the pointer to the last byte of a ziplist, which is, the
 * end of ziplist FF entry.
 * 指向 ziplist 末端 ZIP_END（的起始位置）的指针
 *
 * 即 ziplist 起始地址 + ziplist 大小 -1(这个 1 字节表示 zlend 占用的大小)
 */
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)


/*
空白 ziplist 示例图

area        |<---- ziplist header ---->|<-- end -->|

size          4 bytes   4 bytes 2 bytes  1 byte
            +---------+--------+-------+-----------+
component   | zlbytes | zltail | zllen | zlend     |
            |         |        |       |           |
value       |  1011   |  1010  |   0   | 1111 1111 |
            +---------+--------+-------+-----------+
                                       ^
                                       |
                               ZIPLIST_ENTRY_HEAD
                                       &
address                        ZIPLIST_ENTRY_TAIL
                                       &
                               ZIPLIST_ENTRY_END

非空 ziplist 示例图

area        |<---- ziplist header ---->|<----------- entries ------------->|<-end->|

size          4 bytes  4 bytes  2 bytes    ?        ?        ?        ?     1 byte
            +---------+--------+-------+--------+--------+--------+--------+-------+
component   | zlbytes | zltail | zllen | entry1 | entry2 |  ...   | entryN | zlend |
            +---------+--------+-------+--------+--------+--------+--------+-------+
                                       ^                          ^        ^
address                                |                          |        |
                                ZIPLIST_ENTRY_HEAD                |   ZIPLIST_ENTRY_END
                                                                  |
                                                        ZIPLIST_ENTRY_TAIL
*/




/*--------------------- Utility macros. ziplist 属性宏 --------------*/


/* Increment the number of items field in the ziplist header. Note that this
 * macro should never overflow the unsigned 16 bit integer, since entries are
 * always pushed one at a time. When UINT16_MAX is reached we want the count
 * to stay there to signal that a full scan is needed to get the number of
 * items inside the ziplist.
 *
 * 增加 ziplist 的节点数
 */
#define ZIPLIST_INCR_LENGTH(zl, incr) { \
    if (ZIPLIST_LENGTH(zl) < UINT16_MAX) \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr); \
}

/* We use this function to receive information about a ziplist entry.
 * Note that this is not how the data is actually encoded, is just what we
 * get filled by a function in order to operate more easily.
 *
 * ziplist 列表项
 */
typedef struct zlentry {


    /*
     * 在 ziplist 中每个列表项都会记录前一项的长度，也就是 prevlen，然后，为了节省内存开销，
     * ziplist 会使用不同的空间记录 prevlen ，这个 prevlen 空间大小就是 prevlensize。
     *
     */

    // 编码 prevrawlen 所需要的字节大小
    // 即使用多少个字节存储前置项长度 todo 这个是存储的属性
    unsigned int prevrawlensize; /* Bytes used to encode the previous entry len*/

    // 前置项长度
    unsigned int prevrawlen;     /* Previous entry len. */




    // 编码 len 所需的字节大小
    // 即使用多少字节存储节点数据长度 todo 这个是存储的属性
    unsigned int lensize;        /* Bytes used to encode this entry type/len.
                                    For example strings have a 1, 2 or 5 bytes
                                    header. Integers always use a single byte.*/

    // 当前项的长度（数据长度）
    unsigned int len;            /* Bytes used to represent the actual entry.
                                    For strings this is just the string length
                                    while for integers it is 1, 2, 3, 4, 8 or
                                    0 (for 4 bit immediate) depending on the
                                    number range. */

    // 当前项 header 的大小，等于 prevrawlensize + lensize
    unsigned int headersize;     /* prevrawlensize + lensize. */

    // 当前项所使用的编码类型
    unsigned char encoding;      /* Set to ZIP_STR_* or ZIP_INT_* depending on
                                    the entry encoding. However for 4 bits
                                    immediate integers this can assume a range
                                    of values and must be range-checked. */
    // 指向当前项的指针
    unsigned char *p;            /* Pointer to the very start of the entry, that
                                    is, this points to prev-entry-len field. */
} zlentry;

#define ZIPLIST_ENTRY_ZERO(zle) { \
    (zle)->prevrawlensize = (zle)->prevrawlen = 0; \
    (zle)->lensize = (zle)->len = (zle)->headersize = 0; \
    (zle)->encoding = 0; \
    (zle)->p = NULL; \
}

/* Extract the encoding from the byte pointed by 'ptr' and set it into
 * 'encoding' field of the zlentry structure.
 *
 * 从 ptr 中取出节点值的编码，并将它保存到 encoding 变量中
 */
#define ZIP_ENTRY_ENCODING(ptr, encoding) do {  \
    (encoding) = ((ptr)[0]);                    \
    /* 根据掩码计算 */                                            \
    if ((encoding) < ZIP_STR_MASK) (encoding) &= ZIP_STR_MASK; \
} while(0)

#define ZIP_ENCODING_SIZE_INVALID 0xff

/* Return the number of bytes required to encode the entry type + length.
 * On error, return ZIP_ENCODING_SIZE_INVALID
 *
 * 返回保存 encoding 编码的字符串所需要的字节数量
 */
static inline unsigned int zipEncodingLenSize(unsigned char encoding) {
    if (encoding == ZIP_INT_16B || encoding == ZIP_INT_32B ||
        encoding == ZIP_INT_24B || encoding == ZIP_INT_64B ||
        encoding == ZIP_INT_8B)
        return 1;
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
        return 1;
    if (encoding == ZIP_STR_06B)
        return 1;
    if (encoding == ZIP_STR_14B)
        return 2;
    if (encoding == ZIP_STR_32B)
        return 5;
    return ZIP_ENCODING_SIZE_INVALID;
}

#define ZIP_ASSERT_ENCODING(encoding) do {                                     \
    assert(zipEncodingLenSize(encoding) != ZIP_ENCODING_SIZE_INVALID);         \
} while (0)

/* Return bytes needed to store integer encoded by 'encoding'
 *
 * 返回保存 encoding 编码的整数值所需要的字节数量
 *
 */
static inline unsigned int zipIntSize(unsigned char encoding) {
    switch (encoding) {
        case ZIP_INT_8B:
            return 1;
        case ZIP_INT_16B:
            return 2;
        case ZIP_INT_24B:
            return 3;
        case ZIP_INT_32B:
            return 4;
        case ZIP_INT_64B:
            return 8;
    }

    // 不足一字节 ，0
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
        return 0; /* 4 bit immediate */
    /* bad encoding, covered by a previous call to ZIP_ASSERT_ENCODING */
    redis_unreachable();
    return 0;
}

/* Write the encoding header of the entry in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. Arguments:
 *
 * todo 将节点的头部编码 encoding 写入 p ，如果 p 为空，那么仅返回编码数据长度所需要的字节数。
 *
 * 节点的实际数据既可以是整数也可以是字符串。整数可以是 16、32、64 等字节长度，同时字符串的长度也可以大小不一。所以，
 * 针对整数和字符串，可以分别使用不同字节长度的编码，即使用不同字节来存储实际数据大小。
 *
 *
 * 'encoding' is the encoding we are using for the entry. It could be
 * ZIP_INT_* or ZIP_STR_* or between ZIP_INT_IMM_MIN and ZIP_INT_IMM_MAX
 * for single-byte small immediate integers.
 *
 * encoding 用于节点，用于指定整数或字符串数据的长度和类型编码
 *
 * 'rawlen' is only used for ZIP_STR_* encodings and is the length of the
 * string that this entry represents.
 *
 * rawlen 仅仅用于节点存储字符串的情况，表示字符串的长度。
 *
 * The function returns the number of bytes used by the encoding/length
 * header stored in 'p'.
 *
 */
unsigned int zipStoreEntryEncoding(unsigned char *p, unsigned char encoding, unsigned int rawlen) {

    // 1 默认编码结果是 1 字节
    unsigned char len = 1, buf[5];

    // 2 如果是字符串编码
    if (ZIP_IS_STR(encoding)) {
        /* Although encoding is given it may not be set for strings,
         * so we determine it here using the raw length. */
        // 2.1 字符串长度小于等于63字节（16进制为0x3f），则编码结果是 1 字节
        if (rawlen <= 0x3f) {
            if (!p) return len;
            buf[0] = ZIP_STR_06B | rawlen;

            // 2.2 字符串长度小于等于 16383 （16进制为0x3fff），则编码结果是 2 字节
        } else if (rawlen <= 0x3fff) {
            // 编码结果是 2 字节
            len += 1;
            if (!p) return len;
            buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
            buf[1] = rawlen & 0xff;

            // 2.3 字符串长度大于 16383字节，编码结果是 5 字节
        } else {
            // 编码结果是 5 字节
            len += 4;
            if (!p) return len;
            buf[0] = ZIP_STR_32B;
            buf[1] = (rawlen >> 24) & 0xff;
            buf[2] = (rawlen >> 16) & 0xff;
            buf[3] = (rawlen >> 8) & 0xff;
            buf[4] = rawlen & 0xff;
        }

        // 3 编码整数，编码结果是 1 字节
    } else {
        /* Implies integer encoding, so length is always 1. */
        if (!p) return len;
        buf[0] = encoding;
    }

    /* Store this length at p. */
    // 4 将编码后的长度写入 p
    memcpy(p, buf, len);

    // 5 返回编码所需的字节数
    return len;
}

/* Decode the entry encoding type and data length (string length for strings,
 * number of bytes used for the integer for integer entries) encoded in 'ptr'.
 * The 'encoding' variable is input, extracted by the caller, the 'lensize'
 * variable will hold the number of bytes required to encode the entry
 * length, and the 'len' variable will hold the entry length.
 * On invalid encoding error, lensize is set to 0.
 *
 * 解码 ptr 指针，取出节点的相关信息，并将它们保存在以下变量中：
 *  - lensize 保存编码节点长度所需的字节数
 *  - len 保存节点的长度。
 *
 */
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len) do {                    \
    /* 字符串编码 */                                                             \
    if ((encoding) < ZIP_STR_MASK) {                                           \
                                                                              \
        if ((encoding) == ZIP_STR_06B) {                                       \
            (lensize) = 1;                                                     \
            (len) = (ptr)[0] & 0x3f;                                           \
        } else if ((encoding) == ZIP_STR_14B) {                                \
            (lensize) = 2;                                                     \
            (len) = (((ptr)[0] & 0x3f) << 8) | (ptr)[1];                       \
        } else if ((encoding) == ZIP_STR_32B) {                                \
            (lensize) = 5;                                                     \
            (len) = ((ptr)[1] << 24) |                                         \
                    ((ptr)[2] << 16) |                                         \
                    ((ptr)[3] <<  8) |                                         \
                    ((ptr)[4]);                                                \
        } else {                                                               \
            (lensize) = 0; /* bad encoding, should be covered by a previous */ \
            (len) = 0;     /* ZIP_ASSERT_ENCODING / zipEncodingLenSize, or  */ \
                           /* match the lensize after this macro with 0.    */ \
        }                                                                      \
      /* 整数编码 */                                                             \
    } else {                                                                   \
        (lensize) = 1;                                                         \
        if ((encoding) == ZIP_INT_8B)  (len) = 1;                              \
        else if ((encoding) == ZIP_INT_16B) (len) = 2;                         \
        else if ((encoding) == ZIP_INT_24B) (len) = 3;                         \
        else if ((encoding) == ZIP_INT_32B) (len) = 4;                         \
        else if ((encoding) == ZIP_INT_64B) (len) = 8;                         \
        else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)   \
            (len) = 0; /* 4 bit immediate */                                   \
        else                                                                   \
            (lensize) = (len) = 0; /* bad encoding */                          \
    }                                                                          \
} while(0)


/* Encode the length of the previous entry and write it to "p". This only
 * uses the larger encoding (required in __ziplistCascadeUpdate).
 *
 * todo 编码前置节点的长度，该方法处理较大长度(>= 254)。包含两部分内容：
 * 1）将 5 字节编码标识别写入到 p 中
 * 2）使用后 4 个字节存储长度 len
 */
int zipStorePrevEntryLengthLarge(unsigned char *p, unsigned int len) {
    uint32_t u32;

    // 添加 5 字节长度标识
    if (p != NULL) {

        // 1 将记录前置节点的长度的 prevlen 的第 1 字节设置为  ZIP_BIG_PREVLEN，即 254
        p[0] = ZIP_BIG_PREVLEN;
        u32 = len;

        // 2 将前一个列表项的长度值拷贝至 prevlen 的第2至第5字节，其中sizeof(len)的值为4
        memcpy(p + 1, &u32, sizeof(u32));

        // 3 如果有必要的话进行大小端的转换
        memrev32ifbe(p + 1);
    }

    // 4 返回编码 prevlen 所用字节数，也就是 5
    return 1 + sizeof(uint32_t);
}

/* Encode the length of the previous entry and write it to "p". Return the
 * number of bytes needed to encode this length if "p" is NULL.
 *
 * todo 对前置节点的长度 len 进行编码，并将它写入到 p(节点) 中，然后返回编码 len 所需的字节数量。
 *
 * 如果 p 为 NULL ，那么不进行写入，仅返回编码 len 所需的字节数量
 */
unsigned int zipStorePrevEntryLength(unsigned char *p, unsigned int len) {

    // 1 如果 p 为 NULL，直接返回编码 len 所需的字节数量（存储 len 使用的字节数）
    if (p == NULL) {
        // 前置节点的长度 < 254，则使用 1 字节存储该长度；
        // 前置节点的长度 >= 254 ，则使用 4 + 1 个字节存储该长度
        return (len < ZIP_BIG_PREVLEN) ? 1 : sizeof(uint32_t) + 1;

        // 2 写入 len 并返回编码 len 所需的字节数量
    } else {

        // 2.1 判断前置节点的长度是否小于 ZIP_BIG_PREVLEN 254
        if (len < ZIP_BIG_PREVLEN) {

            // 写入 len
            p[0] = len;

            // 返回编码 len 使用的字节数
            return 1;

            // 2.2 前置节点长度 >= 254，则调用 zipStorePrevEntryLengthLarge 进行编码
        } else {
            return zipStorePrevEntryLengthLarge(p, len);
        }
    }
}

/* Return the number of bytes used to encode the length of the previous
 * entry. The length is returned by setting the var 'prevlensize'.
 *
 * 解码 ptr 指针
 *
 * 取出编码前置节点长度所需的字节数，并将其保存到 prevlensize 变量中。判断长度的第一个字节保存的是否为 254
 */
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do { \
    /* 判断字节指针的第一个字节的值是否 < 254 (使用 5 字节编码长度的标识符)  */           \
    if ((ptr)[0] < ZIP_BIG_PREVLEN) {                                          \
        (prevlensize) = 1;                                                     \
    } else {                                                                   \
        (prevlensize) = 5;                                                     \
    }                                                                          \
} while(0)

/* Return the length of the previous element, and the number of bytes that
 * are used in order to encode the previous element length.
 * 'ptr' must point to the prevlen prefix of an entry (that encodes the
 * length of the previous entry in order to navigate the elements backward).
 * The length of the previous entry is stored in 'prevlen', the number of
 * bytes needed to encode the previous entry length are stored in
 * 'prevlensize'.
 *
 * 1 解码 ptr 指针，取出编码前置节点长度所需的字节数，并将这个字节数保存到 prevlensize 中。
 * 2 根据编码前置节点长度所需字节数 prevlensize ，从 ptr 中取出前置节点的长度值，并将这个长度值保存到 prevlen 中
 *
 */
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do { \
    /* 取出编码前置节点长度所需的字节数，并设置到 prevlensize   */                     \
    ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);                                  \
                                                                               \
    /* 如果使用 1 字节编码前置节点长度，则直接读取该字节的值，并作为前置节点的长度  */       \
    if ((prevlensize) == 1) {                                                  \
        (prevlen) = (ptr)[0];                                                  \
                                                                               \
    /* 如果使用 5 字节编码前置节点长度，则取出后 4 字节的值，并作为前置节点的长度 */        \
    } else { /* prevlensize == 5 */                                            \
        (prevlen) = ((ptr)[4] << 24) |                                         \
                    ((ptr)[3] << 16) |                                         \
                    ((ptr)[2] <<  8) |                                         \
                    ((ptr)[1]);                                                \
    }                                                                          \
} while(0)

/* Given a pointer 'p' to the prevlen info that prefixes an entry, this
 * function returns the difference in number of bytes needed to encode
 * the prevlen if the previous entry changes of size.
 *
 * So if A is the number of bytes used right now to encode the 'prevlen'
 * field.
 *
 * And B is the number of bytes that are needed in order to encode the
 * 'prevlen' if the previous element will be updated to one of size 'len'.
 *
 * Then the function returns B - A
 *
 * So the function returns a positive number if more space is needed,
 * a negative number if less space is needed, or zero if the same space
 * is needed.
 */
/**
 * 计算编码新的前置节点长度 len 所需的字节数，然后减去编码 p 原来的前置节点长度所需的字节数之差
 *
 * @param p 新节点
 * @param len
 * @return
 */
int zipPrevLenByteDiff(unsigned char *p, unsigned int len) {
    unsigned int prevlensize;

    // 1 取出编码节点p的前置节点长度所需的字节数
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);

    // 计算编码 len 所需的字节数，然后进行减法运算
    return zipStorePrevEntryLength(NULL, len) - prevlensize;
}

/* Check if string pointed to by 'entry' can be encoded as an integer.
 *
 * 检查 entry 中指向的字符串能否被转为整数
 *
 * Stores the integer value in 'v' and its encoding in 'encoding'.
 *
 * 如果可以的话，将转换后的整数保存在指针 v 的值中，并将编码的方式保存在指针 encoding 的值中。
 *
 * 注意，这里的 entry 和前面代表节点的 entry 不是一个意思。
 *
 */
int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding) {
    long long value;

    // 忽略太长或太短的字符串
    // ziplist 能支持的最大整数是多大？ 根据这里可以看出，传入的int值大小不会超过32位，那么最大值应该就是int32的最大值
    if (entrylen >= 32 || entrylen == 0) return 0;

    // 尝试将 string 转成  long long，如果可以转换则将转换后的整数保存在 value 中
    if (string2ll((char *) entry, entrylen, &value)) {
        /* Great, the string can be encoded. Check what's the smallest
         * of our encoding types that can hold this value. */

        // 转换成功，以从小到大的顺序检查适合值 value 的编码方式
        if (value >= 0 && value <= 12) { // 1111 1100
            *encoding = ZIP_INT_IMM_MIN + value; // 1111 0001  -> 1111 1101
        } else if (value >= INT8_MIN && value <= INT8_MAX) {
            *encoding = ZIP_INT_8B; // 1111 1110
        } else if (value >= INT16_MIN && value <= INT16_MAX) {
            *encoding = ZIP_INT_16B;
        } else if (value >= INT24_MIN && value <= INT24_MAX) {
            *encoding = ZIP_INT_24B;
        } else if (value >= INT32_MIN && value <= INT32_MAX) {
            *encoding = ZIP_INT_32B;
        } else {
            *encoding = ZIP_INT_64B;
        }

        // 记录值到指针
        *v = value;

        // 返回转换成功标识
        return 1;
    }

    // 转换失败
    return 0;
}

/* Store integer 'value' at 'p', encoded as 'encoding'
 *
 *  以 encoding 指定的编码方式，将整数值 value 写入到 p 。
 */
void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64;
    if (encoding == ZIP_INT_8B) {
        ((int8_t *) p)[0] = (int8_t) value;
    } else if (encoding == ZIP_INT_16B) {
        i16 = value;
        memcpy(p, &i16, sizeof(i16));
        memrev16ifbe(p);
    } else if (encoding == ZIP_INT_24B) {
        i32 = value << 8;
        memrev32ifbe(&i32);
        memcpy(p, ((uint8_t *) &i32) + 1, sizeof(i32) - sizeof(uint8_t));
    } else if (encoding == ZIP_INT_32B) {
        i32 = value;
        memcpy(p, &i32, sizeof(i32));
        memrev32ifbe(p);
    } else if (encoding == ZIP_INT_64B) {
        i64 = value;
        memcpy(p, &i64, sizeof(i64));
        memrev64ifbe(p);
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        /* Nothing to do, the value is stored in the encoding itself. */
    } else {
        assert(NULL);
    }
}

/* Read integer encoded as 'encoding' from 'p'
 *
 * 以 encoding 指定的编码方式，读取并返回指针 p 中的整数值。
 * 即 知道了节点存储整数的长度，就可以从 p 中读取整数值
 */
int64_t zipLoadInteger(unsigned char *p, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;
    if (encoding == ZIP_INT_8B) {
        ret = ((int8_t *) p)[0];
    } else if (encoding == ZIP_INT_16B) {
        memcpy(&i16, p, sizeof(i16));
        memrev16ifbe(&i16);
        ret = i16;
    } else if (encoding == ZIP_INT_32B) {
        memcpy(&i32, p, sizeof(i32));
        memrev32ifbe(&i32);
        ret = i32;
    } else if (encoding == ZIP_INT_24B) {
        i32 = 0;
        memcpy(((uint8_t *) &i32) + 1, p, sizeof(i32) - sizeof(uint8_t));
        memrev32ifbe(&i32);
        ret = i32 >> 8;
    } else if (encoding == ZIP_INT_64B) {
        memcpy(&i64, p, sizeof(i64));
        memrev64ifbe(&i64);
        ret = i64;
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        ret = (encoding & ZIP_INT_IMM_MASK) - 1;
    } else {
        assert(NULL);
    }
    return ret;
}

/* Fills a struct with all information about an entry.
 * This function is the "unsafe" alternative to the one blow.
 * Generally, all function that return a pointer to an element in the ziplist
 * will assert that this element is valid, so it can be freely used.
 * Generally functions such ziplistGet assume the input pointer is already
 * validated (since it's the return value of another function).
 *
 * 将 p 所指向的列表节点的信息全部保存到 zlentry 中，并返回该 zlentry 。
 *
 */
static inline void zipEntry(unsigned char *p, zlentry *e) {

    // 从 p 所指向的列表节点中分别取出 编码前置节点长度所需的字节数 和 前置节点的长度，分别保存到 e->prevrawlensize 和 e->prevrawlen
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);

    // 从 ptr 中取出节点值的编码，并将它保存到 encoding 变量中
    ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);

    // 从 ptr 中取出 编码节点值长度所需的字节数 和 节点值长度
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);

    assert(e->lensize != 0); /* check that encoding was valid. */

    // 计算节点头的字节数
    e->headersize = e->prevrawlensize + e->lensize;

    // 记录指针
    e->p = p;
}

/* Fills a struct with all information about an entry.
 * This function is safe to use on untrusted pointers, it'll make sure not to
 * try to access memory outside the ziplist payload.
 * Returns 1 if the entry is valid, and 0 otherwise. */
static inline int zipEntrySafe(unsigned char *zl, size_t zlbytes, unsigned char *p, zlentry *e, int validate_prevlen) {
    unsigned char *zlfirst = zl + ZIPLIST_HEADER_SIZE;
    unsigned char *zllast = zl + zlbytes - ZIPLIST_END_SIZE;
#define OUT_OF_RANGE(p) (unlikely((p) < zlfirst || (p) > zllast))

    /* If threre's no possibility for the header to reach outside the ziplist,
     * take the fast path. (max lensize and prevrawlensize are both 5 bytes) */
    if (p >= zlfirst && p + 10 < zllast) {
        ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
        ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);
        ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
        e->headersize = e->prevrawlensize + e->lensize;
        e->p = p;
        /* We didn't call ZIP_ASSERT_ENCODING, so we check lensize was set to 0. */
        if (unlikely(e->lensize == 0))
            return 0;
        /* Make sure the entry doesn't rech outside the edge of the ziplist */
        if (OUT_OF_RANGE(p + e->headersize + e->len))
            return 0;
        /* Make sure prevlen doesn't rech outside the edge of the ziplist */
        if (validate_prevlen && OUT_OF_RANGE(p - e->prevrawlen))
            return 0;
        return 1;
    }

    /* Make sure the pointer doesn't rech outside the edge of the ziplist */
    if (OUT_OF_RANGE(p))
        return 0;

    /* Make sure the encoded prevlen header doesn't reach outside the allocation */
    ZIP_DECODE_PREVLENSIZE(p, e->prevrawlensize);
    if (OUT_OF_RANGE(p + e->prevrawlensize))
        return 0;

    /* Make sure encoded entry header is valid. */
    ZIP_ENTRY_ENCODING(p + e->prevrawlensize, e->encoding);
    e->lensize = zipEncodingLenSize(e->encoding);
    if (unlikely(e->lensize == ZIP_ENCODING_SIZE_INVALID))
        return 0;

    /* Make sure the encoded entry header doesn't reach outside the allocation */
    if (OUT_OF_RANGE(p + e->prevrawlensize + e->lensize))
        return 0;

    /* Decode the prevlen and entry len headers. */
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
    e->headersize = e->prevrawlensize + e->lensize;

    /* Make sure the entry doesn't rech outside the edge of the ziplist */
    if (OUT_OF_RANGE(p + e->headersize + e->len))
        return 0;

    /* Make sure prevlen doesn't rech outside the edge of the ziplist */
    if (validate_prevlen && OUT_OF_RANGE(p - e->prevrawlen))
        return 0;

    e->p = p;
    return 1;
#undef OUT_OF_RANGE
}

/* Return the total number of bytes used by the entry pointed to by 'p'.
 *
 * 返回指针 p 所指向的节点占用的字节总数
 */
static inline unsigned int zipRawEntryLengthSafe(unsigned char *zl, size_t zlbytes, unsigned char *p) {
    zlentry e;

    // zipEntrySafe ，用于将 p 所指向的列表节点的信息全部保存到 e 中
    assert(zipEntrySafe(zl, zlbytes, p, &e, 0));

    // 返回 p 指向的节点占用的字节总数
    return e.headersize + e.len;
}

/* Return the total number of bytes used by the entry pointed to by 'p'. */
static inline unsigned int zipRawEntryLength(unsigned char *p) {
    zlentry e;
    zipEntry(p, &e);
    return e.headersize + e.len;
}

/* Validate that the entry doesn't reach outside the ziplist allocation. */
static inline void zipAssertValidEntry(unsigned char *zl, size_t zlbytes, unsigned char *p) {
    zlentry e;
    assert(zipEntrySafe(zl, zlbytes, p, &e, 1));
}


/*
空白 ziplist 示例图

area        |<---- ziplist header ---->|<-- end -->|

size          4 bytes   4 bytes 2 bytes  1 byte
            +---------+--------+-------+-----------+
component   | zlbytes | zltail | zllen | zlend     |
            |         |        |       |           |
value       |  1011   |  1010  |   0   | 1111 1111 |
            +---------+--------+-------+-----------+
                                       ^
                                       |
                               ZIPLIST_ENTRY_HEAD
                                       &
address                        ZIPLIST_ENTRY_TAIL
                                       &
                               ZIPLIST_ENTRY_END
*/

/* Create a new empty ziplist.
 *
 * 创建并返回一个空的 ziplist
 *
 * 格式详见前面代码
 *
 * T = O(1)
 */
unsigned char *ziplistNew(void) {

    // 1 创建一块连续的内存空间，大小为 ZIPLIST_HEADER_SIZE+ZIPLIST_END_SIZE 总和
    // 2 * 32bits + 16bits + 8bits ，其中 8bits 是压缩列表末端的大小
    unsigned int bytes = ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE;

    // 2 为压缩列表头和表末端分配空间
    unsigned char *zl = zmalloc(bytes);

    // 3 初始化 ziplist 属性
    // 3.1 zlbytes 记录 ziplist 存储的总字节大小
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes); // zlbytes
    // 3.2 zltail 记录到尾节点的偏移量
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE); // zltail
    // 3.3 zllen 记录压缩列表中列表项多少
    ZIPLIST_LENGTH(zl) = 0; // zlen

    // 4 将列表尾设置为 ZIP_END，表示列表结束
    zl[bytes - 1] = ZIP_END;

    return zl;
}

/* Resize the ziplist.
 *
 * 调整 ziplist 的大小为 len 字节
 *
 * 注意，当 ziplist 原有的大小小于 len 时，扩展 ziplist 不会改变 ziplist 原有的元素
 *
 */
unsigned char *ziplistResize(unsigned char *zl, unsigned int len) {

    // 1 使用 zrealloc ，扩展时不会改变现有的元素
    zl = zrealloc(zl, len);

    // 更新 zlbytes 属性
    ZIPLIST_BYTES(zl) = intrev32ifbe(len);

    // 重新设置压缩列表末端
    zl[len - 1] = ZIP_END;


    return zl;
}

/* When an entry is inserted, we need to set the prevlen field of the next
 * entry to equal the length of the inserted entry. It can occur that this
 * length cannot be encoded in 1 byte and the next entry needs to be grow
 * a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
 * because this only happens when an entry is already being inserted (which
 * causes a realloc and memmove). However, encoding the prevlen may require
 * that this entry is grown as well. This effect may cascade throughout
 * the ziplist when there are consecutive entries with a size close to
 * ZIP_BIG_PREVLEN, so we need to check that the prevlen can be encoded in
 * every consecutive entry.
 *
 * 当将一个新节点添加到某个节点之前时，如果原节点的 header 空间不足以保存新节点的长度，
 * 则需要对原节点的 header 空间进行扩展（从 1 字节扩展到 5 字节）
 *
 * 但是，当对原节点进行扩展之后，原节点的下一个节点的 prevlen 也可能出现空间不足，这种情况在多个连续节点的长度都接近 ZIP_BIGLEN 时可能发生。
 *
 * 这个函数就用于检查并修复后续节点的空间问题。
 *
 * Note that this effect can also happen in reverse, where the bytes required
 * to encode the prevlen field can shrink. This effect is deliberately ignored,
 * because it can cause a "flapping" effect where a chain prevlen fields is
 * first grown and then shrunk again after consecutive inserts. Rather, the
 * field is allowed to stay larger than necessary, because a large prevlen
 * field implies the ziplist is holding large entries anyway.
 *
 * 反过来说，
 * 因为节点的长度变小而引起的连续缩小也是可能出现的，不过，为了避免扩展-缩小-扩展-缩小这样的情况反复出现（flapping，抖动），
 * 我们不处理这种情况，而是任由 prevlen 比所需的长度更长。
 *
 * The pointer "p" points to the first entry that does NOT need to be
 * updated, i.e. consecutive fields MAY need an update.
 *
 * 注意，程序的检查是针对 p 的后续节点，而不是 p 所指向的节点。因为节点 p 在传入之前已经完成了所需的空间扩展工作。
 *
 */
/**
 * 检查 p 之后的所有节点是否符合 ziplist 的编码要求，以便级联更新。
 *
 * 注意，程序的检查是针对 p 的后续节点，而不是 p 所指向的节点。因为节点 p 在传入之前已经完成了所需的空间扩展工作。
 *
 * todo 级联更新本质是对需要扩展的节点进行 前置节点长度的更新 ，这个过程涉及到两个内容：
 * - 选出需要变更的节点，一般变更要么是 1 个，要么是多个连续的
 * - 变更意味这增加 4 个字节，累加需要扩展多少个字节，然后重新为 ziplist 分配空间
 * - 对需要变动的节点执行更新前置节点长度的操作
 *
 * @param zl
 * @param p 节点
 * @return
 */
unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {
    zlentry cur;
    size_t prevlen, prevlensize, prevoffset; /* Informat of the last changed entry. */
    size_t firstentrylen; /* Used to handle insert at head. */

    // 获取整个 ziplist 所占用的内存字节数
    size_t rawlen, curlen = intrev32ifbe(ZIPLIST_BYTES(zl));
    size_t extra = 0, cnt = 0, offset;

    // 节点扩展时增加的字节数
    size_t delta = 4; /* Extra bytes needed to update a entry's prevlen (5-1). */

    // 获取 ziplist 最后一个节点
    unsigned char *tail = zl + intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl));

    /* Empty ziplist */
    if (p[0] == ZIP_END) return zl;

    // 将 p 所指向的节点的信息保存到 cur 中
    zipEntry(p,
             &cur); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */

    // 获取节点 p 的大小
    firstentrylen = prevlen = cur.headersize + cur.len;

    // 获取编码节点 p 长度所需要字节数
    prevlensize = zipStorePrevEntryLength(NULL, prevlen);

    // 临时保存节点 p 的偏移量
    prevoffset = p - zl;

    // todo 向后移动 p 指针
    p += prevlen;

    /* Iterate ziplist to find out how many extra bytes do we need to update it. */
    // 迭代ziplist，找出我们需要多少额外字节来更新它。
    while (p[0] != ZIP_END) {

        // 将 p 所指向的节点信息保存到 cur  中
        assert(zipEntrySafe(zl, curlen, p, &cur, 0));

        /* Abort when "prevlen" has not changed. */
        // 后续节点编码当前节点长度已经足够，直接跳出。
        // todo 只要某个节点的 header 空间足以保存当前节点的长度，那么这个节点之后的所有节点的空间都是足够的
        if (cur.prevrawlen == prevlen) break;

        /* Abort when entry's "prevlensize" is big enough. */
        // 如果当前遍历的节点的 header 空间的 prevrawlensize >= 前置节点的，则
        // 更新当前遍历的节点p 保存的前置节点的大小为 prevlen 即可
        if (cur.prevrawlensize >= prevlensize) {

            // 当前遍历的节点p 的 header 空间刚好可以编码前置节点大小
            if (cur.prevrawlensize == prevlensize) {
                zipStorePrevEntryLength(p, prevlen);

                // 当前遍历的节点p 的 header 空间有 5 字节，而编码 prevlen 1 字节就够了
                // todo 但是程序不会执行缩小 header 的逻辑
            } else {
                /* This would result in shrinking, which we want to avoid.
                 * So, set "prevlen" in the available bytes. */
                zipStorePrevEntryLengthLarge(p, prevlen);
            }

            // 跳出
            break;
        }

        /* 执行到这里，说明当前遍历的节点的 header 空间不足，所以需要对当前遍历的节点的 header 部分空间进行扩展*/

        /* cur.prevrawlen means cur is the former head entry. */
        assert(cur.prevrawlen == 0 || cur.prevrawlen + delta == prevlen);

        /* Update prev entry's info and advance the cursor. */
        // 当前遍历节点的大小
        rawlen = cur.headersize + cur.len;

        // 扩展后的大小
        prevlen = rawlen + delta;

        //扩展后的大小编码所需的字节数
        prevlensize = zipStorePrevEntryLength(NULL, prevlen);

        // 临时保存节点 p 的偏移量
        prevoffset = p - zl;

        // todo 向后移动 p 指针
        p += rawlen;

        // 累加额外的字节数
        extra += delta;

        cnt++;
    }

    /* Extra bytes is zero all update has been done(or no need to update). */
    // 如果额外字节数为 0 ，则无需更新
    if (extra == 0) return zl;

    /* Update tail offset after loop. */
    // 更新 zltail 的值，即更新到尾节点的偏移量

    // 如果连锁更新到了最后一个节点，减去 1 个（因为更新到了最后一个节点后，还是会进行下一轮判断）
    if (tail == zl + prevoffset) {
        /* When the the last entry we need to update is also the tail, update tail offset
         * unless this is the only entry that was updated (so the tail offset didn't change). */
        if (extra - delta != 0) {
            ZIPLIST_TAIL_OFFSET(zl) =
                    intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + extra - delta);
        }

        // 没有更新到最后一个节点
    } else {
        /* Update the tail offset in cases where the last entry we updated is not the tail. */
        ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + extra);
    }

    /* Now "p" points at the first unchanged byte in original ziplist,move data after that to new ziplist. */
    // todo 此时 p 指向 ziplist 中第一个未改变的节点。

    // 记录 p 的偏移量
    offset = p - zl;

    // todo 对压缩列表执行空间重分配，增加的大小就是级联更新的额外字节数
    // todo 注意，只执行了一次空间重分配
    zl = ziplistResize(zl, curlen + extra);

    // 还原 p
    p = zl + offset;

    // 向后移动 p 节点及之后数据，为扩展的空间腾出位置
    memmove(p + extra, p, curlen - offset - 1);

    // 定位到移动后的 p 节点
    p += extra;

    /* Iterate all entries that need to be updated tail to head. */
    // todo 迭代所有需要从头到尾更新的条目，即对那些需要扩展 header 的节点更新前置节点长度（注意，这里的长度变了，对应的编码也变了）
    while (cnt) {
        zipEntry(zl + prevoffset,
                 &cur); /* no need for "safe" variant since we already iterated on all these entries above. */
        rawlen = cur.headersize + cur.len;

        // 移动空间，为扩展的 header 腾出空间。为哪个节点呢？ p -= (rawlen + delta);
        /* Move entry to tail and reset prevlen. */
        memmove(p - (rawlen - cur.prevrawlensize),
                zl + prevoffset + cur.prevrawlensize,
                rawlen - cur.prevrawlensize);
        p -= (rawlen + delta);

        // todo 为变动节点更新前置节点长度.
        // todo 注意，在更新前已经扩展了内存（重新分配压缩列表空间 + 移动空间）
        if (cur.prevrawlen == 0) {
            /* "cur" is the previous head entry, update its prevlen with firstentrylen. */
            zipStorePrevEntryLength(p, firstentrylen);
        } else {
            /* An entry's prevlen can only increment 4 bytes. */
            zipStorePrevEntryLength(p, cur.prevrawlen + delta);
        }
        /* Foward to previous entry. */
        prevoffset -= cur.prevrawlen;
        cnt--;
    }
    return zl;
}

/* Delete "num" entries, starting at "p". Returns pointer to the ziplist.
 *
 * 从位置 p 开始，连续删除 num 个节点
 *
 * 函数返回值为处理删除操作后的 ziplist
 */
unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num) {
    unsigned int i, totlen, deleted = 0;
    size_t offset;
    int nextdiff = 0;
    zlentry first, tail;

    // 1 获取整个 zl 所占用的内存字节数
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));

    // 将 p 所指向的列表节点的信息全部保存到 first
    zipEntry(p,
             &first); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */

    // 2 计算被删除节点总共占用的内存字节数，以及被删除节点的总个数
    for (i = 0; p[0] != ZIP_END && i < num; i++) {

        // 返回指针 p 所指向的节点占用的字节总数
        // todo 向后移动 p 指针，步长为节点的长度
        p += zipRawEntryLengthSafe(zl, zlbytes, p);

        // 统计删除个数
        deleted++;
    }

    /* 执行到这里，p 是被删除范围之后的第一个节点，或到达了列表尾端  */
    assert(p >= first.p);

    // todo totlen 是所有被删除节点总共占用的内存字节数
    totlen = p - first.p; /* Bytes taken by the element(s) to delete. */

    if (totlen > 0) {
        uint32_t set_tail;
        if (p[0] != ZIP_END) {

            // 执行到这里，表示被删除节点之后仍然有节点存在

            /* Storing `prevrawlen` in this entry may increase or decrease the
             * number of bytes required compare to the current `prevrawlen`.
             * There always is room to store this, because it was previously
             * stored by an entry that is now being deleted. */
            // 因为位于被删除范围之后的第一个节点的 header 部分可能容纳不了新的前置节点，
            // 所以需要计算新旧前置节点之间的字节数差：
            // - 如果 nextdiff <= 0 ，说明被删除范围之后的第一个节点的 header 部分可容纳下新的前置节点
            // - 如果 nextdiff > 0 ，说明被删除范围之后的第一个节点的 header 部分不可容纳下新的前置节点
            nextdiff = zipPrevLenByteDiff(p, first.prevrawlen);

            /* Note that there is always space when p jumps backward: if
             * the new previous entry is large, one of the deleted elements
             * had a 5 bytes prevlen header, so there is for sure at least
             * 5 bytes free and we need just 4. */
            // todo 如果有需要的话，将指针 p 后退 nextdiff 字节，为新 header 空出空间
            p -= nextdiff;
            assert(p >= first.p && p < zl + zlbytes - 1);

            // todo 将 first 的前置节点的长度设置到 p 中，即扩大 p 的 header 空间
            // todo 这里提前更新是可以，因为前面执行了  p -= nextdiff 操作
            zipStorePrevEntryLength(p, first.prevrawlen);

            /* Update offset for tail */
            // todo 更新到尾节点的偏移量
            set_tail = intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) - totlen;

            /* When the tail contains more than one entry, we need to take
             * "nextdiff" in account as well. Otherwise, a change in the
             * size of prevlen doesn't have an effect on the *tail* offset. */
            assert(zipEntrySafe(zl, zlbytes, p, &tail, 1));

            // 如果被删除节点之后仍然有节点存在，到尾节点的偏移量还需要加上 nextdiff,这样才能让表尾偏移量正确对齐表尾节点
            if (p[tail.headersize + tail.len] != ZIP_END) {
                set_tail = set_tail + nextdiff;
            }

            /* Move tail to the front of the ziplist */
            /* since we asserted that p >= first.p. we know totlen >= 0,
             * so we know that p > first.p and this is guaranteed not to reach
             * beyond the allocation, even if the entries lens are corrupted. */
            // todo 从表尾向表头移动数据，覆盖被删除节点的数据
            // T = O(N)
            size_t bytes_to_move = zlbytes - (p - zl) - 1;
            memmove(first.p, p, bytes_to_move);


            // 执行这里，表示被删除节点之后已经没有其他节点了
        } else {
            /* The entire tail was deleted. No need to move memory. */
            set_tail = (first.p - zl) - first.prevrawlen;
        }

        /* Resize the ziplist */

        // 记录
        offset = first.p - zl;

        // 调整 ziplist 缩减后的大小
        zlbytes -= totlen - nextdiff;

        // todo 缩小并更新 ziplist 的长度
        zl = ziplistResize(zl, zlbytes);
        p = zl + offset;

        /* Update record count */
        ZIPLIST_INCR_LENGTH(zl, -deleted);

        /* Set the tail offset computed above */
        assert(set_tail <= zlbytes - ZIPLIST_END_SIZE);

        // 到尾节点的偏移量
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(set_tail);

        /* When nextdiff != 0, the raw length of the next entry has changed, so
         * we need to cascade the update throughout the ziplist */
        // 如果 p 所指向的节点的大小已经变更，那么进行级联更新，检查 p 之后的所有节点是否符合 ziplist 的编码要求
        if (nextdiff != 0)
            zl = __ziplistCascadeUpdate(zl, p);
    }
    return zl;
}

/* Insert item at "p".
 *
 * 根据指针 p 所指定的位置，将长度为 slen 的字符串 s 插入到 zl 中。
 *
 * 如果 p 指向一个节点，那么新节点将放在原有节点的前面。
 *
 */
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {

    // 记录当前 ziplist 的字节大小
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), reqlen, newlen;

    unsigned int prevlensize, prevlen = 0;
    size_t offset;
    int nextdiff = 0;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. Using a value
                                    that is easy to see if for some reason
                                    we use it uninitialized. */
    zlentry tail;

    /* Find out prevlen for the entry that is inserted. */
    // 1 如果 p 不指向列表末端，说明列表非空，并且 p 正指向列表的其中一个节点
    if (p[0] != ZIP_END) { //  非尾插 & 有元素

        // 1.1 记录 p 对应节点所保存的前置节点的长度 prevlen，编码该长度所使用的字节数 prevlensize ；
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);

        // 2 如果 p 指向表尾末端，那么程序需要检查压缩列表的状态：
        // 2.1 如果列表最后一个节点的起始位置的指针也指向 ZIP_END ，那说明列表为空；
        // 2.2 如果列表不为空，那么 ptail 将指向列表的最后一个节点
    } else {
        // 获取指向 zl 最后一个节点（的起始位置）的指针
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);

        // 列表不为空
        if (ptail[0] != ZIP_END) {

            // 取出表尾节点的长度
            // 当插入新节点之后，表尾节点为新节点的前置节点
            prevlen = zipRawEntryLengthSafe(zl, curlen, ptail);
        }
    }

    /* See if the entry can be encoded */
    // 3 尝试将待插入的字符串转换为整数，如果成功的话，value 保存转换后的整数值，encoding 则保存适用于 value 的编码方式
    // 'encoding'被设置为适当的整数编码
    if (zipTryEncoding(s, slen, &value, &encoding)) {
        /* 'encoding' is set to the appropriate integer encoding */
        // 根据整数的编码 encoding 获取整数值的长度（字节数）
        reqlen = zipIntSize(encoding);

        // 4 是个字符串
    } else {
        /* 'encoding' is untouched, however zipStoreEntryEncoding will use the
         * string length to figure out how to encode it. */
        // 设置字符串的长度
        reqlen = slen;
    }

    /* 执行到这里，插入的数据大小就确定了*/

    /* We need space for both the length of the previous entry and
     * the length of the payload.
     * 我们需要空间来容纳前一个入口的长度和有效载荷的长度。
     */
    // 5 计算 编码 prevlen 所需的字节大小
    // todo 即要插入数据对应节点中的存放前置节点大小 prevlensize 的字节数
    reqlen += zipStorePrevEntryLength(NULL, prevlen);
    // 6 计算 编码要插入的数据大小所需的字节大小
    // todo 即要插入数据对应节点中存放自身数据编码信息 encoding 的字节数
    reqlen += zipStoreEntryEncoding(NULL, encoding, slen);

    /* When the insert position is not equal to the tail, we need to
     * make sure that the next entry can hold this entry's length in
     * its prevlen field.
     * 当插入位置不等于尾部时，我们需要确保下一个条目在其prevlen字段中保存该条目的长度。
     */
    int forcelarge = 0;

    // 7 只要新节点不是被添加到列表末端，那么需要判断 p 所指向的节点（的header）能否编码新节点的长度。
    // - 如果新节点被添加到列表末端，则无须考虑新节点的后继节点问题，因为新节点就是最后一个节点
    // - 如果新节点不是被添加到列表末端，则需要考虑新节点的后继节点问题
    // nextdiff 表示新旧编码之间的字节大小差（编码 reqlen 所需字节数和 p 指向节点中保存的前置节点大小的编码字节数的差值），
    // 如果这个值大于 0 ，说明 p 所指向的节点（的header）存不下，需要对 p 所指向的节点（的header）进行扩展
    nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p, reqlen) : 0;

    // 正常的 nextdiff 可能为 0 、-4、4
    if (nextdiff == -4 && reqlen < 4) {
        nextdiff = 0;
        forcelarge = 1;
    }

    /* Store offset because a realloc may change the address of zl. */
    // 8 因为重分配空间可能会改变 zl 的地址，所以在分配之前，需要记录 zl 到 p 的偏移量，然后在分配之后依靠偏移量还原 p
    offset = p - zl;

    // todo ziplist 新的大小包含以下部分：
    // curlen 是 ziplist 原来的长度
    // reqlen 是整个新节点的长度
    // nextdiff 是新节点的后继节点扩展 header 的长度（要么 0 字节，要么 4 个字节）
    newlen = curlen + reqlen + nextdiff;

    // todo 调整 ziplist 大小后，如果新节点不是被添加到列表末端的情况下，需要移动原有节点，为新元素的插入空间腾出位置
    zl = ziplistResize(zl, newlen);

    // 9 还原 p
    p = zl + offset;

    /* Apply memory move when necessary and update tail offset. */
    // 10 如果新节点不是被添加到列表末端
    if (p[0] != ZIP_END) {

        /* Subtract one because of the ZIP_END bytes */
        // 新元素之后还有节点，因为新元素的加入，需要对这些原有节点进行调整
        // todo 10.1 移动现有元素，为新元素的插入空间腾出位置。具体是从 p - nextdiff 处移动，多增加移动的 nextdiff 是为 p 指向的节点（的header）预留的空间
        // T = O(N)
        memmove(p + reqlen, p - nextdiff, curlen - offset - 1 + nextdiff);

        /* Encode this entry's raw length in the next entry. */
        // 10.2 将新节点的长度编码至后置节点, p+reqlen 定位到后置节点，reqlen 是新节点的长度
        // todo 注意，编码是寻找相应的数据类型存储节点长度，最终保存时以匹配的数据类型存储节点长度
        if (forcelarge)
            zipStorePrevEntryLengthLarge(p + reqlen, reqlen);
        else
            zipStorePrevEntryLength(p + reqlen, reqlen);

        /* Update offset for tail */
        // 10.3 更新到达表尾的偏移量，即将原来记录的到达尾节点的偏移量，加上新节点的长度
        ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + reqlen);

        /* When the tail contains more than one entry, we need to take
         * "nextdiff" in account as well. Otherwise, a change in the
         * size of prevlen doesn't have an effect on the *tail* offset. */
        // 10.4 如果新节点的后面有多于一个节点,那么程序需要将 nextdiff 记录的字节数也计算到表尾偏移量中,这样才能让表尾偏移量正确对齐表尾节点
        assert(zipEntrySafe(zl, newlen, p + reqlen, &tail, 1));
        if (p[reqlen + tail.headersize + tail.len] != ZIP_END) {
            ZIPLIST_TAIL_OFFSET(zl) =
                    intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) + nextdiff);
        }

        // 11 新元素是新的表尾节点
    } else {

        /* This element will be the new tail. */
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p - zl);
    }

    /* When nextdiff != 0, the raw length of the next entry has changed, so
     * we need to cascade the update throughout the ziplist */
    // 12 当 nextdiff != 0 时，说明新节点的后继节点的（header 部分）长度已经被改变，所以需要级联地更新后续的节点
    if (nextdiff != 0) {

        // 级联更新的时候，可能 ziplist 会重新分配内存，这里记录 p 的偏移量
        offset = p - zl;

        // 尝试级联更新,针对 p + reqlen 的后续节点
        // todo 使用 zrealloc ，扩展时不会改变现有的元素
        zl = __ziplistCascadeUpdate(zl, p + reqlen);

        // 还原 p
        p = zl + offset;
    }

    /* Write the entry */
    // todo 13 写入节点项的 header 元数据信息，注意写入后指针的移动
    p += zipStorePrevEntryLength(p, prevlen);
    p += zipStoreEntryEncoding(p, encoding, slen);

    // todo  14 写入节点值到节点项
    // 字符串的情况
    if (ZIP_IS_STR(encoding)) {
        memcpy(p, s, slen);

        // 整数的情况
    } else {
        zipSaveInteger(p, value, encoding);
    }

    // 15 更新列表的节点数据计数器
    ZIPLIST_INCR_LENGTH(zl, 1);
    return zl;
}

/* Merge ziplists 'first' and 'second' by appending 'second' to 'first'.
 *
 * NOTE: The larger ziplist is reallocated to contain the new merged ziplist.
 * Either 'first' or 'second' can be used for the result.  The parameter not
 * used will be free'd and set to NULL.
 *
 * After calling this function, the input parameters are no longer valid since
 * they are changed and free'd in-place.
 *
 * The result ziplist is the contents of 'first' followed by 'second'.
 *
 * On failure: returns NULL if the merge is impossible.
 * On success: returns the merged ziplist (which is expanded version of either
 * 'first' or 'second', also frees the other unused input ziplist, and sets the
 * input ziplist argument equal to newly reallocated ziplist return value. */
unsigned char *ziplistMerge(unsigned char **first, unsigned char **second) {
    /* If any params are null, we can't merge, so NULL. */
    if (first == NULL || *first == NULL || second == NULL || *second == NULL)
        return NULL;

    /* Can't merge same list into itself. */
    if (*first == *second)
        return NULL;

    size_t first_bytes = intrev32ifbe(ZIPLIST_BYTES(*first));
    size_t first_len = intrev16ifbe(ZIPLIST_LENGTH(*first));

    size_t second_bytes = intrev32ifbe(ZIPLIST_BYTES(*second));
    size_t second_len = intrev16ifbe(ZIPLIST_LENGTH(*second));

    int append;
    unsigned char *source, *target;
    size_t target_bytes, source_bytes;
    /* Pick the largest ziplist so we can resize easily in-place.
     * We must also track if we are now appending or prepending to
     * the target ziplist. */
    if (first_len >= second_len) {
        /* retain first, append second to first. */
        target = *first;
        target_bytes = first_bytes;
        source = *second;
        source_bytes = second_bytes;
        append = 1;
    } else {
        /* else, retain second, prepend first to second. */
        target = *second;
        target_bytes = second_bytes;
        source = *first;
        source_bytes = first_bytes;
        append = 0;
    }

    /* Calculate final bytes (subtract one pair of metadata) */
    size_t zlbytes = first_bytes + second_bytes -
                     ZIPLIST_HEADER_SIZE - ZIPLIST_END_SIZE;
    size_t zllength = first_len + second_len;

    /* Combined zl length should be limited within UINT16_MAX */
    zllength = zllength < UINT16_MAX ? zllength : UINT16_MAX;

    /* Save offset positions before we start ripping memory apart. */
    size_t first_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*first));
    size_t second_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*second));

    /* Extend target to new zlbytes then append or prepend source. */
    target = zrealloc(target, zlbytes);
    if (append) {
        /* append == appending to target */
        /* Copy source after target (copying over original [END]):
         *   [TARGET - END, SOURCE - HEADER] */
        memcpy(target + target_bytes - ZIPLIST_END_SIZE,
               source + ZIPLIST_HEADER_SIZE,
               source_bytes - ZIPLIST_HEADER_SIZE);
    } else {
        /* !append == prepending to target */
        /* Move target *contents* exactly size of (source - [END]),
         * then copy source into vacated space (source - [END]):
         *   [SOURCE - END, TARGET - HEADER] */
        memmove(target + source_bytes - ZIPLIST_END_SIZE,
                target + ZIPLIST_HEADER_SIZE,
                target_bytes - ZIPLIST_HEADER_SIZE);
        memcpy(target, source, source_bytes - ZIPLIST_END_SIZE);
    }

    /* Update header metadata. */
    ZIPLIST_BYTES(target) = intrev32ifbe(zlbytes);
    ZIPLIST_LENGTH(target) = intrev16ifbe(zllength);
    /* New tail offset is:
     *   + N bytes of first ziplist
     *   - 1 byte for [END] of first ziplist
     *   + M bytes for the offset of the original tail of the second ziplist
     *   - J bytes for HEADER because second_offset keeps no header. */
    ZIPLIST_TAIL_OFFSET(target) = intrev32ifbe(
            (first_bytes - ZIPLIST_END_SIZE) +
            (second_offset - ZIPLIST_HEADER_SIZE));

    /* __ziplistCascadeUpdate just fixes the prev length values until it finds a
     * correct prev length value (then it assumes the rest of the list is okay).
     * We tell CascadeUpdate to start at the first ziplist's tail element to fix
     * the merge seam. */
    target = __ziplistCascadeUpdate(target, target + first_offset);

    /* Now free and NULL out what we didn't realloc */
    if (append) {
        zfree(*second);
        *second = NULL;
        *first = target;
    } else {
        zfree(*first);
        *first = NULL;
        *second = target;
    }
    return target;
}

/*
 * todo 新增
 *
 * 将长度为 slen 的字符串 s 推入到 zl 中。
 *
 * where 参数的值决定了推入的方向：
 * - 值为 ZIPLIST_HEAD 时，将新值推入到表头。
 * - 否则，将新值推入到表末端。
 *
 * 函数的返回值为添加新值后的 ziplist 。
 *
 * T = O(N^2)
 */
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where) {

    // 1 根据 where 参数的值，决定将值推入到表头还是表尾
    unsigned char *p;

    // 1.1 p 为压缩列表头节点的起始位置指针 or 压缩列表末端的起始位置指针
    p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);

    // 2 返回添加新值后的 ziplist
    return __ziplistInsert(zl, p, s, slen);
}

/* Returns an offset to use for iterating with ziplistNext. When the given
 * index is negative, the list is traversed back to front. When the list
 * doesn't contain an element at the provided index, NULL is returned.*/
/*
 * todo 查询
 *
 * 根据给定索引，遍历列表，并返回索引指定节点的指针。
 *
 * 如果索引为正，那么从表头向表尾遍历。
 * 如果索引为负，那么从表尾向表头遍历。
 * 正数索引从 0 开始，负数索引从 -1 开始。
 *
 * 如果索引超过列表的节点数量，或者列表为空，那么返回 NULL 。
 *
 * T = O(N)
 */
unsigned char *ziplistIndex(unsigned char *zl, int index) {
    unsigned char *p;
    unsigned int prevlensize, prevlen = 0;

    // 获取 zl 的所占用的内存字节数
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));

    // 1 处理负数索引，从表尾向表头遍历
    if (index < 0) {

        // 将索引转换为正数
        index = (-index) - 1;

        // todo 通过地址、偏移量定位到表尾节点
        p = ZIPLIST_ENTRY_TAIL(zl);

        // 如果列表不为空
        if (p[0] != ZIP_END) {
            /* No need for "safe" check: when going backwards, we know the header
             * we're parsing is in the range, we just need to assert (below) that
             * the size we take doesn't cause p to go outside the allocation. */

            // 获取为节点 p 保存的前置节点的长度和编码长度的字节数
            ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);

            // todo 不断前移指针 p，每次移动前置节点长度 个步长
            while (prevlen > 0 && index--) {
                p -= prevlen;
                assert(p >= zl + ZIPLIST_HEADER_SIZE && p < zl + zlbytes - ZIPLIST_END_SIZE);

                // 计算前面一个 p 的前置节点的长度和编码长度的字节数
                // todo 得益于 ziplist 的结构
                ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            }
        }


        // 处理正数
    } else {

        // todo 定位到表头节点
        p = ZIPLIST_ENTRY_HEAD(zl);

        // todo 不断向后移动指针
        while (index--) {
            /* Use the "safe" length: When we go forward, we need to be careful
             * not to decode an entry header if it's past the ziplist allocation. */
            // 每个步长是 节点占用的字节总数
            p += zipRawEntryLengthSafe(zl, zlbytes, p);

            // 移动到了表尾
            if (p[0] == ZIP_END)
                break;
        }
    }

    // 如果遍历完成后到了表尾 或 到了表头，则返回 NULL
    if (p[0] == ZIP_END || index > 0)
        return NULL;
    zipAssertValidEntry(zl, zlbytes, p);

    // 返回结果
    return p;
}

/* Return pointer to next entry in ziplist.
 *
 * 返回 p 所指向节点的后置节点
 *
 * zl is the pointer to the ziplist
 * p is the pointer to the current element
 *
 * 如果 p 为表末端，或者 p 已经是表尾节点，那么返回 NULL
 *
 * The element after 'p' is returned, otherwise NULL if we are at the end. */
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {
    ((void) zl);

    // 1 ziplist 总字节数
    size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));

    /* "p" could be equal to ZIP_END, caused by ziplistDelete,
     * and we should return NULL. Otherwise, we should return NULL
     * when the *next* element is ZIP_END (there is no next entry). */
    // 如果 p 已经指向列表末端，则直接返回 NULL
    if (p[0] == ZIP_END) {
        return NULL;
    }

    // todo 指向下一个节点，即指针移动 p 的总字节数
    p += zipRawEntryLength(p);
    if (p[0] == ZIP_END) {
        return NULL;
    }

    zipAssertValidEntry(zl, zlbytes, p);
    return p;
}

/* Return pointer to previous entry in ziplist.
 *
 * 返回 p 所指向节点的前置节点。
 *
 * 如果 p 所指向为空，或者 p 已经指向表头节点，那么返回 NULL
 */
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {
    unsigned int prevlensize, prevlen = 0;

    /* Iterating backwards from ZIP_END should return the tail. When "p" is
     * equal to the first element of the list, we're already at the head,
     * and should return NULL. */

    // 1 如果 p 指向列表末端，则尝试取出列表尾端节点
    if (p[0] == ZIP_END) {

        // todo 通过地址、偏移量定位到表尾节点
        p = ZIPLIST_ENTRY_TAIL(zl);
        return (p[0] == ZIP_END) ? NULL : p;

        // 2 如果 p 指向列表第一个节点，则返回 NULL
    } else if (p == ZIPLIST_ENTRY_HEAD(zl)) {
        return NULL;

        // 3 p 指向列表的中间位置
    } else {

        // 获取 p 指向的节点的 前置节点长度、编码前置节点长度的字节数
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
        assert(prevlen > 0);

        // 向后移动指针 p
        p -= prevlen;
        size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));
        zipAssertValidEntry(zl, zlbytes, p);
        return p;
    }
}

/* Get entry pointed to by 'p' and store in either '*sstr' or 'sval' depending
 * on the encoding of the entry. '*sstr' is always set to NULL to be able
 * to find out whether the string pointer or the integer value was set.
 * Return 0 if 'p' points to the end of the ziplist, 1 otherwise.
 *
 * 取出 p 所指向节点的值：
 *
 * - 如果节点保存的是字符串，那么将字符串值指针保存到 *sstr 中，字符串长度保存到 *slen
 * - 如果节点保存的是整数，那么将整数保存到 *sval
 *
 * 程序可以通过检查 *sstr 是否为 NULL 来检查值是字符串还是整数。
 *
 * 提取值成功返回 1 ，如果 p 为空，或者 p 指向的是列表末端，那么返回 0 ，提取值失败。
 *
 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval) {
    zlentry entry;
    if (p == NULL || p[0] == ZIP_END) return 0;
    if (sstr) *sstr = NULL;

    // 取出 p 所指向的节点的各项信息，并保存到结构 entry 中
    zipEntry(p,
             &entry); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */

     // 节点的值为字符串，将字符串长度保存到 *slen ，字符串保存到 *sstr
    if (ZIP_IS_STR(entry.encoding)) {
        if (sstr) {
            *slen = entry.len;
            // 指针指向数据的起始位置
            *sstr = p + entry.headersize;
        }

        // 节点的值为整数，解码值，并将值保存到 *sval
    } else {
        if (sval) {
            // 指针指向数据的起始位置
            *sval = zipLoadInteger(p + entry.headersize, entry.encoding);
        }
    }
    return 1;
}

/* Insert an entry at "p".
 *
 * 将长度为 slen 的给定值 s 插入到给定的位置 p 中
 *
 * 如果 p 指向一个节点，那么新节点将放在原有节点的前面。
 *
 **/
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    return __ziplistInsert(zl, p, s, slen);
}

/* Delete a single entry from the ziplist, pointed to by *p.
 * Also update *p in place, to be able to iterate over the
 * ziplist, while deleting entries.
 *
 * 从 zl 中删除 *p 所指向的节点.
 *
 * 并且原地更新 *p 所指向的位置，使得可以在迭代列表的过程中对节点进行删除。
 *
 */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p) {

    // 记录 *p 的偏移量
    size_t offset = *p - zl;

    // 执行删除操作，该操作会对 zl 进行内存重分配
    zl = __ziplistDelete(zl, *p, 1);

    /* Store pointer to current element in p, because ziplistDelete will
     * do a realloc which might result in a different "zl"-pointer.
     * When the delete direction is back to front, we might delete the last
     * entry and end up with "p" pointing to ZIP_END, so check this. */
    // 经过上面的操作，zl 一般会缩小一个列表项字节，这里 *p 跳到下个列表项了
    *p = zl + offset;

    return zl;
}

/* Delete a range of entries from the ziplist.
 *
 * 从 index 索引指定的节点开始，连续地从 zl 中删除 num 个节点。
 */
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num) {
    // 根据索引定位到节点
    // T = O(N)
    unsigned char *p = ziplistIndex(zl, index);

    // 连续删除 num 个节点
    // T = O(N^2)
    return (p == NULL) ? zl : __ziplistDelete(zl, p, num);
}

/* Replaces the entry at p. This is equivalent to a delete and an insert,
 * but avoids some overhead when replacing a value of the same size.
 *
 * 替换p处的条目。这相当于删除和插入，但在替换相同大小的值时避免了一些开销。
 *
 */
unsigned char *ziplistReplace(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {

    /* get metadata of the current entry */
    zlentry entry;
    zipEntry(p, &entry);

    /* compute length of entry to store, excluding prevlen */
    unsigned int reqlen;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. */
    if (zipTryEncoding(s, slen, &value, &encoding)) {
        reqlen = zipIntSize(encoding); /* encoding is set */
    } else {
        reqlen = slen; /* encoding == 0 */
    }
    reqlen += zipStoreEntryEncoding(NULL, encoding, slen);

    if (reqlen == entry.lensize + entry.len) {
        /* Simply overwrite the element. */
        p += entry.prevrawlensize;
        p += zipStoreEntryEncoding(p, encoding, slen);
        if (ZIP_IS_STR(encoding)) {
            memcpy(p, s, slen);
        } else {
            zipSaveInteger(p, value, encoding);
        }
    } else {
        /* Fallback. */
        zl = ziplistDelete(zl, &p);
        zl = ziplistInsert(zl, p, s, slen);
    }
    return zl;
}

/* Compare entry pointer to by 'p' with 'sstr' of length 'slen'. */
/* Return 1 if equal.
 *
 * 将 p 所指向的节点的值和 sstr 进行对比。
 *
* 如果节点值和 sstr 的值相等，返回 1 ，不相等则返回 0 。
 *
 */
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen) {
    zlentry entry;
    unsigned char sencoding;
    long long zval, sval;
    if (p[0] == ZIP_END) return 0;

    // 取出节点
    zipEntry(p,
             &entry); /* no need for "safe" variant since the input pointer was validated by the function that returned it. */

    // 节点值为字符串，进行字符串对比
    if (ZIP_IS_STR(entry.encoding)) {
        /* Raw compare */
        if (entry.len == slen) {
            return memcmp(p + entry.headersize, sstr, slen) == 0;
        } else {
            return 0;
        }

        // 节点值为整数，进行整数对比
    } else {
        /* Try to compare encoded values. Don't compare encoding because
         * different implementations may encoded integers differently. */
        if (zipTryEncoding(sstr, slen, &sval, &sencoding)) {
            zval = zipLoadInteger(p + entry.headersize, entry.encoding);
            return zval == sval;
        }
    }
    return 0;
}

/* Find pointer to the entry equal to the specified entry.
 *
 * 寻找节点值和 vstr 相等的列表节点，并返回该节点的指针
 *
 * Skip 'skip' entriesbetween every comparison. Returns NULL when the field could not be found.
 *
 * 每次比对之前都跳过 skip 个节点。如果找不到相应的节点，则返回 NULL 。
 *
 *
 */
unsigned char *
ziplistFind(unsigned char *zl, unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip) {
    int skipcnt = 0;
    unsigned char vencoding = 0;
    long long vll = 0;
    size_t zlbytes = ziplistBlobLen(zl);

    // 只要未到达列表末端，就一直迭代
    // T = O(N^2)
    while (p[0] != ZIP_END) {
        struct zlentry e;
        unsigned char *q;

        assert(zipEntrySafe(zl, zlbytes, p, &e, 1));
        q = p + e.prevrawlensize + e.lensize;

        if (skipcnt == 0) {

            // 对比字符串值
            // T = O(N)
            /* Compare current entry with specified entry */
            if (ZIP_IS_STR(e.encoding)) {
                if (e.len == vlen && memcmp(q, vstr, vlen) == 0) {
                    return p;
                }
            } else {


                // 因为传入值有可能被编码了,所以当第一次进行值对比时，程序会对传入值进行解码,这个解码操作只会进行一次
                /* Find out if the searched field can be encoded. Note that
                 * we do it only the first time, once done vencoding is set
                 * to non-zero and vll is set to the integer value. */
                if (vencoding == 0) {
                    if (!zipTryEncoding(vstr, vlen, &vll, &vencoding)) {
                        /* If the entry can't be encoded we set it to
                         * UCHAR_MAX so that we don't retry again the next
                         * time. */
                        vencoding = UCHAR_MAX;
                    }
                    /* Must be non-zero by now */
                    assert(vencoding);
                }


                // 对比整数值
                /* Compare current entry with specified entry, do it only
                 * if vencoding != UCHAR_MAX because if there is no encoding
                 * possible for the field it can't be a valid integer. */
                if (vencoding != UCHAR_MAX) {
                    long long ll = zipLoadInteger(q, e.encoding);
                    if (ll == vll) {
                        return p;
                    }
                }
            }

            /* Reset skip count */
            skipcnt = skip;
        } else {
            /* Skip entry */
            skipcnt--;
        }

        // 后移指针，指向后置节点
        /* Move to next entry */
        p = q + e.len;
    }

    // 没有找到指定的节点
    return NULL;
}

/* Return length of ziplist.
 *
 * 返回 ziplist 中的节点个数
 *
 */
unsigned int ziplistLen(unsigned char *zl) {
    unsigned int len = 0;

    // 1 节点数小于 UINT16_MAX，直接从 ziplist 的 zllen 取
    // T = O(1)
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX) {
        len = intrev16ifbe(ZIPLIST_LENGTH(zl));


        // 2 节点数大于 UINT16_MAX 时，需要遍历整个列表才能计算出节点数
    } else {
        // zplist 起始地址 + zplist 头 -> 定位 zplist 中第一个元素的起始地址
        unsigned char *p = zl + ZIPLIST_HEADER_SIZE;
        size_t zlbytes = intrev32ifbe(ZIPLIST_BYTES(zl));

        // 遍历
        while (*p != ZIP_END) {
            p += zipRawEntryLengthSafe(zl, zlbytes, p);
            len++;
        }

        /* Re-store length if small enough */
        if (len < UINT16_MAX) ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
    }


    return len;
}


/* Return ziplist blob size in bytes.
 *
 * 返回整个 ziplist 占用的内存字节数
 *
 * T = O(1)
 */
size_t ziplistBlobLen(unsigned char *zl) {
    return intrev32ifbe(ZIPLIST_BYTES(zl));
}

void ziplistRepr(unsigned char *zl) {
    unsigned char *p;
    int index = 0;
    zlentry entry;
    size_t zlbytes = ziplistBlobLen(zl);

    printf(
            "{total bytes %u} "
            "{num entries %u}\n"
            "{tail offset %u}\n",
            intrev32ifbe(ZIPLIST_BYTES(zl)),
            intrev16ifbe(ZIPLIST_LENGTH(zl)),
            intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)));
    p = ZIPLIST_ENTRY_HEAD(zl);
    while (*p != ZIP_END) {
        assert(zipEntrySafe(zl, zlbytes, p, &entry, 1));
        printf(
                "{\n"
                "\taddr 0x%08lx,\n"
                "\tindex %2d,\n"
                "\toffset %5lu,\n"
                "\thdr+entry len: %5u,\n"
                "\thdr len%2u,\n"
                "\tprevrawlen: %5u,\n"
                "\tprevrawlensize: %2u,\n"
                "\tpayload %5u\n",
                (long unsigned) p,
                index,
                (unsigned long) (p - zl),
                entry.headersize + entry.len,
                entry.headersize,
                entry.prevrawlen,
                entry.prevrawlensize,
                entry.len);
        printf("\tbytes: ");
        for (unsigned int i = 0; i < entry.headersize + entry.len; i++) {
            printf("%02x|", p[i]);
        }
        printf("\n");
        p += entry.headersize;
        if (ZIP_IS_STR(entry.encoding)) {
            printf("\t[str]");
            if (entry.len > 40) {
                if (fwrite(p, 40, 1, stdout) == 0) perror("fwrite");
                printf("...");
            } else {
                if (entry.len &&
                    fwrite(p, entry.len, 1, stdout) == 0)
                    perror("fwrite");
            }
        } else {
            printf("\t[int]%lld", (long long) zipLoadInteger(p, entry.encoding));
        }
        printf("\n}\n");
        p += entry.len;
        index++;
    }
    printf("{end}\n\n");
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
int ziplistValidateIntegrity(unsigned char *zl, size_t size, int deep,
                             ziplistValidateEntryCB entry_cb, void *cb_userdata) {
    /* check that we can actually read the header. (and ZIP_END) */
    if (size < ZIPLIST_HEADER_SIZE + ZIPLIST_END_SIZE)
        return 0;

    /* check that the encoded size in the header must match the allocated size. */
    size_t bytes = intrev32ifbe(ZIPLIST_BYTES(zl));
    if (bytes != size)
        return 0;

    /* the last byte must be the terminator. */
    if (zl[size - ZIPLIST_END_SIZE] != ZIP_END)
        return 0;

    /* make sure the tail offset isn't reaching outside the allocation. */
    if (intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)) > size - ZIPLIST_END_SIZE)
        return 0;

    if (!deep)
        return 1;

    unsigned int count = 0;
    unsigned char *p = ZIPLIST_ENTRY_HEAD(zl);
    unsigned char *prev = NULL;
    size_t prev_raw_size = 0;
    while (*p != ZIP_END) {
        struct zlentry e;
        /* Decode the entry headers and fail if invalid or reaches outside the allocation */
        if (!zipEntrySafe(zl, size, p, &e, 1))
            return 0;

        /* Make sure the record stating the prev entry size is correct. */
        if (e.prevrawlen != prev_raw_size)
            return 0;

        /* Optionally let the caller validate the entry too. */
        if (entry_cb && !entry_cb(p, cb_userdata))
            return 0;

        /* Move to the next entry */
        prev_raw_size = e.headersize + e.len;
        prev = p;
        p += e.headersize + e.len;
        count++;
    }

    /* Make sure the <zltail> entry really do point to the start of the last entry. */
    if (prev != ZIPLIST_ENTRY_TAIL(zl))
        return 0;

    /* Check that the count in the header is correct */
    unsigned int header_count = intrev16ifbe(ZIPLIST_LENGTH(zl));
    if (header_count != UINT16_MAX && count != header_count)
        return 0;

    return 1;
}

/* Randomly select a pair of key and value.
 * total_count is a pre-computed length/2 of the ziplist (to avoid calls to ziplistLen)
 * 'key' and 'val' are used to store the result key value pair.
 * 'val' can be NULL if the value is not needed. */
void ziplistRandomPair(unsigned char *zl, unsigned long total_count, ziplistEntry *key, ziplistEntry *val) {
    int ret;
    unsigned char *p;

    /* Avoid div by zero on corrupt ziplist */
    assert(total_count);

    /* Generate even numbers, because ziplist saved K-V pair */
    int r = (rand() % total_count) * 2;
    p = ziplistIndex(zl, r);
    ret = ziplistGet(p, &key->sval, &key->slen, &key->lval);
    assert(ret != 0);

    if (!val)
        return;
    p = ziplistNext(zl, p);
    ret = ziplistGet(p, &val->sval, &val->slen, &val->lval);
    assert(ret != 0);
}

/* int compare for qsort */
int uintCompare(const void *a, const void *b) {
    return (*(unsigned int *) a - *(unsigned int *) b);
}

/* Helper method to store a string into from val or lval into dest */
static inline void ziplistSaveValue(unsigned char *val, unsigned int len, long long lval, ziplistEntry *dest) {
    dest->sval = val;
    dest->slen = len;
    dest->lval = lval;
}

/* Randomly select count of key value pairs and store into 'keys' and
 * 'vals' args. The order of the picked entries is random, and the selections
 * are non-unique (repetitions are possible).
 * The 'vals' arg can be NULL in which case we skip these. */
void ziplistRandomPairs(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    unsigned char *p, *key, *value;
    unsigned int klen = 0, vlen = 0;
    long long klval = 0, vlval = 0;

    /* Notice: the index member must be first due to the use in uintCompare */
    typedef struct {
        unsigned int index;
        unsigned int order;
    } rand_pick;
    rand_pick *picks = zmalloc(sizeof(rand_pick) * count);
    unsigned int total_size = ziplistLen(zl) / 2;

    /* Avoid div by zero on corrupt ziplist */
    assert(total_size);

    /* create a pool of random indexes (some may be duplicate). */
    for (unsigned int i = 0; i < count; i++) {
        picks[i].index = (rand() % total_size) * 2; /* Generate even indexes */
        /* keep track of the order we picked them */
        picks[i].order = i;
    }

    /* sort by indexes. */
    qsort(picks, count, sizeof(rand_pick), uintCompare);

    /* fetch the elements form the ziplist into a output array respecting the original order. */
    unsigned int zipindex = 0, pickindex = 0;
    p = ziplistIndex(zl, 0);
    while (ziplistGet(p, &key, &klen, &klval) && pickindex < count) {
        p = ziplistNext(zl, p);
        assert(ziplistGet(p, &value, &vlen, &vlval));
        while (pickindex < count && zipindex == picks[pickindex].index) {
            int storeorder = picks[pickindex].order;
            ziplistSaveValue(key, klen, klval, &keys[storeorder]);
            if (vals)
                ziplistSaveValue(value, vlen, vlval, &vals[storeorder]);
            pickindex++;
        }
        zipindex += 2;
        p = ziplistNext(zl, p);
    }

    zfree(picks);
}

/* Randomly select count of key value pairs and store into 'keys' and
 * 'vals' args. The selections are unique (no repetitions), and the order of
 * the picked entries is NOT-random.
 * The 'vals' arg can be NULL in which case we skip these.
 * The return value is the number of items picked which can be lower than the
 * requested count if the ziplist doesn't hold enough pairs. */
unsigned int ziplistRandomPairsUnique(unsigned char *zl, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    unsigned char *p, *key;
    unsigned int klen = 0;
    long long klval = 0;
    unsigned int total_size = ziplistLen(zl) / 2;
    unsigned int index = 0;
    if (count > total_size)
        count = total_size;

    /* To only iterate once, every time we try to pick a member, the probability
     * we pick it is the quotient of the count left we want to pick and the
     * count still we haven't visited in the dict, this way, we could make every
     * member be equally picked.*/
    p = ziplistIndex(zl, 0);
    unsigned int picked = 0, remaining = count;
    while (picked < count && p) {
        double randomDouble = ((double) rand()) / RAND_MAX;
        double threshold = ((double) remaining) / (total_size - index);
        if (randomDouble <= threshold) {
            assert(ziplistGet(p, &key, &klen, &klval));
            ziplistSaveValue(key, klen, klval, &keys[picked]);
            p = ziplistNext(zl, p);
            assert(p);
            if (vals) {
                assert(ziplistGet(p, &key, &klen, &klval));
                ziplistSaveValue(key, klen, klval, &vals[picked]);
            }
            remaining--;
            picked++;
        } else {
            p = ziplistNext(zl, p);
            assert(p);
        }
        p = ziplistNext(zl, p);
        index++;
    }
    return picked;
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include "adlist.h"
#include "sds.h"

#define debug(f, ...) { if (DEBUG) printf(f, __VA_ARGS__); }

static unsigned char *createList() {
    unsigned char *zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char*)"foo", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"quux", 4, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"hello", 5, ZIPLIST_HEAD);
    zl = ziplistPush(zl, (unsigned char*)"1024", 4, ZIPLIST_TAIL);
    return zl;
}

static unsigned char *createIntList() {
    unsigned char *zl = ziplistNew();
    char buf[32];

    sprintf(buf, "100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "128000");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "-100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "4294967296");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "much much longer non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    return zl;
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static void stress(int pos, int num, int maxsize, int dnum) {
    int i,j,k;
    unsigned char *zl;
    char posstr[2][5] = { "HEAD", "TAIL" };
    long long start;
    for (i = 0; i < maxsize; i+=dnum) {
        zl = ziplistNew();
        for (j = 0; j < i; j++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,ZIPLIST_TAIL);
        }

        /* Do num times a push+pop from pos */
        start = usec();
        for (k = 0; k < num; k++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,pos);
            zl = ziplistDeleteRange(zl,0,1);
        }
        printf("List size: %8d, bytes: %8d, %dx push+pop (%s): %6lld usec\n",
            i,intrev32ifbe(ZIPLIST_BYTES(zl)),num,posstr[pos],usec()-start);
        zfree(zl);
    }
}

static unsigned char *pop(unsigned char *zl, int where) {
    unsigned char *p, *vstr;
    unsigned int vlen;
    long long vlong;

    p = ziplistIndex(zl,where == ZIPLIST_HEAD ? 0 : -1);
    if (ziplistGet(p,&vstr,&vlen,&vlong)) {
        if (where == ZIPLIST_HEAD)
            printf("Pop head: ");
        else
            printf("Pop tail: ");

        if (vstr) {
            if (vlen && fwrite(vstr,vlen,1,stdout) == 0) perror("fwrite");
        }
        else {
            printf("%lld", vlong);
        }

        printf("\n");
        return ziplistDelete(zl,&p);
    } else {
        printf("ERROR: Could not pop\n");
        exit(1);
    }
}

static int randstring(char *target, unsigned int min, unsigned int max) {
    int p = 0;
    int len = min+rand()%(max-min+1);
    int minval, maxval;
    switch(rand() % 3) {
    case 0:
        minval = 0;
        maxval = 255;
    break;
    case 1:
        minval = 48;
        maxval = 122;
    break;
    case 2:
        minval = 48;
        maxval = 52;
    break;
    default:
        assert(NULL);
    }

    while(p < len)
        target[p++] = minval+rand()%(maxval-minval+1);
    return len;
}

static void verify(unsigned char *zl, zlentry *e) {
    int len = ziplistLen(zl);
    zlentry _e;

    ZIPLIST_ENTRY_ZERO(&_e);

    for (int i = 0; i < len; i++) {
        memset(&e[i], 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, i), &e[i]);

        memset(&_e, 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, -len+i), &_e);

        assert(memcmp(&e[i], &_e, sizeof(zlentry)) == 0);
    }
}

static unsigned char *insertHelper(unsigned char *zl, char ch, size_t len, unsigned char *pos) {
    assert(len <= ZIP_BIG_PREVLEN);
    unsigned char data[ZIP_BIG_PREVLEN] = {0};
    memset(data, ch, len);
    return ziplistInsert(zl, pos, data, len);
}

static int compareHelper(unsigned char *zl, char ch, size_t len, int index) {
    assert(len <= ZIP_BIG_PREVLEN);
    unsigned char data[ZIP_BIG_PREVLEN] = {0};
    memset(data, ch, len);
    unsigned char *p = ziplistIndex(zl, index);
    assert(p != NULL);
    return ziplistCompare(p, data, len);
}

static size_t strEntryBytesSmall(size_t slen) {
    return slen + zipStorePrevEntryLength(NULL, 0) + zipStoreEntryEncoding(NULL, 0, slen);
}

static size_t strEntryBytesLarge(size_t slen) {
    return slen + zipStorePrevEntryLength(NULL, ZIP_BIG_PREVLEN) + zipStoreEntryEncoding(NULL, 0, slen);
}

/* ./redis-server test ziplist <randomseed> --accurate */
int ziplistTest(int argc, char **argv, int accurate) {
    unsigned char *zl, *p;
    unsigned char *entry;
    unsigned int elen;
    long long value;
    int iteration;

    /* If an argument is given, use it as the random seed. */
    if (argc >= 4)
        srand(atoi(argv[3]));

    zl = createIntList();
    ziplistRepr(zl);

    zfree(zl);

    zl = createList();
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_HEAD);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zfree(zl);

    printf("Get element at index 3:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 3);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index 3\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index 4 (out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", (long)(p-zl));
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -1 (last element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -1\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -4 (first element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -4\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -5 (reverse out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -5);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", (long)(p-zl));
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 0 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 1 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 2 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 2);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate starting out of range:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("No entry\n");
        } else {
            printf("ERROR\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front, deleting all items:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            zl = ziplistDelete(zl,&p);
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Delete inclusive range 0,0:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 0,1:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 1,2:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with start index out of range:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 5, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with num overflow:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 5);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete foo while iterating:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        while (ziplistGet(p,&entry,&elen,&value)) {
            if (entry && strncmp("foo",(char*)entry,elen) == 0) {
                printf("Delete foo\n");
                zl = ziplistDelete(zl,&p);
            } else {
                printf("Entry: ");
                if (entry) {
                    if (elen && fwrite(entry,elen,1,stdout) == 0)
                        perror("fwrite");
                } else {
                    printf("%lld",value);
                }
                p = ziplistNext(zl,p);
                printf("\n");
            }
        }
        printf("\n");
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Replace with same size:\n");
    {
        zl = createList(); /* "hello", "foo", "quux", "1024" */
        unsigned char *orig_zl = zl;
        p = ziplistIndex(zl, 0);
        zl = ziplistReplace(zl, p, (unsigned char*)"zoink", 5);
        p = ziplistIndex(zl, 3);
        zl = ziplistReplace(zl, p, (unsigned char*)"yy", 2);
        p = ziplistIndex(zl, 1);
        zl = ziplistReplace(zl, p, (unsigned char*)"65536", 5);
        p = ziplistIndex(zl, 0);
        assert(!memcmp((char*)p,
                       "\x00\x05zoink"
                       "\x07\xf0\x00\x00\x01" /* 65536 as int24 */
                       "\x05\x04quux" "\x06\x02yy" "\xff",
                       23));
        assert(zl == orig_zl); /* no reallocations have happened */
        zfree(zl);
        printf("SUCCESS\n\n");
    }

    printf("Replace with different size:\n");
    {
        zl = createList(); /* "hello", "foo", "quux", "1024" */
        p = ziplistIndex(zl, 1);
        zl = ziplistReplace(zl, p, (unsigned char*)"squirrel", 8);
        p = ziplistIndex(zl, 0);
        assert(!strncmp((char*)p,
                        "\x00\x05hello" "\x07\x08squirrel" "\x0a\x04quux"
                        "\x06\xc0\x00\x04" "\xff",
                        28));
        zfree(zl);
        printf("SUCCESS\n\n");
    }

    printf("Regression test for >255 byte strings:\n");
    {
        char v1[257] = {0}, v2[257] = {0};
        memset(v1,'x',256);
        memset(v2,'y',256);
        zl = ziplistNew();
        zl = ziplistPush(zl,(unsigned char*)v1,strlen(v1),ZIPLIST_TAIL);
        zl = ziplistPush(zl,(unsigned char*)v2,strlen(v2),ZIPLIST_TAIL);

        /* Pop values again and compare their value. */
        p = ziplistIndex(zl,0);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v1,(char*)entry,elen) == 0);
        p = ziplistIndex(zl,1);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v2,(char*)entry,elen) == 0);
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Regression test deleting next to last entries:\n");
    {
        char v[3][257] = {{0}};
        zlentry e[3] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0,
                         .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};
        size_t i;

        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            memset(v[i], 'a' + i, sizeof(v[0]));
        }

        v[0][256] = '\0';
        v[1][  1] = '\0';
        v[2][256] = '\0';

        zl = ziplistNew();
        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            zl = ziplistPush(zl, (unsigned char *) v[i], strlen(v[i]), ZIPLIST_TAIL);
        }

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);
        assert(e[2].prevrawlensize == 1);

        /* Deleting entry 1 will increase `prevrawlensize` for entry 2 */
        unsigned char *p = e[1].p;
        zl = ziplistDelete(zl, &p);

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);

        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Create long list and check indices:\n");
    {
        unsigned long long start = usec();
        zl = ziplistNew();
        char buf[32];
        int i,len;
        for (i = 0; i < 1000; i++) {
            len = sprintf(buf,"%d",i);
            zl = ziplistPush(zl,(unsigned char*)buf,len,ZIPLIST_TAIL);
        }
        for (i = 0; i < 1000; i++) {
            p = ziplistIndex(zl,i);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(i == value);

            p = ziplistIndex(zl,-i-1);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(999-i == value);
        }
        printf("SUCCESS. usec=%lld\n\n", usec()-start);
        zfree(zl);
    }

    printf("Compare strings with ziplist entries:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Merge test:\n");
    {
        /* create list gives us: [hello, foo, quux, 1024] */
        zl = createList();
        unsigned char *zl2 = createList();

        unsigned char *zl3 = ziplistNew();
        unsigned char *zl4 = ziplistNew();

        if (ziplistMerge(&zl4, &zl4)) {
            printf("ERROR: Allowed merging of one ziplist into itself.\n");
            return 1;
        }

        /* Merge two empty ziplists, get empty result back. */
        zl4 = ziplistMerge(&zl3, &zl4);
        ziplistRepr(zl4);
        if (ziplistLen(zl4)) {
            printf("ERROR: Merging two empty ziplists created entries.\n");
            return 1;
        }
        zfree(zl4);

        zl2 = ziplistMerge(&zl, &zl2);
        /* merge gives us: [hello, foo, quux, 1024, hello, foo, quux, 1024] */
        ziplistRepr(zl2);

        if (ziplistLen(zl2) != 8) {
            printf("ERROR: Merged length not 8, but: %u\n", ziplistLen(zl2));
            return 1;
        }

        p = ziplistIndex(zl2,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,4);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,7);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Stress with random payloads of different encoding:\n");
    {
        unsigned long long start = usec();
        int i,j,len,where;
        unsigned char *p;
        char buf[1024];
        int buflen;
        list *ref;
        listNode *refnode;

        /* Hold temp vars from ziplist */
        unsigned char *sstr;
        unsigned int slen;
        long long sval;

        iteration = accurate ? 20000 : 20;
        for (i = 0; i < iteration; i++) {
            zl = ziplistNew();
            ref = listCreate();
            listSetFreeMethod(ref,(void (*)(void*))sdsfree);
            len = rand() % 256;

            /* Create lists */
            for (j = 0; j < len; j++) {
                where = (rand() & 1) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
                if (rand() % 2) {
                    buflen = randstring(buf,1,sizeof(buf)-1);
                } else {
                    switch(rand() % 3) {
                    case 0:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) >> 20);
                        break;
                    case 1:
                        buflen = sprintf(buf,"%lld",(0LL + rand()));
                        break;
                    case 2:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) << 20);
                        break;
                    default:
                        assert(NULL);
                    }
                }

                /* Add to ziplist */
                zl = ziplistPush(zl, (unsigned char*)buf, buflen, where);

                /* Add to reference list */
                if (where == ZIPLIST_HEAD) {
                    listAddNodeHead(ref,sdsnewlen(buf, buflen));
                } else if (where == ZIPLIST_TAIL) {
                    listAddNodeTail(ref,sdsnewlen(buf, buflen));
                } else {
                    assert(NULL);
                }
            }

            assert(listLength(ref) == ziplistLen(zl));
            for (j = 0; j < len; j++) {
                /* Naive way to get elements, but similar to the stresser
                 * executed from the Tcl test suite. */
                p = ziplistIndex(zl,j);
                refnode = listIndex(ref,j);

                assert(ziplistGet(p,&sstr,&slen,&sval));
                if (sstr == NULL) {
                    buflen = sprintf(buf,"%lld",sval);
                } else {
                    buflen = slen;
                    memcpy(buf,sstr,buflen);
                    buf[buflen] = '\0';
                }
                assert(memcmp(buf,listNodeValue(refnode),buflen) == 0);
            }
            zfree(zl);
            listRelease(ref);
        }
        printf("Done. usec=%lld\n\n", usec()-start);
    }

    printf("Stress with variable ziplist size:\n");
    {
        unsigned long long start = usec();
        int maxsize = accurate ? 16384 : 16;
        stress(ZIPLIST_HEAD,100000,maxsize,256);
        stress(ZIPLIST_TAIL,100000,maxsize,256);
        printf("Done. usec=%lld\n\n", usec()-start);
    }

    /* Benchmarks */
    {
        zl = ziplistNew();
        iteration = accurate ? 100000 : 100;
        for (int i=0; i<iteration; i++) {
            char buf[4096] = "asdf";
            zl = ziplistPush(zl, (unsigned char*)buf, 4, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 40, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 400, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)buf, 4000, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"1", 1, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"10", 2, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"100", 3, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"1000", 4, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"10000", 5, ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)"100000", 6, ZIPLIST_TAIL);
        }

        printf("Benchmark ziplistFind:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                unsigned char *fptr = ziplistIndex(zl, ZIPLIST_HEAD);
                fptr = ziplistFind(zl, fptr, (unsigned char*)"nothing", 7, 1);
            }
            printf("%lld\n", usec()-start);
        }

        printf("Benchmark ziplistIndex:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                ziplistIndex(zl, 99999);
            }
            printf("%lld\n", usec()-start);
        }

        printf("Benchmark ziplistValidateIntegrity:\n");
        {
            unsigned long long start = usec();
            for (int i = 0; i < 2000; i++) {
                ziplistValidateIntegrity(zl, ziplistBlobLen(zl), 1, NULL, NULL);
            }
            printf("%lld\n", usec()-start);
        }

        zfree(zl);
    }

    printf("Stress __ziplistCascadeUpdate:\n");
    {
        char data[ZIP_BIG_PREVLEN];
        zl = ziplistNew();
        iteration = accurate ? 100000 : 100;
        for (int i = 0; i < iteration; i++) {
            zl = ziplistPush(zl, (unsigned char*)data, ZIP_BIG_PREVLEN-4, ZIPLIST_TAIL);
        }
        unsigned long long start = usec();
        zl = ziplistPush(zl, (unsigned char*)data, ZIP_BIG_PREVLEN-3, ZIPLIST_HEAD);
        printf("Done. usec=%lld\n\n", usec()-start);
        zfree(zl);
    }

    printf("Edge cases of __ziplistCascadeUpdate:\n");
    {
        /* Inserting a entry with data length greater than ZIP_BIG_PREVLEN-4 
         * will leads to cascade update. */
        size_t s1 = ZIP_BIG_PREVLEN-4, s2 = ZIP_BIG_PREVLEN-3;
        zl = ziplistNew();

        zlentry e[4] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0,
                         .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};

        zl = insertHelper(zl, 'a', s1, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'a', s1, 0));
        ziplistRepr(zl);

        /* No expand. */
        zl = insertHelper(zl, 'b', s1, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'b', s1, 0));

        assert(e[1].prevrawlensize == 1 && e[1].prevrawlen == strEntryBytesSmall(s1));
        assert(compareHelper(zl, 'a', s1, 1));

        ziplistRepr(zl);

        /* Expand(tail included). */
        zl = insertHelper(zl, 'c', s2, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'c', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'b', s1, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s1));
        assert(compareHelper(zl, 'a', s1, 2));

        ziplistRepr(zl);

        /* Expand(only previous head entry). */
        zl = insertHelper(zl, 'd', s2, ZIPLIST_ENTRY_HEAD(zl));
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'd', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'c', s2, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s2));
        assert(compareHelper(zl, 'b', s1, 2));

        assert(e[3].prevrawlensize == 5 && e[3].prevrawlen == strEntryBytesLarge(s1));
        assert(compareHelper(zl, 'a', s1, 3));

        ziplistRepr(zl);

        /* Delete from mid. */
        unsigned char *p = ziplistIndex(zl, 2);
        zl = ziplistDelete(zl, &p);
        verify(zl, e);

        assert(e[0].prevrawlensize == 1 && e[0].prevrawlen == 0);
        assert(compareHelper(zl, 'd', s2, 0));

        assert(e[1].prevrawlensize == 5 && e[1].prevrawlen == strEntryBytesSmall(s2));
        assert(compareHelper(zl, 'c', s2, 1));

        assert(e[2].prevrawlensize == 5 && e[2].prevrawlen == strEntryBytesLarge(s2));
        assert(compareHelper(zl, 'a', s1, 2));

        ziplistRepr(zl);

        zfree(zl);
    }

    return 0;
}
#endif
