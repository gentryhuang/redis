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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"
#include "redisassert.h"

/* Note that these encodings are ordered, so:
 * INTSET_ENC_INT16 < INTSET_ENC_INT32 < INTSET_ENC_INT64.
 *
 * intset 编码方式
 */
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

/* Return the required encoding for the provided value.
 *
 * 返回适用于传入值 v 的编码方式
 */
static uint8_t _intsetValueEncoding(int64_t v) {
    if (v < INT32_MIN || v > INT32_MAX)
        return INTSET_ENC_INT64;
    else if (v < INT16_MIN || v > INT16_MAX)
        return INTSET_ENC_INT32;
    else
        return INTSET_ENC_INT16;
}

/* Return the value at pos, given an encoding.
 *
 * 根据给定的编码方式 enc ，返回集合的底层数组在 pos 索引上的元素
 *
 * 索引通过地址运算
 *
 */
static int64_t _intsetGetEncoded(intset *is, int pos, uint8_t enc) {
    int64_t v64;
    int32_t v32;
    int16_t v16;

    /**
     * 1 根据编码方式将整数集合数组转换为对应类型 （编码方式是确定正确的数据类型）
     * 2 根据指定的 pos ，计算出元素在数组中的正确位置，并获取对应位置的值
     */
    if (enc == INTSET_ENC_INT64) {
        memcpy(&v64, ((int64_t *) is->contents) + pos, sizeof(v64));
        memrev64ifbe(&v64);
        return v64;
    } else if (enc == INTSET_ENC_INT32) {
        memcpy(&v32, ((int32_t *) is->contents) + pos, sizeof(v32));
        memrev32ifbe(&v32);
        return v32;
    } else {
        memcpy(&v16, ((int16_t *) is->contents) + pos, sizeof(v16));
        memrev16ifbe(&v16);
        return v16;
    }
}


/* Return the value at pos, using the configured encoding.
 *
 * 根据整数集合的编码方式，返回底层数组在 pos 索引上的值
 */
static int64_t _intsetGet(intset *is, int pos) {
    return _intsetGetEncoded(is, pos, intrev32ifbe(is->encoding));
}

/* Set the value at pos, using the configured encoding.
 *
 * 根据集合的编码方式，将底层数组在 pos 位置上的值设置为 value
 */
static void _intsetSet(intset *is, int pos, int64_t value) {

    // 1 取出集合的编码方式
    uint32_t encoding = intrev32ifbe(is->encoding);

    /**
     * 2 根据编码方式将数组转回正确的类型，然后将数据放入对位置。
     * 注意，如果有需要的话，memrevXxx 函数将对值进行大小类型转换
     */
    if (encoding == INTSET_ENC_INT64) {
        ((int64_t *) is->contents)[pos] = value;
        memrev64ifbe(((int64_t *) is->contents) + pos);
    } else if (encoding == INTSET_ENC_INT32) {
        ((int32_t *) is->contents)[pos] = value;
        memrev32ifbe(((int32_t *) is->contents) + pos);
    } else {
        ((int16_t *) is->contents)[pos] = value;
        memrev16ifbe(((int16_t *) is->contents) + pos);
    }
}

/* Create an empty intset.
 *
 * 创建并返回一个新的空整数集合
 *
 * T= O(1)
 *
 */
intset *intsetNew(void) {
    // 1 为整数集合结构分配空间
    intset *is = zmalloc(sizeof(intset));

    // 2 设置初始化编码方式 为 INTSET_ENC_INT16
    is->encoding = intrev32ifbe(INTSET_ENC_INT16);

    // 3 初始化元素数量
    is->length = 0;
    return is;
}

/* Resize the intset
 *
 * 调整整数集合的内存空间大小
 *
 * 返回调整大小后的整数集合
 *
 */
static intset *intsetResize(intset *is, uint32_t len) {
    // 计算数组的空间大小
    uint32_t size = len * intrev32ifbe(is->encoding);

    // 根据空间大小，重新分配空间
    // 注意，zrealloc 分配空间，如果新空间大小比原来的空间大小要大，那么数组原有的数据会被保留
    is = zrealloc(is, sizeof(intset) + size);

    // 返回调整大小后的整数集合
    return is;
}

/* Search for the position of "value". Return 1 when the value was found and
 * sets "pos" to the position of the value within the intset. Return 0 when
 * the value is not present in the intset and sets "pos" to the position
 * where "value" can be inserted.
 *
 * 在集合 is 的底层数组中查找值 value 所在的索引。
 *
 * 1）成功找到 value 时，函数返回 1 ，并将 *pos 的值设为 value 所在的索引。
 * 2）当在数组中没找到 value 时，返回 0 。并将 *pos 的值设为 value 可以插入到数组中的位置。
 *
 */
static uint8_t intsetSearch(intset *is, int64_t value, uint32_t *pos) {

    // 初始 min、max、mid
    int min = 0, max = intrev32ifbe(is->length) - 1, mid = -1;
    int64_t cur = -1;

    /* The value can never be found when the set is empty */
    // 处理 is 为空的情况
    if (intrev32ifbe(is->length) == 0) {
        if (pos) *pos = 0;
        return 0;

        // 不为空的情况
    } else {
        /* Check for the case where we know we cannot find the value,
         * but do know the insert position. */
        // 因为底层数组是有序的，如果 value 比数组中最后一个值都要大，那么 value 肯定不存在于集合中，应该将 value 添加到底层数组的最末端。
        if (value > _intsetGet(is, max)) {
            if (pos) *pos = intrev32ifbe(is->length);
            return 0;

            // 因为底层数组是有序的，如果 value 比数组中最前一个值都要小，那么 value 肯定不存在于集合中，应该将它添加到底层数组的最前端
        } else if (value < _intsetGet(is, 0)) {
            if (pos) *pos = 0;
            return 0;
        }
    }

    // 执行到这里说明，不满足极限值

    // 在有序数组中进行二分查找
    while (max >= min) {
        mid = ((unsigned int) min + (unsigned int) max) >> 1;
        cur = _intsetGet(is, mid);
        if (value > cur) {
            min = mid + 1;
        } else if (value < cur) {
            max = mid - 1;
        } else {
            break;
        }
    }

    // 检查通过二分查找是否已经找到了 value
    if (value == cur) {
        if (pos) *pos = mid;
        return 1;
    } else {
        if (pos) *pos = min;
        return 0;
    }
}

/* Upgrades the intset to a larger encoding and inserts the given integer.
 *
 * todo 根据值 value 所使用的编码类型，对整数集合的编码进行升级，并将值 value 添加到升级后的整数集合中。
 *
 * 注意，执行该函数的上层会判断，只有 value 的编码比集合原有的编码大，才会执行该函数，
 *
 */
static intset *intsetUpgradeAndAdd(intset *is, int64_t value) {
    // 1 获取整数集合当前编码方式
    uint8_t curenc = intrev32ifbe(is->encoding);

    // 2 获取新值所需的编码方式
    uint8_t newenc = _intsetValueEncoding(value);

    // 3 获取整数集合的元素数量
    int length = intrev32ifbe(is->length);

    // 4 根据 value 的值，决定是将它添加到底层数组的最前端还是最后端
    int prepend = value < 0 ? 1 : 0;

    /* First set new encoding and resize */
    // 5 更新集合的编码方式
    is->encoding = intrev32ifbe(newenc);

    // 6 根据新编码对集合进行空间调整（集合的编码方式变了，内存空间肯定也变）
    is = intsetResize(is, intrev32ifbe(is->length) + 1);

    /* Upgrade back-to-front so we don't overwrite values.
     * Note that the "prepend" variable is used to make sure we have an empty
     * space at either the beginning or the end of the intset. */
    /**
     * 7 将原编码数组的元素重新放置到新编码数组中（因为数组类型变了，重新放置是为了对原数据类型进行转换）
     * 1）根据整数集合原来的编码方式，依次从后向前取出集合中的元素。
     * 2）将元素以新编码的方式重新放入对应的位置
     * 注意：
     * 1）完成以上步骤后，集合中所有原有的元素就完成了从旧编码到新编码的转换。
     * 2）从后向前取出元素，是因为新分配的空间都放在数组的后端，所以程序先从后端向前端移动元素
     */
    while (length--)
        _intsetSet(is, length + prepend, _intsetGetEncoded(is, length, curenc));

    /* Set the value at the beginning or the end. */
    // 8 根据 prepend 的值，决定是添加到数组头还是数组尾
    if (prepend)
        _intsetSet(is, 0, value);
    else
        _intsetSet(is, intrev32ifbe(is->length), value);

    // 9 更新整数集合的元素数量
    is->length = intrev32ifbe(intrev32ifbe(is->length) + 1);
    return is;
}

/**
 * 向前或向后移动指定索引范围内的数组元素
 *
 * 1 在添加新元素到数组时，就需要进行向后移动
 * 如果数组表示如下（？表示一个未设置新值的空间）：
 * | x | y | z | ? |
 *     |<----->|
 * 而新元素 n 的 pos 为 1 ，那么数组将移动 y 和 z 两个元素
 * | x | y | y | z |
 *         |<----->|
 * 接着就可以将新元素 n 设置到 pos 上了：
 * | x | n | y | z |
 *
 * 2 当从数组中删除元素时，就需要进行向前移动
 * 如果数组表示如下，并且 b 为要删除的目标：
 * | a | b | c | d |
 *         |<----->|
 * 那么程序就会移动 b 后的所有元素向前一个元素的位置，
 * 从而覆盖 b 的数据：
 * | a | c | d | d |
 *     |<----->|
 * 最后，程序再从数组末尾删除一个元素的空间：
 * | a | c | d |
 * 这样就完成了删除操作。
 *
 * @param is
 * @param from
 * @param to
 */
static void intsetMoveTail(intset *is, uint32_t from, uint32_t to) {
    void *src, *dst;

    // 1 要移动的元素个数
    uint32_t bytes = intrev32ifbe(is->length) - from;

    // 2 集合的编码方式
    uint32_t encoding = intrev32ifbe(is->encoding);

    // 3 根据不同的编码，对整数数组进行操作，计算移动元素的开始和结束的位置，并累加共需要移动多少字节
    if (encoding == INTSET_ENC_INT64) {
        src = (int64_t *) is->contents + from;
        dst = (int64_t *) is->contents + to;
        bytes *= sizeof(int64_t);
    } else if (encoding == INTSET_ENC_INT32) {
        src = (int32_t *) is->contents + from;
        dst = (int32_t *) is->contents + to;
        bytes *= sizeof(int32_t);
    } else {
        src = (int16_t *) is->contents + from;
        dst = (int16_t *) is->contents + to;
        bytes *= sizeof(int16_t);
    }

    // 4 进行移动
    memmove(dst, src, bytes);
}

/* Insert an integer in the intset
 *
 * 尝试将元素 value 添加到整数集合中
 *
 * *success 的值指示添加是否成功：
 * - 如果添加成功，那么将 *success 的值设为 1 。
 * - 因为元素已存在而造成添加失败时，将 *success 的值设为 0 。
 */
intset *intsetAdd(intset *is, int64_t value, uint8_t *success) {
    // 1 获取 value 的编码方式
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;

    // 2 默认设置插入成功
    if (success) *success = 1;

    /* Upgrade encoding if necessary. If we need to upgrade, we know that
     * this value should be either appended (if > 0) or prepended (if < 0),
     * because it lies outside the range of existing values. */
    // 3 如果 value 的编码比整数集合现在的编码大，说明：
    // 3.1 表示 value 必然可以添加到整数集合中（因为编码要大，所集合中肯定不会有相同的元素）
    // 3.2 则需要对整数集合进行升级才能满足 value 所需的编码
    if (valenc > intrev32ifbe(is->encoding)) {
        /* This always succeeds, so we don't need to curry *success. */
        return intsetUpgradeAndAdd(is, value);

        // 4 运行到这里，表示整数集合现有的编码方式适用于 value
    } else {

        /* Abort if the value is already present in the set.
         * This call will populate "pos" with the right position to insert
         * the value when it cannot be found. */
        // 4.1  判断 value 是否在集合中存在
        // 如果存在，那么将 *success 设置为 0 ，并返回未经改动的整数集合。
        // 如果不存在，那么可以将 value 放入到 pos 指针位置
        if (intsetSearch(is, value, &pos)) {
            if (success) *success = 0;
            return is;
        }

        // 4.2 为 value 在集合中分配空间
        is = intsetResize(is, intrev32ifbe(is->length) + 1);

        // 4.3 如果新元素不是被添加到底层数组的末尾，那么需要对现有元素的数据进行移动，空出 pos 上的位置，用于设置新值。
        if (pos < intrev32ifbe(is->length)) intsetMoveTail(is, pos, pos + 1);
    }

    // 4.4 将新值设置到底层数组的 pos 位置
    _intsetSet(is, pos, value);

    /// 5 增加集合元素数量
    is->length = intrev32ifbe(intrev32ifbe(is->length) + 1);
    return is;
}

/* Delete integer from intset
 *
 * 从整数集合中删除值 value ,
 * *success 的值指示删除是否成功：
 * - 因值不存在而造成删除失败时该值为 0 。
 * - 删除成功时该值为 1 。
 *
 */
intset *intsetRemove(intset *is, int64_t value, int *success) {
    // 1 计算 value 的编码方式
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;

    // 2 默认设置标识值为删除失败
    if (success) *success = 0;

    // 3 当 value 的编码大小 <= 整数集合当前编码方式，才说明 value 有可能存在于集合
    // 且 通过查找确实存在集合中
    if (valenc <= intrev32ifbe(is->encoding) && intsetSearch(is, value, &pos)) {

        // 3.1 获取整数集合当前的元素数量
        uint32_t len = intrev32ifbe(is->length);

        /* We know we can delete */
        // 3.2 可以删除
        if (success) *success = 1;

        /* Overwrite value with tail and update length */
        // 3.3 如果要删除的 value 不是位于数组的末端，那么需要对原本位于 value 之后的元素进行移动
        // 从后向前移动
        if (pos < (len - 1)) intsetMoveTail(is, pos + 1, pos);

        // 3.4 缩小数组的大小，数组末尾多出来的一个元素的空间将被移除。
        // todo 即移除被删除元素占用的空间
        is = intsetResize(is, len - 1);

        // 3.5 更新集合的元素数量
        is->length = intrev32ifbe(len - 1);
    }

    return is;
}

/* Determine whether a value belongs to this set
 *
 * 检查给定值 value 是否集合中的元素
 *
 * 是返回 1 ，不是返回 0
 *
 * T = O(log N)
 */
uint8_t intsetFind(intset *is, int64_t value) {
    // 1 计算 value 的编码
    uint8_t valenc = _intsetValueEncoding(value);

    // 2 如果 value 的编码大于集合的当前编码，那么 value 一定不存在于集合
    // 当 value 的编码小于等于集合的当前编码时，需要进行查找
    return valenc <= intrev32ifbe(is->encoding) && intsetSearch(is, value, NULL);
}

/* Return random member
 *  从整数集合中随机返回一个元素，只能在集合非空时使用
 */
int64_t intsetRandom(intset *is) {
    uint32_t len = intrev32ifbe(is->length);
    assert(len); /* avoid division by zero on corrupt intset payload. */
    return _intsetGet(is, rand() % len);
}

/* Get the value at the given position. When this position is
 * out of range the function returns 0, when in range it returns 1.
 *
 * 取出集合底层数组指定位置中的值，并将它保存到 value 指针中。
 *
 * 如果 pos 没超出数组的索引范围，那么返回 1 ，如果超出索引，那么返回 0 。
 * 注意，不是使用下标，不会发生越界
 */
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value) {

    // 是否符合数组的范围
    if (pos < intrev32ifbe(is->length)) {

        // 保存值到指针
        *value = _intsetGet(is, pos);

        // 返回成功指示值
        return 1;
    }

    // 超出索引范围
    return 0;
}

/* Return intset length
 *
 *  返回整数集合现有的元素个数
 */
uint32_t intsetLen(const intset *is) {
    return intrev32ifbe(is->length);
}

/* Return intset blob size in bytes.
 *
 *  返回整数集合现在占用的字节总数量，
 *  这个数量包括整数集合的结构大小，以及整数集合所有元素的总大小
 */
size_t intsetBlobLen(intset *is) {
    return sizeof(intset) + (size_t) intrev32ifbe(is->length) * intrev32ifbe(is->encoding);
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we make sure there are no duplicate or out of order records. */
int intsetValidateIntegrity(const unsigned char *p, size_t size, int deep) {
    intset *is = (intset *) p;
    /* check that we can actually read the header. */
    if (size < sizeof(*is))
        return 0;

    uint32_t encoding = intrev32ifbe(is->encoding);

    size_t record_size;
    if (encoding == INTSET_ENC_INT64) {
        record_size = INTSET_ENC_INT64;
    } else if (encoding == INTSET_ENC_INT32) {
        record_size = INTSET_ENC_INT32;
    } else if (encoding == INTSET_ENC_INT16) {
        record_size = INTSET_ENC_INT16;
    } else {
        return 0;
    }

    /* check that the size matchies (all records are inside the buffer). */
    uint32_t count = intrev32ifbe(is->length);
    if (sizeof(*is) + count * record_size != size)
        return 0;

    /* check that the set is not empty. */
    if (count == 0)
        return 0;

    if (!deep)
        return 1;

    /* check that there are no dup or out of order records. */
    int64_t prev = _intsetGet(is, 0);
    for (uint32_t i = 1; i < count; i++) {
        int64_t cur = _intsetGet(is, i);
        if (cur <= prev)
            return 0;
        prev = cur;
    }

    return 1;
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include <time.h>

#if 0
static void intsetRepr(intset *is) {
    for (uint32_t i = 0; i < intrev32ifbe(is->length); i++) {
        printf("%lld\n", (uint64_t)_intsetGet(is,i));
    }
    printf("\n");
}

static void error(char *err) {
    printf("%s\n", err);
    exit(1);
}
#endif

static void ok(void) {
    printf("OK\n");
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static intset *createSet(int bits, int size) {
    uint64_t mask = (1<<bits)-1;
    uint64_t value;
    intset *is = intsetNew();

    for (int i = 0; i < size; i++) {
        if (bits > 32) {
            value = (rand()*rand()) & mask;
        } else {
            value = rand() & mask;
        }
        is = intsetAdd(is,value,NULL);
    }
    return is;
}

static void checkConsistency(intset *is) {
    for (uint32_t i = 0; i < (intrev32ifbe(is->length)-1); i++) {
        uint32_t encoding = intrev32ifbe(is->encoding);

        if (encoding == INTSET_ENC_INT16) {
            int16_t *i16 = (int16_t*)is->contents;
            assert(i16[i] < i16[i+1]);
        } else if (encoding == INTSET_ENC_INT32) {
            int32_t *i32 = (int32_t*)is->contents;
            assert(i32[i] < i32[i+1]);
        } else {
            int64_t *i64 = (int64_t*)is->contents;
            assert(i64[i] < i64[i+1]);
        }
    }
}

#define UNUSED(x) (void)(x)
int intsetTest(int argc, char **argv, int accurate) {
    uint8_t success;
    int i;
    intset *is;
    srand(time(NULL));

    UNUSED(argc);
    UNUSED(argv);
    UNUSED(accurate);

    printf("Value encodings: "); {
        assert(_intsetValueEncoding(-32768) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(+32767) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(-32769) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+32768) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483648) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+2147483647) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483649) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+2147483648) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(-9223372036854775808ull) ==
                    INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+9223372036854775807ull) ==
                    INTSET_ENC_INT64);
        ok();
    }

    printf("Basic adding: "); {
        is = intsetNew();
        is = intsetAdd(is,5,&success); assert(success);
        is = intsetAdd(is,6,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(!success);
        ok();
        zfree(is);
    }

    printf("Large number of random adds: "); {
        uint32_t inserts = 0;
        is = intsetNew();
        for (i = 0; i < 1024; i++) {
            is = intsetAdd(is,rand()%0x800,&success);
            if (success) inserts++;
        }
        assert(intrev32ifbe(is->length) == inserts);
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int16 to int32: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,65535));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-65535));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int16 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Upgrade from int32 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);
        zfree(is);

        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
        zfree(is);
    }

    printf("Stress lookups: "); {
        long num = 100000, size = 10000;
        int i, bits = 20;
        long long start;
        is = createSet(bits,size);
        checkConsistency(is);

        start = usec();
        for (i = 0; i < num; i++) intsetSearch(is,rand() % ((1<<bits)-1),NULL);
        printf("%ld lookups, %ld element set, %lldusec\n",
               num,size,usec()-start);
        zfree(is);
    }

    printf("Stress add+delete: "); {
        int i, v1, v2;
        is = intsetNew();
        for (i = 0; i < 0xffff; i++) {
            v1 = rand() % 0xfff;
            is = intsetAdd(is,v1,NULL);
            assert(intsetFind(is,v1));

            v2 = rand() % 0xfff;
            is = intsetRemove(is,v2,NULL);
            assert(!intsetFind(is,v2));
        }
        checkConsistency(is);
        ok();
        zfree(is);
    }

    return 0;
}
#endif
