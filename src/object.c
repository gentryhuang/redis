/* Redis Object implementation.
 *
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

#include "server.h"
#include <math.h>
#include <ctype.h>

#ifdef __CYGWIN__
#define strtold(a,b) ((long double)strtod((a),(b)))
#endif

/** ===================== Creation and parsing of objects ==================== */
/**
 * 通用的创建 Object 的方法，适用于各种数据类型。调用方根据各自对象的特点，覆盖对应的属性即可。
 *
 * @param type 对象的类型 -> 数据类型
 * @param ptr 指向数据对象的指针
 * @return
 */
robj *createObject(int type, void *ptr) {
    // 给 redisObject 结构体分配空间
    robj *o = zmalloc(sizeof(*o));

    // 设置redisObject的类型
    o->type = type;

    // 设置redisObject的编码类型，此处是OBJ_ENCODING_RAW，表示常规的SDS。
    // 注意，这里是默认的，不同数据类型可选择覆盖
    o->encoding = OBJ_ENCODING_RAW;

    //直接将传入的指针赋值给redisObject中的指针。
    o->ptr = ptr;
    o->refcount = 1;

    /* Set the LRU to the current lruclock (minutes resolution), or
     * alternatively the LFU counter.
     *
     * lru: 当内存淘汰策略是 LFU 时，该属性为 LFU 计数器。
     */
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        o->lru = (LFUGetTimeInMinutes() << 8) | LFU_INIT_VAL;
    } else {
        o->lru = LRU_CLOCK();
    }
    return o;
}

/* Set a special refcount in the object to make it "shared":
 * incrRefCount and decrRefCount() will test for this special refcount
 * and will not touch the object. This way it is free to access shared
 * objects such as small integers from different threads without any
 * mutex.
 *
 * A common patter to create shared objects:
 *
 * robj *myobject = makeObjectShared(createObject(...));
 *
 */
robj *makeObjectShared(robj *o) {
    serverAssert(o->refcount == 1);
    o->refcount = OBJ_SHARED_REFCOUNT;
    return o;
}


/** ===================== Creation and parsing of String objects ==================== */

/* Create a string object with encoding OBJ_ENCODING_RAW, that is a plain
 * string object where o->ptr points to a proper sds string. */
/**
 * 创建一个 REDIS_ENCODING_RAW 编码的字符串对象，该对象的指针指向一个 sds 结构地址。
 * 说明：
 * 在创建普通字符串时，Redis 需要分别给 redisObject 和 SDS 分别分配一次内存，这样就既带来了内存分配开销，同时也会导致内存碎片。
 * 因此，当字符串小于等于 44 字节时，Redis 就使用了嵌入式字符串的创建方法，以此减少内存分配和内存碎片。
 * @param ptr
 * @param len
 * @return
 */
robj *createRawStringObject(const char *ptr, size_t len) {
    // 对象类型：OBJ_STRING
    // 指向 SDS 结构的指针，由 sdsnewlen 函数返回，本质是指向字符串数组的指针
    return createObject(OBJ_STRING, sdsnewlen(ptr, len));
}

/* Create a string object with encoding OBJ_ENCODING_EMBSTR, that is
 * an object where the sds string is actually an unmodifiable string
 * allocated in the same chunk as the object itself. */
/**
 * 创建一个 REDIS_ENCODING_EMBSTR 编码的字符串对象，该字符串对象中的 SDS 字符串实际上是一个不可修改的字符串，因为它和对象本身分配在同一个块中。
 * 特别说明：
 *  Redis 会通过设计实现一块连续的内存空间，把 redisObject 结构体和 SDS 结构体紧凑地放置在一起。这样一来，对于不超过 44 字节的字符串来说，就可以
 *  避免内存碎片和两次内存分配的开销了。
 * @param ptr
 * @param len
 * @return
 */
robj *createEmbeddedStringObject(const char *ptr, size_t len) {

    // 1 分配一块连续的内存空间，大小如下：
    // redisObject 结构体大小 + SDS结构头 sdshdr8 的大小 + 字符串大小的总和+ 1 字节结束符号"\0"
    // 注意，分配 redisObject 和 SDS 使用的是一块连续的内存空间
    robj *o = zmalloc(sizeof(robj) + sizeof(struct sdshdr8) + len + 1);

    // 2 创建 SDS 结构，并把 sh 指向这块连续空间中 SDS 结构头所在位置
    // 其中，o 是 redisObject 结构体的变量，o+1 表示将内存地址从变量 o 开始移动一段距离，而移动的距离等于 redisObject 这个结构体的大小（o+1:表示累加一个单位的 o）。
    struct sdshdr8 *sh = (void *) (o + 1);

    o->type = OBJ_STRING;
    o->encoding = OBJ_ENCODING_EMBSTR;

    // 3 将 redisObject 中的指针 ptr 指向 SDS 结构中的字符数组
    // sh 是刚才介绍的指向 SDS 结构的指针，属于 sdshdr8 类型。而 sh+1 表示把内存地址从 sh 起始地址开始移动一定的大小，移动的距离等于 sdshdr8 结构体的大小。
    o->ptr = sh + 1;

    o->refcount = 1;
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        o->lru = (LFUGetTimeInMinutes() << 8) | LFU_INIT_VAL;
    } else {
        o->lru = LRU_CLOCK();
    }

    sh->len = len;
    sh->alloc = len;
    sh->flags = SDS_TYPE_8;

    // 4 将传入的指针 ptr 指向的字符串拷贝到 SDS 结构体中的字符数组中，并在数组最后添加结束符号
    if (ptr == SDS_NOINIT)
        sh->buf[len] = '\0';
    else if (ptr) {
        memcpy(sh->buf, ptr, len);
        sh->buf[len] = '\0';
    } else {
        memset(sh->buf, 0, len + 1);
    }
    return o;
}

/* Create a string object with EMBSTR encoding if it is smaller than
 * OBJ_ENCODING_EMBSTR_SIZE_LIMIT, otherwise the RAW encoding is
 * used.
 *
 * The current limit of 44 is chosen so that the biggest string object
 * we allocate as EMBSTR will still fit into the 64 byte arena of jemalloc. */
/**
 * 如果小于 OBJ_ENCODING_EMBSTR_SIZE_LIMIT ，则创建一个 EMBSTR 编码的字符串对象，否则使用 RAW 编码。
 * 说明：选择 OBJ_ENCODING_EMBSTR_SIZE_LIMIT 的限制是为了让我们分配给 EMBSTR 的最大字符串对象仍然可以容纳 64 字节（使用 EMBSTR 编码使用的 sdshdr 为 sdshdr8，jemalloc 分配内存为 64字节）。
 * 解释如下：
 * redis 3.2及其以后的版本sds会根据实际使用的buf长度，采用不同的sds对象表示，这里只说下小于等64字节的sds对象结构：
     - len：表示buf的使用长度，1字节。
     - alloc：表示分配给buf的总长度，1字节。
     - flag：表示具体的sds类型，1个字节。
     - buf：真正存储数据的地方，肯定有1字节的‘\0’表示结束符。
    如果想让redis3.2以后的版本使用embstr编码的string需要buf的最大值为： 64-16（redisObjct除去ptr后使用的内存）-（1+1+1+1）（sds最小用的内存）= 44字节。
 */
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44

/**
 * 创建字符串对象
 * @param ptr 字符串 ptr
 * @param len 字符串长度
 * @return
 */
robj *createStringObject(const char *ptr, size_t len) {

    // 当 len 长度小于等于 44 字节，创建 EMBSTR 编码的字符串对象（SDS)
    // 即创建 嵌入式字符串，字符串长度小于等于 44 字节
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr, len);

        // 创建 RAW 编码的字符串对象(SDS)，即普通字符串，字符串长度大于44字节
    else
        return createRawStringObject(ptr, len);
}

/* Create a string object from a long long value. When possible returns a
 * shared integer object, or at least an integer encoded one.
 * 从 long long 值创建一个字符串对象。在可能的情况下返回一个共享整数对象，或者至少是一个经过编码的整数。
 *
 * If valueobj is non zero, the function avoids returning a shared
 * integer, because the object is going to be used as value in the Redis key
 * space (for instance when the INCR command is used), so we want LFU/LRU
 * values specific for each key.
 *
 * valueobj != 0 ,不会创建一个共享整数对象。
 *
 * */
/**
 * 根据传入的 long long 值创建一个字符串对象：
 * 1 该字符串的对象保存的可以是 INT 编码的 long 值
 * 2 可以是 RAW 编码的、被转换成字符串的 long long 值。
 * @param value
 * @param valueobj
 * @return
 */
robj *createStringObjectFromLongLongWithOptions(long long value, int valueobj) {
    robj *o;

    if (server.maxmemory == 0 ||
        !(server.maxmemory_policy & MAXMEMORY_FLAG_NO_SHARED_INTEGERS)) {
        /* If the maxmemory policy permits, we can still return shared integers
         * even if valueobj is true. */
        valueobj = 0;
    }

    // value 的大小符合 REDIS 共享整数的范围： 0<= value <= 9999  && valueobj == 0，则返回一个共享对象
    if (value >= 0 && value < OBJ_SHARED_INTEGERS && valueobj == 0) {
        incrRefCount(shared.integers[value]);
        o = shared.integers[value];

        // 不符合共享范围，创建一个新的整数对象
    } else {
        // 值的大小符合 long 类型，则创建一个 OBJ_ENCODING_INT 编码的字符串
        if (value >= LONG_MIN && value <= LONG_MAX) {
            // 创建基础对象
            o = createObject(OBJ_STRING, NULL);
            // 指定编码方式
            o->encoding = OBJ_ENCODING_INT;
            // 设置数据指针的值为 value
            o->ptr = (void *) ((long) value);

            // 值的大小不符合 long 类型，需要将值转为字符串
        } else {
            o = createObject(OBJ_STRING, sdsfromlonglong(value));
        }
    }
    return o;
}

/* Wrapper for createStringObjectFromLongLongWithOptions() always demanding
 * to create a shared object if possible.
 * 根据传入的 long long 数据，尽可能创建一个共享对象
 *
 **/
robj *createStringObjectFromLongLong(long long value) {
    return createStringObjectFromLongLongWithOptions(value, 0);
}

/* Wrapper for createStringObjectFromLongLongWithOptions() avoiding a shared
 * object when LFU/LRU info are needed, that is, when the object is used
 * as a value in the key space, and Redis is configured to evict based on
 * LFU/LRU.
 * 避免创建共享对象
 **/
robj *createStringObjectFromLongLongForValue(long long value) {
    return createStringObjectFromLongLongWithOptions(value, 1);
}

/* Create a string object from a long double. If humanfriendly is non-zero
 * it does not use exponential format and trims trailing zeroes at the end,
 * however this results in loss of precision. Otherwise exp format is used
 * and the output of snprintf() is not modified.
 *
 * The 'humanfriendly' option is used for INCRBYFLOAT and HINCRBYFLOAT.
 *
 * 根据传入的 long double 值，为它创建一个字符串对象
 *
 * */
robj *createStringObjectFromLongDouble(long double value, int humanfriendly) {
    char buf[MAX_LONG_DOUBLE_CHARS];
    // 根据精度获取长度
    int len = ld2string(buf, sizeof(buf), value, humanfriendly ? LD_STR_HUMAN : LD_STR_AUTO);
    return createStringObject(buf, len);
}

/* Duplicate a string object, with the guarantee that the returned object
 * has the same encoding as the original one.
 * 复制一个字符串对象，复制出的对象和输入对象拥有相同编码。
 *
 * This function also guarantees that duplicating a small integer object
 * (or a string object that contains a representation of a small integer)
 * will always result in a fresh object that is unshared (refcount == 1).
 * 这个函数在复制一个包含整数值的字符串对象时，总是产生一个非共享的对象。
 *
 * The resulting object always has refcount set to 1.
 * 输出对象的 refcount 总为 1 。
 *
 **/
robj *dupStringObject(const robj *o) {
    robj *d;
    serverAssert(o->type == OBJ_STRING);
    // 根据目标对象的编码，创建对应编码的对象
    switch (o->encoding) {
        case OBJ_ENCODING_RAW:
            return createRawStringObject(o->ptr, sdslen(o->ptr));
        case OBJ_ENCODING_EMBSTR:
            return createEmbeddedStringObject(o->ptr, sdslen(o->ptr));
        case OBJ_ENCODING_INT:
            d = createObject(OBJ_STRING, NULL);
            d->encoding = OBJ_ENCODING_INT;
            d->ptr = o->ptr;
            return d;
        default:
            serverPanic("Wrong encoding.");
            break;
    }
}

/** ===================== Creation and parsing of List objects ==================== */

/**
 * 创建一个 QUICKLIST 编码的列表对象
 * @return
 */
robj *createQuicklistObject(void) {
    // 创建 快速列表
    quicklist *l = quicklistCreate();

    // 创建 listObject 对象封装 快速列表
    robj *o = createObject(OBJ_LIST, l);
    o->encoding = OBJ_ENCODING_QUICKLIST;
    return o;
}

/**
 * 创建一个 ZIPLIST 编码的列表对象
 * @return
 */
robj *createZiplistObject(void) {
    // 创建 压缩列表
    unsigned char *zl = ziplistNew();

    // 创建 listObject 对象封装 压缩列表
    robj *o = createObject(OBJ_LIST, zl);
    o->encoding = OBJ_ENCODING_ZIPLIST;
    return o;
}



/** ===================== Creation and parsing of Set objects ==================== */

/**
 * 创建一个 HASHTABLE 编码的集合对象
 * @return
 */
robj *createSetObject(void) {
    // 创建字典
    dict *d = dictCreate(&setDictType, NULL);

    // 创建 setObject 对象封装字典
    robj *o = createObject(OBJ_SET, d);
    o->encoding = OBJ_ENCODING_HT;
    return o;
}

/**
 * 创建一个 INTSET 编码的集合对象
 * @return
 */
robj *createIntsetObject(void) {
    // 创建 整数集合
    intset *is = intsetNew();

    // 创建 SetObject 对象封装 整数集合
    robj *o = createObject(OBJ_SET, is);
    o->encoding = OBJ_ENCODING_INTSET;
    return o;
}



/** ===================== Creation and parsing of Hash objects ==================== */

/**
 * 创建一个 ZIPLIST 编码的哈希对象
 * @return
 */
robj *createHashObject(void) {
    // 创建 压缩列表
    unsigned char *zl = ziplistNew();

    // 创建 HashObject 对象封装 压缩列表
    robj *o = createObject(OBJ_HASH, zl);
    o->encoding = OBJ_ENCODING_ZIPLIST;
    return o;
}


/** ===================== Creation and parsing of ZSet objects ==================== */

/**
 * 创建一个 SKIPLIST 编码的有序集合
 * @return
 */
robj *createZsetObject(void) {
    // 1 分配 zset 结构空间
    zset *zs = zmalloc(sizeof(*zs));
    robj *o;

    // 2 创建字典
    zs->dict = dictCreate(&zsetDictType, NULL);
    // 3 创建跳表
    zs->zsl = zslCreate();

    // 4 创建 ZSetObject 对象封装 跳表
    o = createObject(OBJ_ZSET, zs);
    o->encoding = OBJ_ENCODING_SKIPLIST;
    return o;
}

/**
 * 创建一个 ZIPLIST 编码的有序集合
 * @return
 */
robj *createZsetZiplistObject(void) {
    // 创建一个压缩列表
    unsigned char *zl = ziplistNew();

    // 创建 ZSetObject 对象封装压缩列表
    robj *o = createObject(OBJ_ZSET, zl);
    o->encoding = OBJ_ENCODING_ZIPLIST;
    return o;
}


/** ===================== Creation and parsing of Stream objects ==================== */

/**
 * 创建一个 STREAM 编码的流对象
 * @return
 */
robj *createStreamObject(void) {
    // 创建流结构
    stream *s = streamNew();

    // 创建 StreamObject 对象封装 流结构
    robj *o = createObject(OBJ_STREAM, s);
    o->encoding = OBJ_ENCODING_STREAM;
    return o;
}


/** ===================== Creation and parsing of Module objects ==================== */
/**
 * 创建一个module对象
 * @param mt
 * @param value
 * @return
 */
robj *createModuleObject(moduleType *mt, void *value) {
    moduleValue *mv = zmalloc(sizeof(*mv));
    mv->type = mt;
    mv->value = value;
    return createObject(OBJ_MODULE, mv);
}


/** ===================== Free String objects ==================== */
void freeStringObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}


/** ===================== Free List objects ==================== */
void freeListObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistRelease(o->ptr);
    } else {
        serverPanic("Unknown list encoding type");
    }
}


/** ===================== Free Set objects ==================== */
void freeSetObject(robj *o) {
    switch (o->encoding) {
        case OBJ_ENCODING_HT:
            dictRelease((dict *) o->ptr);
            break;
        case OBJ_ENCODING_INTSET:
            zfree(o->ptr);
            break;
        default:
            serverPanic("Unknown set encoding type");
    }
}


/** ===================== Free ZSet objects ==================== */
void freeZsetObject(robj *o) {
    zset *zs;
    switch (o->encoding) {
        case OBJ_ENCODING_SKIPLIST:
            zs = o->ptr;
            dictRelease(zs->dict);
            zslFree(zs->zsl);
            zfree(zs);
            break;
        case OBJ_ENCODING_ZIPLIST:
            zfree(o->ptr);
            break;
        default:
            serverPanic("Unknown sorted set encoding");
    }
}

/** ===================== Free Hash objects ==================== */
void freeHashObject(robj *o) {
    switch (o->encoding) {
        case OBJ_ENCODING_HT:
            dictRelease((dict *) o->ptr);
            break;
        case OBJ_ENCODING_ZIPLIST:
            zfree(o->ptr);
            break;
        default:
            serverPanic("Unknown hash encoding type");
            break;
    }
}

/** ===================== Free Module objects ==================== */
void freeModuleObject(robj *o) {
    moduleValue *mv = o->ptr;
    mv->type->free(mv->value);
    zfree(mv);
}


/** ===================== Free Stream objects ==================== */
void freeStreamObject(robj *o) {
    freeStream(o->ptr);
}

/*
 * 为对象的引用数增一
 */
void incrRefCount(robj *o) {
    if (o->refcount < OBJ_FIRST_SPECIAL_REFCOUNT) {
        o->refcount++;
    } else {
        if (o->refcount == OBJ_SHARED_REFCOUNT) {
            /* Nothing to do: this refcount is immutable. */
        } else if (o->refcount == OBJ_STATIC_REFCOUNT) {
            serverPanic("You tried to retain an object allocated in the stack");
        }
    }
}

/*
 * 为对象的引用计数减一
 */
void decrRefCount(robj *o) {
    // 释放对象
    if (o->refcount == 1) {
        switch (o->type) {
            case OBJ_STRING:
                freeStringObject(o);
                break;
            case OBJ_LIST:
                freeListObject(o);
                break;
            case OBJ_SET:
                freeSetObject(o);
                break;
            case OBJ_ZSET:
                freeZsetObject(o);
                break;
            case OBJ_HASH:
                freeHashObject(o);
                break;
            case OBJ_MODULE:
                freeModuleObject(o);
                break;
            case OBJ_STREAM:
                freeStreamObject(o);
                break;
            default:
                serverPanic("Unknown object type");
                break;
        }
        zfree(o);

        // 减少计数
    } else {
        if (o->refcount <= 0) serverPanic("decrRefCount against refcount <= 0");
        if (o->refcount != OBJ_SHARED_REFCOUNT) o->refcount--;
    }
}

/* This variant of decrRefCount() gets its argument as void, and is useful
 * as free method in data structures that expect a 'void free_object(void*)'
 * prototype for the free method. */
void decrRefCountVoid(void *o) {
    decrRefCount(o);
}

/**
 * 命令执行之前，检查对象是否可以执行给定的命令。
 * 如：根据 key 获得是 string 类型的对象，但是执行的是 hash 类型的命令。那么这个 string 类型的对象就不能执行 hash 类型的命令。
 *
 * @param c 客户端
 * @param o 某个类型的对象
 * @param type 数据类型
 * @return
 */
int checkType(client *c, robj *o, int type) {
    /* A NULL is considered an empty key
     *
     * 类型不匹配，提示以下错误信息：
     *
     * -WRONGTYPE Operation against a key holding the wrong kind of value
     */
    if (o && o->type != type) {
        addReplyErrorObject(c, shared.wrongtypeerr);
        return 1;
    }
    return 0;
}

int isSdsRepresentableAsLongLong(sds s, long long *llval) {
    return string2ll(s, sdslen(s), llval) ? C_OK : C_ERR;
}

int isObjectRepresentableAsLongLong(robj *o, long long *llval) {
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
    if (o->encoding == OBJ_ENCODING_INT) {
        if (llval) *llval = (long) o->ptr;
        return C_OK;
    } else {
        return isSdsRepresentableAsLongLong(o->ptr, llval);
    }
}

/* Optimize the SDS string inside the string object to require little space,
 * in case there is more than 10% of free space at the end of the SDS
 * string. This happens because SDS strings tend to overallocate to avoid
 * wasting too much time in allocations when appending to the string.
 *
 * 优化字符串对象中的SDS字符串，以减少需要的空间，以防SDS字符串末尾有超过10%的空闲空间。发生这种情况是因为SDS字符串倾向于过度分配以避免
 * 当附加到字符串时，在分配中浪费了太多的时间。
 *
 */
void trimStringObjectIfNeeded(robj *o) {
    // 简单动态字符串 & buf 数组末尾超过 10%的空闲空间
    if (o->encoding == OBJ_ENCODING_RAW && sdsavail(o->ptr) > sdslen(o->ptr) / 10) {
        // 缩容
        o->ptr = sdsRemoveFreeSpace(o->ptr);
    }
}

/* Try to encode a string object in order to save space
 *
 * 尝试编码一个字符串对象，以节省空间
 */
robj *tryObjectEncoding(robj *o) {
    long value;
    sds s = o->ptr;
    size_t len;

    /* Make sure this is a string object, the only type we encode
     * in this function. Other types use encoded memory efficient
     * representations but are handled by the commands implementing
     * the type. */
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);

    /* We try some specialized encoding only for objects that are
     * RAW or EMBSTR encoded, in other words objects that are still
     * in represented by an actually array of chars. */
    // 只在字符串的编码为 RAW 或者 EMBSTR 时尝试进行编码
    if (!sdsEncodedObject(o)) return o;

    /* It's not safe to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of Redis and may end in places where
     * they are not handled. We handle them only as values in the keyspace. */
    // 不对共享对象进行编码
    if (o->refcount > 1) return o;

    /* Check if we can represent this string as a long integer.
     * Note that we are sure that a string larger than 20 chars is not
     * representable as a 32 nor 64 bit integer. */
    // 计算sds的实际长度
    len = sdslen(s);
    // 只对长度 <= 20 字节 && 可被解析为整数的字符串进行编码
    if (len <= 20 && string2l(s, len, &value)) {
        /* This object is encodable as a long. Try to use a shared object.
         * Note that we avoid using shared integers when maxmemory is used
         * because every object needs to have a private LRU field for the LRU
         * algorithm to work well. */
        if ((server.maxmemory == 0 ||
             !(server.maxmemory_policy & MAXMEMORY_FLAG_NO_SHARED_INTEGERS)) &&
            value >= 0 &&
            value < OBJ_SHARED_INTEGERS) {
            decrRefCount(o);
            incrRefCount(shared.integers[value]);
            return shared.integers[value];
        } else {
            if (o->encoding == OBJ_ENCODING_RAW) {
                sdsfree(o->ptr);
                o->encoding = OBJ_ENCODING_INT;
                o->ptr = (void *) value;
                return o;
            } else if (o->encoding == OBJ_ENCODING_EMBSTR) {
                decrRefCount(o);
                return createStringObjectFromLongLongForValue(value);
            }
        }
    }

    /* If the string is small and is still RAW encoded,
     * try the EMBSTR encoding which is more efficient.
     * In this representation the object and the SDS string are allocated
     * in the same chunk of memory to save space and cache misses. */
    // 尝试将 RAW 编码的字符串编码为 EMBSTR 编码
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT) {
        robj *emb;

        if (o->encoding == OBJ_ENCODING_EMBSTR) return o;
        emb = createEmbeddedStringObject(s, sdslen(s));
        decrRefCount(o);
        return emb;
    }

    /* We can't encode the object...
     *
     * Do the last try, and at least optimize the SDS string inside
     * the string object to require little space, in case there
     * is more than 10% of free space at the end of the SDS string.
     *
     * We do that only for relatively large strings as this branch
     * is only entered if the length of the string is greater than
     * OBJ_ENCODING_EMBSTR_SIZE_LIMIT. */
    // 这个对象没办法进行编码，尝试从 SDS 中移除所有空余空间
    trimStringObjectIfNeeded(o);

    /* Return the original object. */
    return o;
}

/* Get a decoded version of an encoded object (returned as a new object).
 * If the object is already raw-encoded just increment the ref count.
 *
 * 获取已编码对象的解码版本(作为新对象返回)。如果对象已经是原始编码（RAW 编码）的，只需增加ref计数。
 */
robj *getDecodedObject(robj *o) {
    robj *dec;

    if (sdsEncodedObject(o)) {
        incrRefCount(o);
        return o;
    }

    // 解码对象，将对象的值从整数转换为字符串
    if (o->type == OBJ_STRING && o->encoding == OBJ_ENCODING_INT) {
        char buf[32];

        ll2string(buf, 32, (long) o->ptr);
        dec = createStringObject(buf, strlen(buf));
        return dec;
    } else {
        serverPanic("Unknown encoding type");
    }
}

/* Compare two string objects via strcmp() or strcoll() depending on flags.
 * Note that the objects may be integer-encoded. In such a case we
 * use ll2string() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * Important note: when REDIS_COMPARE_BINARY is used a binary-safe comparison
 * is used. */

#define REDIS_COMPARE_BINARY (1<<0)
#define REDIS_COMPARE_COLL (1<<1)

int compareStringObjectsWithFlags(robj *a, robj *b, int flags) {
    serverAssertWithInfo(NULL, a, a->type == OBJ_STRING && b->type == OBJ_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    size_t alen, blen, minlen;

    if (a == b) return 0;
    if (sdsEncodedObject(a)) {
        astr = a->ptr;
        alen = sdslen(astr);
    } else {
        alen = ll2string(bufa, sizeof(bufa), (long) a->ptr);
        astr = bufa;
    }
    if (sdsEncodedObject(b)) {
        bstr = b->ptr;
        blen = sdslen(bstr);
    } else {
        blen = ll2string(bufb, sizeof(bufb), (long) b->ptr);
        bstr = bufb;
    }
    if (flags & REDIS_COMPARE_COLL) {
        return strcoll(astr, bstr);
    } else {
        int cmp;

        minlen = (alen < blen) ? alen : blen;
        cmp = memcmp(astr, bstr, minlen);
        if (cmp == 0) return alen - blen;
        return cmp;
    }
}

/* Wrapper for compareStringObjectsWithFlags() using binary comparison. */
int compareStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a, b, REDIS_COMPARE_BINARY);
}

/* Wrapper for compareStringObjectsWithFlags() using collation. */
int collateStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a, b, REDIS_COMPARE_COLL);
}

/* Equal string objects return 1 if the two objects are the same from the
 * point of view of a string comparison, otherwise 0 is returned. Note that
 * this function is faster then checking for (compareStringObject(a,b) == 0)
 * because it can perform some more optimization. */
int equalStringObjects(robj *a, robj *b) {
    if (a->encoding == OBJ_ENCODING_INT &&
        b->encoding == OBJ_ENCODING_INT) {
        /* If both strings are integer encoded just check if the stored
         * long is the same. */
        return a->ptr == b->ptr;
    } else {
        return compareStringObjects(a, b) == 0;
    }
}

size_t stringObjectLen(robj *o) {
    serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
    if (sdsEncodedObject(o)) {
        return sdslen(o->ptr);
    } else {
        return sdigits10((long) o->ptr);
    }
}

int getDoubleFromObject(const robj *o, double *target) {
    double value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (!string2d(o->ptr, sdslen(o->ptr), &value))
                return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long) o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    *target = value;
    return C_OK;
}

int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg) {
    double value;
    if (getDoubleFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c, (char *) msg);
        } else {
            addReplyError(c, "value is not a valid float");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

int getLongDoubleFromObject(robj *o, long double *target) {
    long double value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (!string2ld(o->ptr, sdslen(o->ptr), &value))
                return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long) o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    *target = value;
    return C_OK;
}

int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg) {
    long double value;
    if (getLongDoubleFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c, (char *) msg);
        } else {
            addReplyError(c, "value is not a valid float");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

int getLongLongFromObject(robj *o, long long *target) {
    long long value;

    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL, o, o->type == OBJ_STRING);
        if (sdsEncodedObject(o)) {
            if (string2ll(o->ptr, sdslen(o->ptr), &value) == 0) return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            value = (long) o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
    if (target) *target = value;
    return C_OK;
}

int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg) {
    long long value;
    if (getLongLongFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
            addReplyError(c, (char *) msg);
        } else {
            addReplyError(c, "value is not an integer or out of range");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg) {
    long long value;

    if (getLongLongFromObjectOrReply(c, o, &value, msg) != C_OK) return C_ERR;
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c, (char *) msg);
        } else {
            addReplyError(c, "value is out of range");
        }
        return C_ERR;
    }
    *target = value;
    return C_OK;
}

int getRangeLongFromObjectOrReply(client *c, robj *o, long min, long max, long *target, const char *msg) {
    if (getLongFromObjectOrReply(c, o, target, msg) != C_OK) return C_ERR;
    if (*target < min || *target > max) {
        if (msg != NULL) {
            addReplyError(c, (char *) msg);
        } else {
            addReplyErrorFormat(c, "value is out of range, value must between %ld and %ld", min, max);
        }
        return C_ERR;
    }
    return C_OK;
}

int getPositiveLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg) {
    if (msg) {
        return getRangeLongFromObjectOrReply(c, o, 0, LONG_MAX, target, msg);
    } else {
        return getRangeLongFromObjectOrReply(c, o, 0, LONG_MAX, target, "value is out of range, must be positive");
    }
}

int getIntFromObjectOrReply(client *c, robj *o, int *target, const char *msg) {
    long value;

    if (getRangeLongFromObjectOrReply(c, o, INT_MIN, INT_MAX, &value, msg) != C_OK)
        return C_ERR;

    *target = value;
    return C_OK;
}

char *strEncoding(int encoding) {
    switch (encoding) {
        case OBJ_ENCODING_RAW:
            return "raw";
        case OBJ_ENCODING_INT:
            return "int";
        case OBJ_ENCODING_HT:
            return "hashtable";
        case OBJ_ENCODING_QUICKLIST:
            return "quicklist";
        case OBJ_ENCODING_ZIPLIST:
            return "ziplist";
        case OBJ_ENCODING_INTSET:
            return "intset";
        case OBJ_ENCODING_SKIPLIST:
            return "skiplist";
        case OBJ_ENCODING_EMBSTR:
            return "embstr";
        case OBJ_ENCODING_STREAM:
            return "stream";
        default:
            return "unknown";
    }
}

/* =========================== Memory introspection ========================= */


/* This is an helper function with the goal of estimating the memory
 * size of a radix tree that is used to store Stream IDs.
 *
 * Note: to guess the size of the radix tree is not trivial, so we
 * approximate it considering 16 bytes of data overhead for each
 * key (the ID), and then adding the number of bare nodes, plus some
 * overhead due by the data and child pointers. This secret recipe
 * was obtained by checking the average radix tree created by real
 * workloads, and then adjusting the constants to get numbers that
 * more or less match the real memory usage.
 *
 * Actually the number of nodes and keys may be different depending
 * on the insertion speed and thus the ability of the radix tree
 * to compress prefixes. */
size_t streamRadixTreeMemoryUsage(rax *rax) {
    size_t size;
    size = rax->numele * sizeof(streamID);
    size += rax->numnodes * sizeof(raxNode);
    /* Add a fixed overhead due to the aux data pointer, children, ... */
    size += rax->numnodes * sizeof(long) * 30;
    return size;
}

/* Returns the size in bytes consumed by the key's value in RAM.
 * Note that the returned value is just an approximation, especially in the
 * case of aggregated data types where only "sample_size" elements
 * are checked and averaged to estimate the total size. */
#define OBJ_COMPUTE_SIZE_DEF_SAMPLES 5 /* Default sample size. */

size_t objectComputeSize(robj *o, size_t sample_size) {
    sds ele, ele2;
    dict *d;
    dictIterator *di;
    struct dictEntry *de;
    size_t asize = 0, elesize = 0, samples = 0;

    if (o->type == OBJ_STRING) {
        if (o->encoding == OBJ_ENCODING_INT) {
            asize = sizeof(*o);
        } else if (o->encoding == OBJ_ENCODING_RAW) {
            asize = sdsZmallocSize(o->ptr) + sizeof(*o);
        } else if (o->encoding == OBJ_ENCODING_EMBSTR) {
            asize = sdslen(o->ptr) + 2 + sizeof(*o);
        } else {
            serverPanic("Unknown string encoding");
        }
    } else if (o->type == OBJ_LIST) {
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;
            asize = sizeof(*o) + sizeof(quicklist);
            do {
                elesize += sizeof(quicklistNode) + ziplistBlobLen(node->zl);
                samples++;
            } while ((node = node->next) && samples < sample_size);
            asize += (double) elesize / samples * ql->len;
        } else if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            asize = sizeof(*o) + ziplistBlobLen(o->ptr);
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        if (o->encoding == OBJ_ENCODING_HT) {
            d = o->ptr;
            di = dictGetIterator(d);
            asize = sizeof(*o) + sizeof(dict) + (sizeof(struct dictEntry *) * dictSlots(d));
            while ((de = dictNext(di)) != NULL && samples < sample_size) {
                ele = dictGetKey(de);
                elesize += sizeof(struct dictEntry) + sdsZmallocSize(ele);
                samples++;
            }
            dictReleaseIterator(di);
            if (samples) asize += (double) elesize / samples * dictSize(d);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            intset *is = o->ptr;
            asize = sizeof(*o) + sizeof(*is) + (size_t) is->encoding * is->length;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            asize = sizeof(*o) + (ziplistBlobLen(o->ptr));
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            d = ((zset *) o->ptr)->dict;
            zskiplist *zsl = ((zset *) o->ptr)->zsl;
            zskiplistNode *znode = zsl->header->level[0].forward;
            asize = sizeof(*o) + sizeof(zset) + sizeof(zskiplist) + sizeof(dict) +
                    (sizeof(struct dictEntry *) * dictSlots(d)) +
                    zmalloc_size(zsl->header);
            while (znode != NULL && samples < sample_size) {
                elesize += sdsZmallocSize(znode->ele);
                elesize += sizeof(struct dictEntry) + zmalloc_size(znode);
                samples++;
                znode = znode->level[0].forward;
            }
            if (samples) asize += (double) elesize / samples * dictSize(d);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            asize = sizeof(*o) + (ziplistBlobLen(o->ptr));
        } else if (o->encoding == OBJ_ENCODING_HT) {
            d = o->ptr;
            di = dictGetIterator(d);
            asize = sizeof(*o) + sizeof(dict) + (sizeof(struct dictEntry *) * dictSlots(d));
            while ((de = dictNext(di)) != NULL && samples < sample_size) {
                ele = dictGetKey(de);
                ele2 = dictGetVal(de);
                elesize += sdsZmallocSize(ele) + sdsZmallocSize(ele2);
                elesize += sizeof(struct dictEntry);
                samples++;
            }
            dictReleaseIterator(di);
            if (samples) asize += (double) elesize / samples * dictSize(d);
        } else {
            serverPanic("Unknown hash encoding");
        }
    } else if (o->type == OBJ_STREAM) {
        stream *s = o->ptr;
        asize = sizeof(*o);
        asize += streamRadixTreeMemoryUsage(s->rax);

        /* Now we have to add the listpacks. The last listpack is often non
         * complete, so we estimate the size of the first N listpacks, and
         * use the average to compute the size of the first N-1 listpacks, and
         * finally add the real size of the last node. */
        raxIterator ri;
        raxStart(&ri, s->rax);
        raxSeek(&ri, "^", NULL, 0);
        size_t lpsize = 0, samples = 0;
        while (samples < sample_size && raxNext(&ri)) {
            unsigned char *lp = ri.data;
            lpsize += lpBytes(lp);
            samples++;
        }
        if (s->rax->numele <= samples) {
            asize += lpsize;
        } else {
            if (samples) lpsize /= samples; /* Compute the average. */
            asize += lpsize * (s->rax->numele - 1);
            /* No need to check if seek succeeded, we enter this branch only
             * if there are a few elements in the radix tree. */
            raxSeek(&ri, "$", NULL, 0);
            raxNext(&ri);
            asize += lpBytes(ri.data);
        }
        raxStop(&ri);

        /* Consumer groups also have a non trivial memory overhead if there
         * are many consumers and many groups, let's count at least the
         * overhead of the pending entries in the groups and consumers
         * PELs. */
        if (s->cgroups) {
            raxStart(&ri, s->cgroups);
            raxSeek(&ri, "^", NULL, 0);
            while (raxNext(&ri)) {
                streamCG *cg = ri.data;
                asize += sizeof(*cg);
                asize += streamRadixTreeMemoryUsage(cg->pel);
                asize += sizeof(streamNACK) * raxSize(cg->pel);

                /* For each consumer we also need to add the basic data
                 * structures and the PEL memory usage. */
                raxIterator cri;
                raxStart(&cri, cg->consumers);
                raxSeek(&cri, "^", NULL, 0);
                while (raxNext(&cri)) {
                    streamConsumer *consumer = cri.data;
                    asize += sizeof(*consumer);
                    asize += sdslen(consumer->name);
                    asize += streamRadixTreeMemoryUsage(consumer->pel);
                    /* Don't count NACKs again, they are shared with the
                     * consumer group PEL. */
                }
                raxStop(&cri);
            }
            raxStop(&ri);
        }
    } else if (o->type == OBJ_MODULE) {
        moduleValue *mv = o->ptr;
        moduleType *mt = mv->type;
        if (mt->mem_usage != NULL) {
            asize = mt->mem_usage(mv->value);
        } else {
            asize = 0;
        }
    } else {
        serverPanic("Unknown object type");
    }
    return asize;
}

/* Release data obtained with getMemoryOverheadData(). */
void freeMemoryOverheadData(struct redisMemOverhead *mh) {
    zfree(mh->db);
    zfree(mh);
}

/* Return a struct redisMemOverhead filled with memory overhead
 * information used for the MEMORY OVERHEAD and INFO command. The returned
 * structure pointer should be freed calling freeMemoryOverheadData(). */
struct redisMemOverhead *getMemoryOverheadData(void) {
    int j;
    size_t mem_total = 0;
    size_t mem = 0;
    size_t zmalloc_used = zmalloc_used_memory();
    struct redisMemOverhead *mh = zcalloc(sizeof(*mh));

    mh->total_allocated = zmalloc_used;
    mh->startup_allocated = server.initial_memory_usage;
    mh->peak_allocated = server.stat_peak_memory;
    mh->total_frag =
            (float) server.cron_malloc_stats.process_rss / server.cron_malloc_stats.zmalloc_used;
    mh->total_frag_bytes =
            server.cron_malloc_stats.process_rss - server.cron_malloc_stats.zmalloc_used;
    mh->allocator_frag =
            (float) server.cron_malloc_stats.allocator_active / server.cron_malloc_stats.allocator_allocated;
    mh->allocator_frag_bytes =
            server.cron_malloc_stats.allocator_active - server.cron_malloc_stats.allocator_allocated;
    mh->allocator_rss =
            (float) server.cron_malloc_stats.allocator_resident / server.cron_malloc_stats.allocator_active;
    mh->allocator_rss_bytes =
            server.cron_malloc_stats.allocator_resident - server.cron_malloc_stats.allocator_active;
    mh->rss_extra =
            (float) server.cron_malloc_stats.process_rss / server.cron_malloc_stats.allocator_resident;
    mh->rss_extra_bytes =
            server.cron_malloc_stats.process_rss - server.cron_malloc_stats.allocator_resident;

    mem_total += server.initial_memory_usage;

    mem = 0;
    if (server.repl_backlog)
        mem += zmalloc_size(server.repl_backlog);
    mh->repl_backlog = mem;
    mem_total += mem;

    /* Computing the memory used by the clients would be O(N) if done
     * here online. We use our values computed incrementally by
     * clientsCronTrackClientsMemUsage(). */
    mh->clients_slaves = server.stat_clients_type_memory[CLIENT_TYPE_SLAVE];
    mh->clients_normal = server.stat_clients_type_memory[CLIENT_TYPE_MASTER] +
                         server.stat_clients_type_memory[CLIENT_TYPE_PUBSUB] +
                         server.stat_clients_type_memory[CLIENT_TYPE_NORMAL];
    mem_total += mh->clients_slaves;
    mem_total += mh->clients_normal;

    mem = 0;
    if (server.aof_state != AOF_OFF) {
        mem += sdsZmallocSize(server.aof_buf);
        mem += aofRewriteBufferSize();
    }
    mh->aof_buffer = mem;
    mem_total += mem;

    mem = server.lua_scripts_mem;
    mem += dictSize(server.lua_scripts) * sizeof(dictEntry) +
           dictSlots(server.lua_scripts) * sizeof(dictEntry *);
    mem += dictSize(server.repl_scriptcache_dict) * sizeof(dictEntry) +
           dictSlots(server.repl_scriptcache_dict) * sizeof(dictEntry *);
    if (listLength(server.repl_scriptcache_fifo) > 0) {
        mem += listLength(server.repl_scriptcache_fifo) * (sizeof(listNode) +
                                                           sdsZmallocSize(listNodeValue(listFirst(
                                                                   server.repl_scriptcache_fifo))));
    }
    mh->lua_caches = mem;
    mem_total += mem;

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db + j;
        long long keyscount = dictSize(db->dict);
        if (keyscount == 0) continue;

        mh->total_keys += keyscount;
        mh->db = zrealloc(mh->db, sizeof(mh->db[0]) * (mh->num_dbs + 1));
        mh->db[mh->num_dbs].dbid = j;

        mem = dictSize(db->dict) * sizeof(dictEntry) +
              dictSlots(db->dict) * sizeof(dictEntry *) +
              dictSize(db->dict) * sizeof(robj);
        mh->db[mh->num_dbs].overhead_ht_main = mem;
        mem_total += mem;

        mem = dictSize(db->expires) * sizeof(dictEntry) +
              dictSlots(db->expires) * sizeof(dictEntry *);
        mh->db[mh->num_dbs].overhead_ht_expires = mem;
        mem_total += mem;

        mh->num_dbs++;
    }

    mh->overhead_total = mem_total;
    mh->dataset = zmalloc_used - mem_total;
    mh->peak_perc = (float) zmalloc_used * 100 / mh->peak_allocated;

    /* Metrics computed after subtracting the startup memory from
     * the total memory. */
    size_t net_usage = 1;
    if (zmalloc_used > mh->startup_allocated)
        net_usage = zmalloc_used - mh->startup_allocated;
    mh->dataset_perc = (float) mh->dataset * 100 / net_usage;
    mh->bytes_per_key = mh->total_keys ? (net_usage / mh->total_keys) : 0;

    return mh;
}

/* Helper for "MEMORY allocator-stats", used as a callback for the jemalloc
 * stats output. */
void inputCatSds(void *result, const char *str) {
    /* result is actually a (sds *), so re-cast it here */
    sds *info = (sds *) result;
    *info = sdscat(*info, str);
}

/* This implements MEMORY DOCTOR. An human readable analysis of the Redis
 * memory condition. */
sds getMemoryDoctorReport(void) {
    int empty = 0;          /* Instance is empty or almost empty. */
    int big_peak = 0;       /* Memory peak is much larger than used mem. */
    int high_frag = 0;      /* High fragmentation. */
    int high_alloc_frag = 0;/* High allocator fragmentation. */
    int high_proc_rss = 0;  /* High process rss overhead. */
    int high_alloc_rss = 0; /* High rss overhead. */
    int big_slave_buf = 0;  /* Slave buffers are too big. */
    int big_client_buf = 0; /* Client buffers are too big. */
    int many_scripts = 0;   /* Script cache has too many scripts. */
    int num_reports = 0;
    struct redisMemOverhead *mh = getMemoryOverheadData();

    if (mh->total_allocated < (1024 * 1024 * 5)) {
        empty = 1;
        num_reports++;
    } else {
        /* Peak is > 150% of current used memory? */
        if (((float) mh->peak_allocated / mh->total_allocated) > 1.5) {
            big_peak = 1;
            num_reports++;
        }

        /* Fragmentation is higher than 1.4 and 10MB ?*/
        if (mh->total_frag > 1.4 && mh->total_frag_bytes > 10 << 20) {
            high_frag = 1;
            num_reports++;
        }

        /* External fragmentation is higher than 1.1 and 10MB? */
        if (mh->allocator_frag > 1.1 && mh->allocator_frag_bytes > 10 << 20) {
            high_alloc_frag = 1;
            num_reports++;
        }

        /* Allocator rss is higher than 1.1 and 10MB ? */
        if (mh->allocator_rss > 1.1 && mh->allocator_rss_bytes > 10 << 20) {
            high_alloc_rss = 1;
            num_reports++;
        }

        /* Non-Allocator rss is higher than 1.1 and 10MB ? */
        if (mh->rss_extra > 1.1 && mh->rss_extra_bytes > 10 << 20) {
            high_proc_rss = 1;
            num_reports++;
        }

        /* Clients using more than 200k each average? */
        long numslaves = listLength(server.slaves);
        long numclients = listLength(server.clients) - numslaves;
        if (mh->clients_normal / numclients > (1024 * 200)) {
            big_client_buf = 1;
            num_reports++;
        }

        /* Slaves using more than 10 MB each? */
        if (numslaves > 0 && mh->clients_slaves / numslaves > (1024 * 1024 * 10)) {
            big_slave_buf = 1;
            num_reports++;
        }

        /* Too many scripts are cached? */
        if (dictSize(server.lua_scripts) > 1000) {
            many_scripts = 1;
            num_reports++;
        }
    }

    sds s;
    if (num_reports == 0) {
        s = sdsnew(
                "Hi Sam, I can't find any memory issue in your instance. "
                "I can only account for what occurs on this base.\n");
    } else if (empty == 1) {
        s = sdsnew(
                "Hi Sam, this instance is empty or is using very little memory, "
                "my issues detector can't be used in these conditions. "
                "Please, leave for your mission on Earth and fill it with some data. "
                "The new Sam and I will be back to our programming as soon as I "
                "finished rebooting.\n");
    } else {
        s = sdsnew("Sam, I detected a few issues in this Redis instance memory implants:\n\n");
        if (big_peak) {
            s = sdscat(s,
                       " * Peak memory: In the past this instance used more than 150% the memory that is currently using. The allocator is normally not able to release memory after a peak, so you can expect to see a big fragmentation ratio, however this is actually harmless and is only due to the memory peak, and if the Redis instance Resident Set Size (RSS) is currently bigger than expected, the memory will be used as soon as you fill the Redis instance with more data. If the memory peak was only occasional and you want to try to reclaim memory, please try the MEMORY PURGE command, otherwise the only other option is to shutdown and restart the instance.\n\n");
        }
        if (high_frag) {
            s = sdscatprintf(s,
                             " * High total RSS: This instance has a memory fragmentation and RSS overhead greater than 1.4 (this means that the Resident Set Size of the Redis process is much larger than the sum of the logical allocations Redis performed). This problem is usually due either to a large peak memory (check if there is a peak memory entry above in the report) or may result from a workload that causes the allocator to fragment memory a lot. If the problem is a large peak memory, then there is no issue. Otherwise, make sure you are using the Jemalloc allocator and not the default libc malloc. Note: The currently used allocator is \"%s\".\n\n",
                             ZMALLOC_LIB);
        }
        if (high_alloc_frag) {
            s = sdscatprintf(s,
                             " * High allocator fragmentation: This instance has an allocator external fragmentation greater than 1.1. This problem is usually due either to a large peak memory (check if there is a peak memory entry above in the report) or may result from a workload that causes the allocator to fragment memory a lot. You can try enabling 'activedefrag' config option.\n\n");
        }
        if (high_alloc_rss) {
            s = sdscatprintf(s,
                             " * High allocator RSS overhead: This instance has an RSS memory overhead is greater than 1.1 (this means that the Resident Set Size of the allocator is much larger than the sum what the allocator actually holds). This problem is usually due to a large peak memory (check if there is a peak memory entry above in the report), you can try the MEMORY PURGE command to reclaim it.\n\n");
        }
        if (high_proc_rss) {
            s = sdscatprintf(s,
                             " * High process RSS overhead: This instance has non-allocator RSS memory overhead is greater than 1.1 (this means that the Resident Set Size of the Redis process is much larger than the RSS the allocator holds). This problem may be due to Lua scripts or Modules.\n\n");
        }
        if (big_slave_buf) {
            s = sdscat(s,
                       " * Big replica buffers: The replica output buffers in this instance are greater than 10MB for each replica (on average). This likely means that there is some replica instance that is struggling receiving data, either because it is too slow or because of networking issues. As a result, data piles on the master output buffers. Please try to identify what replica is not receiving data correctly and why. You can use the INFO output in order to check the replicas delays and the CLIENT LIST command to check the output buffers of each replica.\n\n");
        }
        if (big_client_buf) {
            s = sdscat(s,
                       " * Big client buffers: The clients output buffers in this instance are greater than 200K per client (on average). This may result from different causes, like Pub/Sub clients subscribed to channels bot not receiving data fast enough, so that data piles on the Redis instance output buffer, or clients sending commands with large replies or very large sequences of commands in the same pipeline. Please use the CLIENT LIST command in order to investigate the issue if it causes problems in your instance, or to understand better why certain clients are using a big amount of memory.\n\n");
        }
        if (many_scripts) {
            s = sdscat(s,
                       " * Many scripts: There seem to be many cached scripts in this instance (more than 1000). This may be because scripts are generated and `EVAL`ed, instead of being parameterized (with KEYS and ARGV), `SCRIPT LOAD`ed and `EVALSHA`ed. Unless `SCRIPT FLUSH` is called periodically, the scripts' caches may end up consuming most of your memory.\n\n");
        }
        s = sdscat(s, "I'm here to keep you safe, Sam. I want to help you.\n");
    }
    freeMemoryOverheadData(mh);
    return s;
}

/* Set the object LRU/LFU depending on server.maxmemory_policy.
 * The lfu_freq arg is only relevant if policy is MAXMEMORY_FLAG_LFU.
 * The lru_idle and lru_clock args are only relevant if policy
 * is MAXMEMORY_FLAG_LRU.
 * Either or both of them may be <0, in that case, nothing is set. */
int objectSetLRUOrLFU(robj *val, long long lfu_freq, long long lru_idle,
                      long long lru_clock, int lru_multiplier) {
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        if (lfu_freq >= 0) {
            serverAssert(lfu_freq <= 255);
            val->lru = (LFUGetTimeInMinutes() << 8) | lfu_freq;
            return 1;
        }
    } else if (lru_idle >= 0) {
        /* Provided LRU idle time is in seconds. Scale
         * according to the LRU clock resolution this Redis
         * instance was compiled with (normally 1000 ms, so the
         * below statement will expand to lru_idle*1000/1000. */
        lru_idle = lru_idle * lru_multiplier / LRU_CLOCK_RESOLUTION;
        long lru_abs = lru_clock - lru_idle; /* Absolute access time. */
        /* If the LRU field underflows (since LRU it is a wrapping
         * clock), the best we can do is to provide a large enough LRU
         * that is half-way in the circlular LRU clock we use: this way
         * the computed idle time for this object will stay high for quite
         * some time. */
        if (lru_abs < 0)
            lru_abs = (lru_clock + (LRU_CLOCK_MAX / 2)) % LRU_CLOCK_MAX;
        val->lru = lru_abs;
        return 1;
    }
    return 0;
}

/* ======================= The OBJECT and MEMORY commands =================== */

/* This is a helper function for the OBJECT command. We need to lookup keys
 * without any modification of LRU or other parameters. */
robj *objectCommandLookup(client *c, robj *key) {
    return lookupKeyReadWithFlags(c->db, key, LOOKUP_NOTOUCH | LOOKUP_NONOTIFY);
}

robj *objectCommandLookupOrReply(client *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c, key);

    if (!o) addReply(c, reply);
    return o;
}

/* Object command allows to inspect the internals of a Redis Object.
 * Usage: OBJECT <refcount|encoding|idletime|freq> <key> */
void objectCommand(client *c) {
    robj *o;

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "help")) {
        const char *help[] = {
                "ENCODING <key>",
                "    Return the kind of internal representation used in order to store the value",
                "    associated with a <key>.",
                "FREQ <key>",
                "    Return the access frequency index of the <key>. The returned integer is",
                "    proportional to the logarithm of the recent access frequency of the key.",
                "IDLETIME <key>",
                "    Return the idle time of the <key>, that is the approximated number of",
                "    seconds elapsed since the last access to the key.",
                "REFCOUNT <key>",
                "    Return the number of references of the value associated with the specified",
                "    <key>.",
                NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr, "refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c, c->argv[2], shared.null[c->resp]))
            == NULL)
            return;
        addReplyLongLong(c, o->refcount);
    } else if (!strcasecmp(c->argv[1]->ptr, "encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c, c->argv[2], shared.null[c->resp]))
            == NULL)
            return;
        addReplyBulkCString(c, strEncoding(o->encoding));
    } else if (!strcasecmp(c->argv[1]->ptr, "idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c, c->argv[2], shared.null[c->resp]))
            == NULL)
            return;
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            addReplyError(c,
                          "An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return;
        }
        addReplyLongLong(c, estimateObjectIdleTime(o) / 1000);
    } else if (!strcasecmp(c->argv[1]->ptr, "freq") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c, c->argv[2], shared.null[c->resp]))
            == NULL)
            return;
        if (!(server.maxmemory_policy & MAXMEMORY_FLAG_LFU)) {
            addReplyError(c,
                          "An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return;
        }
        /* LFUDecrAndReturn should be called
         * in case of the key has not been accessed for a long time,
         * because we update the access time only
         * when the key is read or overwritten. */
        addReplyLongLong(c, LFUDecrAndReturn(o));
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* The memory command will eventually be a complete interface for the
 * memory introspection capabilities of Redis.
 *
 * Usage: MEMORY usage <key> */
void memoryCommand(client *c) {
    if (!strcasecmp(c->argv[1]->ptr, "help") && c->argc == 2) {
        const char *help[] = {
                "DOCTOR",
                "    Return memory problems reports.",
                "MALLOC-STATS"
                "    Return internal statistics report from the memory allocator.",
                "PURGE",
                "    Attempt to purge dirty pages for reclamation by the allocator.",
                "STATS",
                "    Return information about the memory usage of the server.",
                "USAGE <key> [SAMPLES <count>]",
                "    Return memory in bytes used by <key> and its value. Nested values are",
                "    sampled up to <count> times (default: 5).",
                NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr, "usage") && c->argc >= 3) {
        dictEntry *de;
        long long samples = OBJ_COMPUTE_SIZE_DEF_SAMPLES;
        for (int j = 3; j < c->argc; j++) {
            if (!strcasecmp(c->argv[j]->ptr, "samples") &&
                j + 1 < c->argc) {
                if (getLongLongFromObjectOrReply(c, c->argv[j + 1], &samples, NULL)
                    == C_ERR)
                    return;
                if (samples < 0) {
                    addReplyErrorObject(c, shared.syntaxerr);
                    return;
                }
                if (samples == 0) samples = LLONG_MAX;
                j++; /* skip option argument. */
            } else {
                addReplyErrorObject(c, shared.syntaxerr);
                return;
            }
        }
        if ((de = dictFind(c->db->dict, c->argv[2]->ptr)) == NULL) {
            addReplyNull(c);
            return;
        }
        size_t usage = objectComputeSize(dictGetVal(de), samples);
        usage += sdsZmallocSize(dictGetKey(de));
        usage += sizeof(dictEntry);
        addReplyLongLong(c, usage);
    } else if (!strcasecmp(c->argv[1]->ptr, "stats") && c->argc == 2) {
        struct redisMemOverhead *mh = getMemoryOverheadData();

        addReplyMapLen(c, 25 + mh->num_dbs);

        addReplyBulkCString(c, "peak.allocated");
        addReplyLongLong(c, mh->peak_allocated);

        addReplyBulkCString(c, "total.allocated");
        addReplyLongLong(c, mh->total_allocated);

        addReplyBulkCString(c, "startup.allocated");
        addReplyLongLong(c, mh->startup_allocated);

        addReplyBulkCString(c, "replication.backlog");
        addReplyLongLong(c, mh->repl_backlog);

        addReplyBulkCString(c, "clients.slaves");
        addReplyLongLong(c, mh->clients_slaves);

        addReplyBulkCString(c, "clients.normal");
        addReplyLongLong(c, mh->clients_normal);

        addReplyBulkCString(c, "aof.buffer");
        addReplyLongLong(c, mh->aof_buffer);

        addReplyBulkCString(c, "lua.caches");
        addReplyLongLong(c, mh->lua_caches);

        for (size_t j = 0; j < mh->num_dbs; j++) {
            char dbname[32];
            snprintf(dbname, sizeof(dbname), "db.%zd", mh->db[j].dbid);
            addReplyBulkCString(c, dbname);
            addReplyMapLen(c, 2);

            addReplyBulkCString(c, "overhead.hashtable.main");
            addReplyLongLong(c, mh->db[j].overhead_ht_main);

            addReplyBulkCString(c, "overhead.hashtable.expires");
            addReplyLongLong(c, mh->db[j].overhead_ht_expires);
        }

        addReplyBulkCString(c, "overhead.total");
        addReplyLongLong(c, mh->overhead_total);

        addReplyBulkCString(c, "keys.count");
        addReplyLongLong(c, mh->total_keys);

        addReplyBulkCString(c, "keys.bytes-per-key");
        addReplyLongLong(c, mh->bytes_per_key);

        addReplyBulkCString(c, "dataset.bytes");
        addReplyLongLong(c, mh->dataset);

        addReplyBulkCString(c, "dataset.percentage");
        addReplyDouble(c, mh->dataset_perc);

        addReplyBulkCString(c, "peak.percentage");
        addReplyDouble(c, mh->peak_perc);

        addReplyBulkCString(c, "allocator.allocated");
        addReplyLongLong(c, server.cron_malloc_stats.allocator_allocated);

        addReplyBulkCString(c, "allocator.active");
        addReplyLongLong(c, server.cron_malloc_stats.allocator_active);

        addReplyBulkCString(c, "allocator.resident");
        addReplyLongLong(c, server.cron_malloc_stats.allocator_resident);

        addReplyBulkCString(c, "allocator-fragmentation.ratio");
        addReplyDouble(c, mh->allocator_frag);

        addReplyBulkCString(c, "allocator-fragmentation.bytes");
        addReplyLongLong(c, mh->allocator_frag_bytes);

        addReplyBulkCString(c, "allocator-rss.ratio");
        addReplyDouble(c, mh->allocator_rss);

        addReplyBulkCString(c, "allocator-rss.bytes");
        addReplyLongLong(c, mh->allocator_rss_bytes);

        addReplyBulkCString(c, "rss-overhead.ratio");
        addReplyDouble(c, mh->rss_extra);

        addReplyBulkCString(c, "rss-overhead.bytes");
        addReplyLongLong(c, mh->rss_extra_bytes);

        addReplyBulkCString(c, "fragmentation"); /* this is the total RSS overhead, including fragmentation */
        addReplyDouble(c, mh->total_frag); /* it is kept here for backwards compatibility */

        addReplyBulkCString(c, "fragmentation.bytes");
        addReplyLongLong(c, mh->total_frag_bytes);

        freeMemoryOverheadData(mh);
    } else if (!strcasecmp(c->argv[1]->ptr, "malloc-stats") && c->argc == 2) {
#if defined(USE_JEMALLOC)
        sds info = sdsempty();
        je_malloc_stats_print(inputCatSds, &info, NULL);
        addReplyVerbatim(c,info,sdslen(info),"txt");
        sdsfree(info);
#else
        addReplyBulkCString(c, "Stats not supported for the current allocator");
#endif
    } else if (!strcasecmp(c->argv[1]->ptr, "doctor") && c->argc == 2) {
        sds report = getMemoryDoctorReport();
        addReplyVerbatim(c, report, sdslen(report), "txt");
        sdsfree(report);
    } else if (!strcasecmp(c->argv[1]->ptr, "purge") && c->argc == 2) {
        if (jemalloc_purge() == 0)
            addReply(c, shared.ok);
        else
            addReplyError(c, "Error purging dirty pages");
    } else {
        addReplySubcommandSyntaxError(c);
    }
}
