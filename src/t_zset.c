/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 * zset 同时使用两种数据结构来持有同一个元素，从而提供 O(log N) 复杂度的有序数据结构插入和移除操作
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view").
 *
 * 哈希表将 Redis 对象映射都分值上，而跳跃表则将分值映射到 Redis 对象上，
 * 以跳跃表的视角来看，可以说 Redis 对象是根据分值来排序的
 *
 * Note that the SDS string representing the element is the same in both
 * the hash table and skiplist in order to save memory. What we do in order
 * to manage the shared SDS string more easily is to free the SDS string
 * only in zslFreeNode(). The dictionary has no value free method set.
 * So we should always remove an element from the dictionary, and later from
 * the skiplist.
 *
 * This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 *
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. */

#include "server.h"
#include <math.h>

/**-----------------------------------------------------------------------------
 * Skiplist implementation of the low level API    跳表实现 API
 *----------------------------------------------------------------------------*/

int zslLexValueGteMin(sds value, zlexrangespec *spec);

int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call.
 *
 * 创建一个层数为 level 的跳跃表节点，返回值为新创建的跳跃表节点
 *
 */
zskiplistNode *zslCreateNode(int level, double score, sds ele) {

    // 分配空间
    zskiplistNode *zn =
            zmalloc(sizeof(*zn) + level * sizeof(struct zskiplistLevel));

    // 设置属性
    zn->score = score;
    zn->ele = ele;
    return zn;
}

/* Create a new skiplist.
 *
 * 创建并返回一个新的跳表
 *
 * T = O(1)
 *
 */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    // 分配空间
    zsl = zmalloc(sizeof(*zsl));

    // 设置跳表的层数，初始值为 1
    zsl->level = 1;

    // 设置跳表的长度
    zsl->length = 0;

    // 初始化表头节点
    // 默认 32 层、分数为 0、值为 NULL
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL, 0, NULL);

    // 根据最大层数 32，为头节点关联多个层
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }

    // 设置头节点的向后指针
    zsl->header->backward = NULL;

    // 设置表尾
    zsl->tail = NULL;
    return zsl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function.
 *
 * 释放给定的跳跃表节点
 *
 */
void zslFreeNode(zskiplistNode *node) {
    sdsfree(node->ele);
    zfree(node);
}

/* Free a whole skiplist.
 *
 * 释放给定跳跃表，以及表中的所有节点
 *
 */
void zslFree(zskiplist *zsl) {
    // 获取头节点最低层的前置指针指向的节点 node
    zskiplistNode *node = zsl->header->level[0].forward, *next;

    // 释放表头
    zfree(zsl->header);

    // 释放表中所有节点
    // 以 node 不断向前遍历。注意，这里使用的 0 层，该层具有所有节点
    while (node) {
        next = node->level[0].forward;

        // 释放给定的跳跃表节点
        zslFreeNode(node);
        node = next;
    }

    // 释放跳表结构
    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned.
 *
 * 返回一个随机值，用作新跳跃表节点的层数，即随机生成节点的层数。
 *
 * 返回值介于 1 和 ZSKIPLIST_MAXLEVEL（32） 之间（包含 ZSKIPLIST_MAXLEVEL），根据随机算法所使用的幂次定律，越大的值生成的几率越小。
 *
 * 说明：
 * 这个预设「概率」决定了一个跳表的内存占用和查询复杂度：概率设置越低，层数越少，元素指针越少，内存占用也就越少，但查询复杂会变高，反之亦然。
 * 这也是 skiplist 的一大特点，可通过控制概率，进而控制内存和查询效率
 *
 */
int zslRandomLevel(void) {
    // 初始化层为 1
    int level = 1;
    while ((random() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;

    return (level < ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'.
 *
 * 创建一个成员为 ele，分值为 score 的新节点，并将这个节点插入到跳跃表 zsl 中。
 *
 * 函数返回值为新节点
 *
 * 步骤：
 * 1）逐层遍历每层的链表，找出最后一个小于输入的 score（score 相同时，比较成员的大小），并收集这个该节点
 * 2）为新节点随机生成一个层级 level
 * 3）根据生成的层级 level，判断是否超过跳表最大的层级，超过的话就会启用头节点中还没有用到的层级（最大 32 个）
 * 4）依次调整 0 ～ level -1 层级上的链表
 *    - 维护前后指针
 *    - 更新新节点和新节点前一个节点的排位
 * 5）更新剩余 update 数组中节点的排位，加 1 即可
 * 6）更新新节点的后置指针，以及新节点的下一个节点的后置指针为自己
 *
 */
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele) {
    // 0 定义某一 level 链表中要更新的节点
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    serverAssert(!isnan(score));

    // 1 获取跳表的头节点
    x = zsl->header;

    // 2 在各个层查找节点的插入位置，先从最高层链表进行遍历，直到把最低层遍历完
    // T_wrost = O(N^2), T_avg = O(N log N)
    for (i = zsl->level - 1; i >= 0; i--) {

        /* store rank that is crossed to reach the insert position 为了到达插入位置而交叉的存储 rank */
        /*
         * 如果 i 不是 zsl->level-1 层，那么 i 层的起始 rank 值为 i + 1 层的 rank 值，也就是从上层下到下层后，当前累加排位。 如果是最高层，则排位为 0
         * 各个层的 rank 值一层层累加，最终 rank[0] 的值加一就是新节点的前置节点的排位，
         * rank[0] 会在后面成为计算 span 值和 rank 值的基础
         */
        rank[i] = i == (zsl->level - 1) ? 0 : rank[i + 1];

        // 沿着前进指针遍历链表，找出链表中最后一个小于输入 score 或 成员的节点，并记录到 update 数组中。因为是按照 score 排序的。
        // T_wrost = O(N^2), T_avg = O(N log N)
        while (x->level[i].forward &&
               // 比对分值 - 要插入节点的分值要更大
               (x->level[i].forward->score < score ||
                // 分值相同，比对成员 - 成员要更大
                (x->level[i].forward->score == score &&
                 sdscmp(x->level[i].forward->ele, ele) < 0)
               )) {

            /*执行到这里，说明要插入节点的位置还在当前层的后面，这里以 score、元素大小从小到大排序 */

            // 累加沿途跨越了多少个节点
            // x->level[i].span 指的是，x 节点在 i 层指向下个节点的跨度
            rank[i] += x->level[i].span;

            // 移动至下一个指针
            x = x->level[i].forward;
        }

        /* 执行到这里，说明 i 层和新节点连接的是 x ，要么 x 是最后一个节点，要么是找到了合适的节点 */

        // 记录将要和新节点相连接的节点，具体是作为新节点的前面一个节点
        // todo 注意，这个只是作为备用的数组，具体哪层节点和新节点连接，要看随机为新节点生成的层数大小
        update[i] = x;

        /* 执行到这里，说明要从当前节点进入到下一层 level 了 */
    }

    /* we assume the elemen t is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of zslInsert() should test in the hash table if the element is
     * already inside or not.
     *
     * zlInsert() 的调用者会确保同分值同成员的元素不会出现，所以这里不需要进一步进行检查，可以直接创建新元素。
     */

    // 获取一个随机值作为新节点的层数，范围在 1～32
    level = zslRandomLevel();

    // 如果新节点的层数比表中其它节点的层数都要大，那么初始化表头节点中未使用的层，并将它们记录到 update 数组中，将来也指向新节点
    // 这种情况有点类似兜底，随机生成的层数如果大于跳表中最大的层数，那么直接将头节点作为新节点的
    if (level > zsl->level) {

        // 初始化未使用的层，也就是 zsl->level ~ level-1 范围的层
        for (i = zsl->level; i < level; i++) {
            // 初始化 i 层的跨度为 0
            rank[i] = 0;
            // 初始化连接到新节点的节点是 头节点
            update[i] = zsl->header;

            // 初始化跨度为跳表的长度
            update[i]->level[i].span = zsl->length;
        }

        // 更新表中节点最大层数
        zsl->level = level;
    }

    // 创建一个新的跳表节点，包括节点的 level 数组结构
    x = zslCreateNode(level, score, ele);

    // 根据生成的层级，为新的跳表节点关联每层的指针
    // 将前面记录的指针指向新节点，并做相应的设置
    for (i = 0; i < level; i++) {

        /*------ 重新构建每层链表 ------*/

        // 设置新节点的 forward 指针
        // 维护后继节点
        x->level[i].forward = update[i]->level[i].forward;

        // 将记录的和新节点连接的节点的 forward 指针指向新节点
        // 维护前置节点，即前置节点使用 forward 指向自己
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        // 计算新节点跨越的节点数量
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);

        // 更新新节点插入之后，沿途节点的 span 值
        // 其中的 +1 计算的是新节点
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    // 未接触的节点的 span 值也需要增一，因为中间加了一个新节点
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    // 设置新节点的后退指针，用最底层的 update 节点
    x->backward = (update[0] == zsl->header) ? NULL : update[0];

    // 设置新节点的后一个节点的后退指针，也就是新节点了
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
        // 新节点是最后一个节点，那么更新跳表的尾节点是新节点
    else
        zsl->tail = x;

    // 跳跃表的节点计数增一
    zsl->length++;
    return x;
}

/* Internal function used by zslDelete, zslDeleteRangeByScore and
 * zslDeleteRangeByRank.
 *
 * 通用删除函数
 *
 * 被 zslDelete、zslDeleteRangeByScore 和 zslDeleteByRank 等函数调用
 *
 *
 * @param zsl 压缩列表
 * @param x 要删除的节点
 * @param update 和要删除的节点相连的节点数组，具体来说是每层链表中最后一个 <= x 的节点数组
 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;
    // 更新所有和被删除节点 x 有关的节点的指针，解除它们之间的关系

    // 遍历所有的层，单层的链表操作
    for (i = 0; i < zsl->level; i++) {
        // 前置指针指向 x ，则需要更新跨度和前置指针
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;

            // 没有指向 x ，仅需要更新跨度即可
        } else {
            update[i]->level[i].span -= 1;
        }
    }

    // 处理和 x 节点的后退指针
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }


    // 更新跳跃表最大层数（只在被删除节点是跳跃表中最高的节点时才执行）
    while (zsl->level > 1 && zsl->header->level[zsl->level - 1].forward == NULL)
        zsl->level--;

    // 跳跃表节点计数器减一
    zsl->length--;
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele).
 *
 * 从跳跃表 zsl 中删除指定的节点
 *
 */
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    // 跳表的头节点
    x = zsl->header;

    // 逐层遍历链表，查找目标节点，并记录所有沿途相关节点
    // T_wrost = O(N^2), T_avg = O(N log N)
    for (i = zsl->level - 1; i >= 0; i--) {

        // 遍历 i 层链表
        while (x->level[i].forward &&
               // 比对分值
               (x->level[i].forward->score < score ||

                // 分值相同，比对对象
                (x->level[i].forward->score == score &&
                 sdscmp(x->level[i].forward->ele, ele) < 0))) {

            /*执行到这里，说明目标节点的位置可能还在当前层的后面，这里以 score、元素大小从小到大排序 */

            // 沿着前进指针移动
            x = x->level[i].forward;
        }

        /* 执行到这里，说明 x 节点可能是目标节点 ，可能 x 是 i 层最后一个节点 */
        // 记录沿途相关节点
        update[i] = x;
    }

    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */

    // 检查找到的元素 x ，只有在它的分值和对象都相同时，才将它删除
    x = x->level[0].forward;
    if (x && score == x->score && sdscmp(x->ele, ele) == 0) {
        // 删除 x 节点，及更新沿途相关节点
        zslDeleteNode(zsl, x, update);
        if (!node)
            zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    return 0; /* not found */
}

/* Update the score of an element inside the sorted set skiplist.
 * Note that the element must exist and must match 'score'.
 * This function does not update the score in the hash table side, the
 * caller should take care of it.
 *
 * Note that this function attempts to just update the node, in case after
 * the score update, the node would be exactly at the same position.
 * Otherwise the skiplist is modified by removing and re-adding a new
 * element, which is more costly.
 *
 * The function returns the updated element skiplist node pointer. */
zskiplistNode *zslUpdateScore(zskiplist *zsl, double curscore, sds ele, double newscore) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    /* We need to seek to element to update to start: this is useful anyway,
     * we'll have to update or remove it. */
    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < curscore ||
                (x->level[i].forward->score == curscore &&
                 sdscmp(x->level[i].forward->ele, ele) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* Jump to our element: note that this function assumes that the
     * element with the matching score exists. */
    x = x->level[0].forward;
    serverAssert(x && curscore == x->score && sdscmp(x->ele, ele) == 0);

    /* If the node, after the score update, would be still exactly
     * at the same position, we can just update the score without
     * actually removing and re-inserting the element in the skiplist. */
    if ((x->backward == NULL || x->backward->score < newscore) &&
        (x->level[0].forward == NULL || x->level[0].forward->score > newscore)) {
        x->score = newscore;
        return x;
    }

    /* No way to reuse the old node: we need to remove and insert a new
     * one at a different place. */
    zslDeleteNode(zsl, x, update);
    zskiplistNode *newnode = zslInsert(zsl, newscore, x->ele);
    /* We reused the old node x->ele SDS string, free the node now
     * since zslInsert created a new one. */
    x->ele = NULL;
    zslFreeNode(x);
    return newnode;
}

/*
 * 检测给定值 value 是否大于 或大于等于 范围 spec 中的 min 项
 *
 * 返回 1 表示 value 大于等于 min 项，否则返回 0
 */
int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

/*
 * 检测给定值 value 是否小于（或小于等于）范围 spec 中的 max 项。
 *
 * 返回 1 表示 value 小于等于 max 项，否则返回 0
 */
int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range.
 *
 *如果给定的分数范围包含在跳表的分数范围内，那么返回 1 ，否则返回 0
 */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    // 排除总为空的范围值
    if (range->min > range->max ||
        (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // 检查最大分数值，即 尾节点中的分数值
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score, range))
        return 0;

    // 检查最小分数值，即 头节点的 0 层链表的第一个节点
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score, range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range.
 *
 * 返回 zsl 中第一个分值符合 range 中指定范围的节点，没有则返回 NULL
 */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 确保跳跃表中存在节点符合 range 指定的范围
    if (!zslIsInRange(zsl, range)) return NULL;

    x = zsl->header;

    // 逐层遍历跳表，查找符合范围 min 的节点
    // T_wrost = O(N), T_avg = O(log N)
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
               !zslValueGteMin(x->level[i].forward->score, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    // 获取大于 range 的 min 第一个节点
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    // 检查节点是否符合范围 max 项
    if (!zslValueLteMax(x->score, range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range.
 *
 * 返回 zsl 中最后一个分值符合 range 中指定范围的节点，如果 zsl 中没有符合范围的节点，返回 NULL
 */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    // 先确保跳跃表中至少有一个节点符合 range 指定的范围
    if (!zslIsInRange(zsl, range)) return NULL;


    x = zsl->header;

    // 逐层遍历跳跃表（每层对应一个链表），查找符合范围 max 项的节点
    // T_wrost = O(N), T_avg = O(log N)
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
               zslValueLteMax(x->level[i].forward->score, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    // 检查节点是否符合范围的 min 项
    if (!zslValueGteMin(x->score, range)) return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 *
 * 删除所有分值在给定范围之内的节点
 *
 * Both min and max can be inclusive or exclusive (see range->minex and
 * range->maxex). When inclusive a score >= min && score <= max is deleted.
 *
 * min 和 max 参数都是包含在范围之内，所以分值 >= min && <= max 的节点都会被删除
 *
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too.
 *
 * 节点不仅会从跳表中删除，而且会从相应的字典中删除
 *
 * 返回值为被删除节点的数量
 */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;

    // 逐层遍历跳表
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               !zslValueGteMin(x->level[i].forward->score, range))
            x = x->level[i].forward;

        // 记录被删除的节点相关的节点
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    // 定位到给定范围开始的第一个节点
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    // 删除范围中的所有节点
    while (x && zslValueLteMax(x->score, range)) {

        // 记录要删除节点的下一个节点
        zskiplistNode *next = x->level[0].forward;

        // 跳表删除
        zslDeleteNode(zsl, x, update);

        // 字典删除
        dictDelete(dict, x->ele);

        // 释放跳表节点
        zslFreeNode(x); /* Here is where x->ele is actually released. */

        // 增加删除计数器
        removed++;

        // 继续处理下个节点
        x = next;
    }

    // 返回被删除节点的数量
    return removed;
}

unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               !zslLexValueGteMin(x->level[i].forward->ele, range))
            x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x && zslLexValueLteMax(x->ele, range)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl, x, update);
        dictDelete(dict, x->ele);
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 *
 * 从跳表中删除所有给定排位内的节点
 *
 * Start and end are inclusive. Note that start and end need to be 1-based
 *
 * start 和 end 两个位置都是包含在内的。注意它们都是以 1 为起始值。
 *
 */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;

    // 逐层遍历跳表
    for (i = zsl->level - 1; i >= 0; i--) {
        // 遍历 i 层链表，寻找 >= start 的节点
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }

        // 记录沿途相关节点
        update[i] = x;
    }

    // 移动到排位的起始的第一个节点
    traversed++;
    x = x->level[0].forward;

    // 删除所有在给定排位范围内的节点
    while (x && traversed <= end) {
        // 记录下一个节点的指针
        zskiplistNode *next = x->level[0].forward;

        // 从跳表中删除节点
        zslDeleteNode(zsl, x, update);
        // 从字典中删除节点
        dictDelete(dict, x->ele);

        // 释放节点结构
        zslFreeNode(x);
        removed++;

        // 为排位计数器增一
        traversed++;

        // 处理下个节点
        x = next;
    }

    // 返回被删除节点的数量
    return removed;
}

/* Find the rank for an element by both score and key.
 *
 * 查找包含给定分值和成员对象的节点在跳跃表中的排位。
 *
 * Returns 0 when the element cannot be found, rank otherwise.
 *
 * 如果没有包含给定分值和成员对象的节点，返回 0 ，否则返回排位。
 *
 *
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element.
 *
 * 注意，因为跳跃表的表头也被计算在内，所以返回的排位以 1 为起始值。
 */
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;

    // 分层遍历跳表
    for (i = zsl->level - 1; i >= 0; i--) {

        // 遍历当前 i 层的链表节点
        while (x->level[i].forward &&
               // (当前节点分数 < score) || (当前节点分数 == score && 对象大小 < ele)
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                 sdscmp(x->level[i].forward->ele, ele) <= 0))) {

            // 累加跨越的节点数量，即排位
            rank += x->level[i].span;

            // 下一个节点
            x = x->level[i].forward;
        }

        /* 执行到这里，说明 i 层的链表遍历完了，或者找到一个满足条件的节点 */

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        // 必须确保不仅分值相等，而且成员对象也要相等
        if (x->ele && sdscmp(x->ele, ele) == 0) {
            return rank;
        }
    }

    // 没找到
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based.
 *
 * 根据排位在跳表中查找元素，排位的起始值为 1
 *
 * 成功查找返回相应的跳跃表节点，没找到则返回 NULL 。
 */
zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;

    // 逐层遍历跳表
    for (i = zsl->level - 1; i >= 0; i--) {

        // 遍历链表并累加越过的节点数量
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }

        // 每一层遍历完后，都判断一次，越过的节点数是否已经等于 rank，如果等于说明找到了目标节点
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* Populate the rangespec according to the objects min and max. */
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    if (min->encoding == OBJ_ENCODING_INT) {
        spec->min = (long) min->ptr;
    } else {
        if (((char *) min->ptr)[0] == '(') {
            spec->min = strtod((char *) min->ptr + 1, &eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod((char *) min->ptr, &eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
        }
    }
    if (max->encoding == OBJ_ENCODING_INT) {
        spec->max = (long) max->ptr;
    } else {
        if (((char *) max->ptr)[0] == '(') {
            spec->max = strtod((char *) max->ptr + 1, &eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod((char *) max->ptr, &eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
        }
    }

    return C_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparison, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. C_OK will be
  * returned.
  *
  * If the string is not a valid range C_ERR is returned, and the value
  * of *dest and *ex is undefined. */
int zslParseLexRangeItem(robj *item, sds *dest, int *ex) {
    char *c = item->ptr;

    switch (c[0]) {
        case '+':
            if (c[1] != '\0') return C_ERR;
            *ex = 1;
            *dest = shared.maxstring;
            return C_OK;
        case '-':
            if (c[1] != '\0') return C_ERR;
            *ex = 1;
            *dest = shared.minstring;
            return C_OK;
        case '(':
            *ex = 1;
            *dest = sdsnewlen(c + 1, sdslen(c) - 1);
            return C_OK;
        case '[':
            *ex = 0;
            *dest = sdsnewlen(c + 1, sdslen(c) - 1);
            return C_OK;
        default:
            return C_ERR;
    }
}

/* Free a lex range structure, must be called only after zelParseLexRange()
 * populated the structure with success (C_OK returned). */
void zslFreeLexRange(zlexrangespec *spec) {
    if (spec->min != shared.minstring &&
        spec->min != shared.maxstring)
        sdsfree(spec->min);
    if (spec->max != shared.minstring &&
        spec->max != shared.maxstring)
        sdsfree(spec->max);
}

/* Populate the lex rangespec according to the objects min and max.
 *
 * Return C_OK on success. On error C_ERR is returned.
 * When OK is returned the structure must be freed with zslFreeLexRange(),
 * otherwise no release is needed. */
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec) {
    /* The range can't be valid if objects are integer encoded.
     * Every item must start with ( or [. */
    if (min->encoding == OBJ_ENCODING_INT ||
        max->encoding == OBJ_ENCODING_INT)
        return C_ERR;

    spec->min = spec->max = NULL;
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) == C_ERR ||
        zslParseLexRangeItem(max, &spec->max, &spec->maxex) == C_ERR) {
        zslFreeLexRange(spec);
        return C_ERR;
    } else {
        return C_OK;
    }
}

/* This is just a wrapper to sdscmp() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of
 * -inf and +inf for strings */
int sdscmplex(sds a, sds b) {
    if (a == b) return 0;
    if (a == shared.minstring || b == shared.maxstring) return -1;
    if (a == shared.maxstring || b == shared.minstring) return 1;
    return sdscmp(a, b);
}

int zslLexValueGteMin(sds value, zlexrangespec *spec) {
    return spec->minex ?
           (sdscmplex(value, spec->min) > 0) :
           (sdscmplex(value, spec->min) >= 0);
}

int zslLexValueLteMax(sds value, zlexrangespec *spec) {
    return spec->maxex ?
           (sdscmplex(value, spec->max) < 0) :
           (sdscmplex(value, spec->max) <= 0);
}

/* Returns if there is a part of the zset is in the lex range. */
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    int cmp = sdscmplex(range->min, range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslLexValueGteMin(x->ele, range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslLexValueLteMax(x->ele, range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInLexRange(zsl, range)) return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
               !zslLexValueGteMin(x->level[i].forward->ele, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    if (!zslLexValueLteMax(x->ele, range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInLexRange(zsl, range)) return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
               zslLexValueLteMax(x->level[i].forward->ele, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    if (!zslLexValueGteMin(x->ele, range)) return NULL;
    return x;
}

/**-----------------------------------------------------------------------------
 * Ziplist-backed sorted set API   使用压缩列表的 API
 *----------------------------------------------------------------------------*/

double zzlStrtod(unsigned char *vstr, unsigned int vlen) {
    char buf[128];
    if (vlen > sizeof(buf))
        vlen = sizeof(buf);
    memcpy(buf, vstr, vlen);
    buf[vlen] = '\0';
    return strtod(buf, NULL);
}

/*
 * 取出 sptr 指向节点所保存的有序集合元素的分值
 */
double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    double score;

    serverAssert(sptr != NULL);

    // 取出 sptr 指向的 ziplist 节点的值
    serverAssert(ziplistGet(sptr, &vstr, &vlen, &vlong));

    // 如果保存的是字符串
    if (vstr) {
        // 字符串转 double
        score = zzlStrtod(vstr, vlen);

        // 如果保存的是整数
    } else {
        // double 值
        score = vlong;
    }

    // 返回分数
    return score;
}

/* Return a ziplist element as an SDS string. */
sds ziplistGetObject(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    serverAssert(sptr != NULL);
    serverAssert(ziplistGet(sptr, &vstr, &vlen, &vlong));

    if (vstr) {
        return sdsnewlen((char *) vstr, vlen);
    } else {
        return sdsfromlonglong(vlong);
    }
}

/* Compare element in sorted set with given element.
 *
 * 将 eptr 中的元素和 cstr 进行对比。
 *
 * 相等返回 0 ，
 * 不相等并且 eptr 的字符串比 cstr 大时，返回正整数。
 * 不相等并且 eptr 的字符串比 cstr 小时，返回负整数。
 */
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    serverAssert(ziplistGet(eptr, &vstr, &vlen, &vlong));
    if (vstr == NULL) {
        /* Store string representation of long long in buf. */
        vlen = ll2string((char *) vbuf, sizeof(vbuf), vlong);
        vstr = vbuf;
    }

    minlen = (vlen < clen) ? vlen : clen;
    cmp = memcmp(vstr, cstr, minlen);
    if (cmp == 0) return vlen - clen;
    return cmp;
}

/*
 * 返回 ziplist 存的跳表节点的元素个数
 *
 * 注意，除以 2 是因为使用 ziplist 存储有序数据需要成对的，一个存储元素，另一个存储分数
 */
unsigned int zzlLength(unsigned char *zl) {
    return ziplistLen(zl) / 2;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry.
 *
 * 获取下个成员和分值
 *
 * 如果后面已经没有元素，那么两个指针都被设为 NULL 。
 */
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    // 指向下个成员
    _eptr = ziplistNext(zl, *sptr);
    if (_eptr != NULL) {
        // 指向下个分数
        _sptr = ziplistNext(zl, _eptr);
        serverAssert(_sptr != NULL);
    } else {
        /* No next entry. */
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no next entry.
 *
 * 获取上个成员和分值
 */
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);
    _sptr = ziplistPrev(zl, *eptr);
    if (_sptr != NULL) {
        _eptr = ziplistPrev(zl, _sptr);
        serverAssert(_eptr != NULL);
    } else {
        /* No previous entry. */
        _eptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange.
 *
 * 如果给定的 ziplist 有至少一个节点符合 range 中指定的范围，那么返回 1 ，否则返回 0
 *
 */
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* Test for ranges that will always be empty. */
    // 排除总为空的范围值
    if (range->min > range->max ||
        (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // 取出 ziplist 中的最大分值，并和 range 的最大值对比
    // 即取出 ziplist 最后一个列表项，这个列表项存储的是分数
    p = ziplistIndex(zl, -1); /* Last score. */
    if (p == NULL) return 0; /* Empty sorted set */

    // 获取分数值
    score = zzlGetScore(p);
    // 判断 zset 中最大分数值 和 范围的最小项比较
    if (!zslValueGteMin(score, range))
        return 0;

    // 取出 ziplist 中的最小值，并和 range 的最小值进行对比
    // 即取出 ziplist 第二个列表项，这个列表项存储的是分数
    p = ziplistIndex(zl, 1); /* First score. */
    serverAssert(p != NULL);

    // 获取分数值
    score = zzlGetScore(p);
    // 判断 zset 中最小分数值 和 范围的最大项比较
    if (!zslValueLteMax(score, range))
        return 0;

    // ziplist 有至少一个节点符合范围
    return 1;
}

/* Find pointer to the first element contained in the specified range.
 * Returns NULL when no element is contained in the range.
 *
 * 返回第一个 score 值在给定范围内的节点
 *
 * 如果没有节点的 score 值在给定的范围内，返回 NULL
 */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
    // 从压缩列表表头开始遍历
    unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
    double score;

    /* If everything is out of range, return early. */
    if (!zzlIsInRange(zl, range)) return NULL;

    // 成员分数在 ziplist 中是从小到大排序的
    while (eptr != NULL) {
        // 获取分数列表项
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        // 取出分数
        score = zzlGetScore(sptr);

        // 判断当前分数是否大于范围的最小项
        if (zslValueGteMin(score, range)) {

            /* Check if score <= max. */
            // 不能超出范围的最大项
            if (zslValueLteMax(score, range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        // 移动到下个成员
        eptr = ziplistNext(zl, sptr);
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified range.
 * Returns NULL when no element is contained in the range.
 *
 * 返回 score 在给定范围的最后一个节点
 *
 */
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range) {
    // 从压缩列表的倒数第 2 项开始遍历
    unsigned char *eptr = ziplistIndex(zl, -2), *sptr;
    double score;

    /* If everything is out of range, return early. */
    if (!zzlIsInRange(zl, range)) return NULL;

    // 从后往前遍历，依次比较成员的分数是否在范围内
    while (eptr != NULL) {

        // 取出存储分数的列表项
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        // 取出分数
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score, range)) {
            /* Check if score >= min. */
            if (zslValueGteMin(score, range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        // 向前遍历成员
        sptr = ziplistPrev(zl, eptr);
        if (sptr != NULL)
            serverAssert((eptr = ziplistPrev(zl, sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec) {
    sds value = ziplistGetObject(p);
    int res = zslLexValueGteMin(value, spec);
    sdsfree(value);
    return res;
}

int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec) {
    sds value = ziplistGetObject(p);
    int res = zslLexValueLteMax(value, spec);
    sdsfree(value);
    return res;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *p;

    /* Test for ranges that will always be empty. */
    int cmp = sdscmplex(range->min, range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;

    p = ziplistIndex(zl, -2); /* Last element. */
    if (p == NULL) return 0;
    if (!zzlLexValueGteMin(p, range))
        return 0;

    p = ziplistIndex(zl, 0); /* First element. */
    serverAssert(p != NULL);
    if (!zzlLexValueLteMax(p, range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = ziplistIndex(zl, 0), *sptr;

    /* If everything is out of range, return early. */
    if (!zzlIsInLexRange(zl, range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueGteMin(eptr, range)) {
            /* Check if score <= max. */
            if (zzlLexValueLteMax(eptr, range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        sptr = ziplistNext(zl, eptr); /* This element score. Skip it. */
        serverAssert(sptr != NULL);
        eptr = ziplistNext(zl, sptr); /* Next element. */
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = ziplistIndex(zl, -2), *sptr;

    /* If everything is out of range, return early. */
    if (!zzlIsInLexRange(zl, range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueLteMax(eptr, range)) {
            /* Check if score >= min. */
            if (zzlLexValueGteMin(eptr, range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        sptr = ziplistPrev(zl, eptr);
        if (sptr != NULL)
            serverAssert((eptr = ziplistPrev(zl, sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/*
 * 从 ziplist 中查找 ele 成员，并将它的分值保存到 score
 *
 * 寻找成功返回指向成员 ele 的指针，查找失败返回 NULL 。
 */
unsigned char *zzlFind(unsigned char *zl, sds ele, double *score) {

    // 从压缩列表表头开始遍历，这里也就是保存成员的节点
    unsigned char *eptr = ziplistIndex(zl, 0), *sptr;

    // 遍历整个 ziplist ，查找元素
    while (eptr != NULL) {

        // 获取下个节点，这里也就是保存分数的节点
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        // 比对成员
        if (ziplistCompare(eptr, (unsigned char *) ele, sdslen(ele))) {
            /* Matching element, pull out score. */

            // 成员匹配，取出分值
            if (score != NULL) *score = zzlGetScore(sptr);
            return eptr;
        }

        /* Move to next element. */
        // 移动到下个节点
        eptr = ziplistNext(zl, sptr);
    }
    return NULL;
}

/* Delete (element,score) pair from ziplist. Use local copy of eptr because we
 * don't want to modify the one given as argument.
 *
 * 从 ziplist 中删除 eptr 所指定的有序集合元素，包括成员和分数
 */
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    unsigned char *p = eptr;

    /* TODO: add function to ziplist API to delete N elements from offset. */

    /**
     * 在使用压缩列表时，会先后使用两个压缩列表项依次存储元素和分数，因此删除的时候只需依次删除即可，
     * 因为列表项内部进行了原地更新 *p 所指向的位置
     */
    zl = ziplistDelete(zl, &p);
    zl = ziplistDelete(zl, &p);
    return zl;
}

/*
 * 将带有给定成员和分数的新节点插入到 eptr 所指向的节点的前面，
 * 如果 eptr 为 NULL ，那么将新节点插入到 ziplist 的末端。
 */
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, sds ele, double score) {
    unsigned char *sptr;
    char scorebuf[128];
    int scorelen;
    size_t offset;

    // 计算分值的字节长度
    scorelen = d2string(scorebuf, sizeof(scorebuf), score);

    // 插入到表尾，或者空表
    if (eptr == NULL) {
        /*
         * 数据样例： | member-1 | score-1 | member-2 | score-2 | ... | member-N | score-N |
         * 1 先推入成员
         * 2 在推入分数
         * 即 使用两个压缩列表项
         */
        zl = ziplistPush(zl, (unsigned char *) ele, sdslen(ele), ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *) scorebuf, scorelen, ZIPLIST_TAIL);

        // 插入到某个节点的前面
    } else {
        /* Keep offset relative to zl, as it might be re-allocated. */

        // 1 先推入成员
        offset = eptr - zl;
        zl = ziplistInsert(zl, eptr, (unsigned char *) ele, sdslen(ele));
        eptr = zl + offset;

        /* Insert score after the element. */
        // 2 将分值插入在成员之后
        serverAssert((sptr = ziplistNext(zl, eptr)) != NULL);
        zl = ziplistInsert(zl, sptr, (unsigned char *) scorebuf, scorelen);
    }
    return zl;
}

/* Insert (element,score) pair in ziplist. This function assumes the element is
 * not yet present in the list.
 *
 * 将 ele 成员和它的分数 score 添加到 ziplist 中，
 *
 * ziplist 中各个节点按 score 值从小到大排列
 *
 * 注意，该函数假设 ele 不存在于有序集
 *
 */
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score) {

    // 指向 ziplist 第一个节点，也就是有序集的 成员 域
    unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
    double s;

    // 遍历整个 ziplist
    while (eptr != NULL) {

        // 取出下个节点，即取出存储分数的节点
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);
        // 获取分数
        s = zzlGetScore(sptr);

        // 如果要插入数据的分数大于当前 s，则将数据插入到 eptr 前面
        // 让节点在 ziplist 里根据 score 从小到大排列
        if (s > score) {
            /* First element with score larger than score for element to be
             * inserted. This means we should take its spot in the list to
             * maintain ordering. */
            zl = zzlInsertAt(zl, eptr, ele, score);
            break;

            // 如果输入 score 和节点的 score 相同，则根据元素大小来决定新节点的插入位置
        } else if (s == score) {
            /* Ensure lexicographical ordering for elements. */
            if (zzlCompareElements(eptr, (unsigned char *) ele, sdslen(ele)) > 0) {
                zl = zzlInsertAt(zl, eptr, ele, score);
                break;
            }
        }

        // 执行到这里，说明输入 score 比节点的 score 大

        /* Move to next element. */
        // 下一个节点
        eptr = ziplistNext(zl, sptr);
    }

    /* Push on tail of list when it was not yet inserted. */
    // 输入的 score 比压缩列表中所有分数节点的值都大，那么将数据插入到末尾
    if (eptr == NULL)
        zl = zzlInsertAt(zl, NULL, ele, score);
    return zl;
}

/*
 * 删除 ziplist 中分值在指定范围内的元素
 *
 * deleted 不为 NULL 时，在删除完毕之后，将被删除元素的数量保存到 *deleted 中。
 */
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    // 指向 ziplist 中第一个符合范围的节点，也就是成员节点
    eptr = zzlFirstInRange(zl, range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    // 一直删除节点，直到遇到不在范围内的值为止
    // 注意，节点中的值都是有序的
    while ((sptr = ziplistNext(zl, eptr)) != NULL) {
        score = zzlGetScore(sptr);

        // 还在 range 最大的范围内
        if (zslValueLteMax(score, range)) {
            /* Delete both the element and the score. */
            // 删除元素和分数
            zl = ziplistDelete(zl, &eptr);
            zl = ziplistDelete(zl, &eptr);
            num++;

            // 不在 range 范围，结束
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    eptr = zzlFirstInLexRange(zl, range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    while ((sptr = ziplistNext(zl, eptr)) != NULL) {
        if (zzlLexValueLteMax(eptr, range)) {
            /* Delete both the element and the score. */
            zl = ziplistDelete(zl, &eptr);
            zl = ziplistDelete(zl, &eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based
 *
 * 删除 ziplist 中所有在给定排位范围内的元素
 *
 * start 和 end 索引都是包括在内的。并且它们都以 1 为起始值。
 *
 * 如果 deleted 不为 NULL ，那么在删除操作完成之后，将删除元素的数量保存到 *deleted 中
 *
 * 排位对于 ziplist 来说就是索引；对于跳表来说就是遍历经过节点的跨度累加
 */
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    unsigned int num = (end - start) + 1;
    if (deleted) *deleted = num;

    // ziplist 存储 zset 数据时，会一次存储元素和分数两个节点。
    // 因为 ziplist 的索引以 0 为起始值，而 zzl 的起始值为 1 ， 所以需要 start - 1
    zl = ziplistDeleteRange(zl, 2 * (start - 1), 2 * num);

    return zl;
}

/**-----------------------------------------------------------------------------
 * Common sorted set API   通用的 zset API
 *----------------------------------------------------------------------------*/

/*
 * 获取 zset 长度
 */
unsigned long zsetLength(const robj *zobj) {
    unsigned long length = 0;

    // 1 如果 zset 使用 ziplist 数据结构
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        length = zzlLength(zobj->ptr);

        // 2 如果是跳表
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        length = ((const zset *) zobj->ptr)->zsl->length;

        // 未知的
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return length;
}

/*
 * 将跳表对象 zobj 的底层编码转为 encoding
 */
void zsetConvert(robj *zobj, int encoding) {
    zset *zs;
    zskiplistNode *node, *next;
    sds ele;
    double score;

    // 1 已经是当前编码，无需转换
    if (zobj->encoding == encoding) return;

    // 2 从 ziplist 编码转为 skiplist 编码
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {

        // 获取数据指针，这里是 ziplist 结构地址
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != OBJ_ENCODING_SKIPLIST)
            serverPanic("Unknown target encoding");

        // 2.1 分配 zset 结构
        zs = zmalloc(sizeof(*zs));
        // 2.1.1 创建字典
        zs->dict = dictCreate(&zsetDictType, NULL);
        // 2.1.2 创建跳表
        zs->zsl = zslCreate();

        // 2.2 获取 ziplist 的第一个列表项，也就是第一个成员
        eptr = ziplistIndex(zl, 0);
        serverAssertWithInfo(NULL, zobj, eptr != NULL);
        // 2.3 获取 ziplist 的第一个列表项，也就是第一个分数
        sptr = ziplistNext(zl, eptr);
        serverAssertWithInfo(NULL, zobj, sptr != NULL);

        // 2.4 从前向后遍历 ziplist ,并将元素的成员和分值添加到有序集合中
        while (eptr != NULL) {
            // 取出分数
            score = zzlGetScore(sptr);
            // 取出成员，并根据成员的类型创建一个 sds
            serverAssertWithInfo(NULL, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen((char *) vstr, vlen);

            // 2.5 将成员和分数作为跳表的节点，添加到跳表中
            node = zslInsert(zs->zsl, score, ele);

            // 2.6 将成员作为 key，分数作为 value ，添加到 字典中
            serverAssert(dictAdd(zs->dict, ele, &node->score) == DICT_OK);

            // 2.7 移动指针，指向 ziplist 的下个列表项
            zzlNext(zl, &eptr, &sptr);
        }

        // 释放原来的 ziplist
        zfree(zobj->ptr);

        // 更新对象的数据指针，这里是 zset
        zobj->ptr = zs;
        // 更新编码方式 skiplist
        zobj->encoding = OBJ_ENCODING_SKIPLIST;

        // 从 skiplist 转为 ziplist 编码
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {

        // 创建一个空的 ziplist
        unsigned char *zl = ziplistNew();

        if (encoding != OBJ_ENCODING_ZIPLIST)
            serverPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the ziplist. */
        // 指向 zset
        zs = zobj->ptr;

        // 先释放字典，因为只需要跳跃表就可以遍历整个有序集合
        dictRelease(zs->dict);

        // 使用最低层的链表，获取首个节点
        node = zs->zsl->header->level[0].forward;

        // 释放跳跃表头和zset结构体中的跳表结构
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        // 遍历链表
        while (node) {
            // 将跳表节点中的成员和分数一次以两个列表项添加到 ziplist
            zl = zzlInsertAt(zl, NULL, node->ele, node->score);

            // 链表下个节点
            next = node->level[0].forward;

            // 释放无用的节点
            zslFreeNode(node);
            node = next;
        }

        // 释放 zset
        zfree(zs);

        // 更新对象的数据指针，这里是 ziplist
        zobj->ptr = zl;
        // 设置对象的编码类型为 ziplist
        zobj->encoding = OBJ_ENCODING_ZIPLIST;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* Convert the sorted set object into a ziplist if it is not already a ziplist
 * and if the number of elements and the maximum element size is within the
 * expected ranges.
 *
 * 在必要的情况下将 zset 转为 ziplist
 *
 */
void zsetConvertToZiplistIfNeeded(robj *zobj, size_t maxelelen) {
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) return;
    zset *zset = zobj->ptr;

    if (zset->zsl->length <= server.zset_max_ziplist_entries &&
        maxelelen <= server.zset_max_ziplist_value)

        // 将 zset 转为 ziplist
        zsetConvert(zobj, OBJ_ENCODING_ZIPLIST);
}

/* Return (by reference) the score of the specified member of the sorted set
 * storing it into *score. If the element does not exist C_ERR is returned
 * otherwise C_OK is returned and *score is correctly populated.
 * If 'zobj' or 'member' is NULL, C_ERR is returned.
 *
 * 获取成员的分数
 */
int zsetScore(robj *zobj, sds member, double *score) {
    if (!zobj || !member) return C_ERR;

    // 1 如果是 ziplist 遍历查找
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        if (zzlFind(zobj->ptr, member, score) == NULL) return C_ERR;

        // 2 如果是 siplist ，直接使用字典查找
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de = dictFind(zs->dict, member);
        if (de == NULL) return C_ERR;
        *score = *(double *) dictGetVal(de);
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return C_OK;
}

/* Add a new element or update the score of an existing element in a sorted
 * set, regardless of its encoding.
 *
 * 添加新元素或更新已排序集合中现有元素的得分，无论其编码如何。
 *
 * The set of flags change the command behavior. 
 *
 * The input flags are the following:
 *
 * ZADD_INCR: Increment the current element score by 'score' instead of updating
 *            the current element score. If the element does not exist, we
 *            assume 0 as previous score.
 * ZADD_NX:   Perform the operation only if the element does not exist.
 * ZADD_XX:   Perform the operation only if the element already exist.
 * ZADD_GT:   Perform the operation on existing elements only if the new score is 
 *            greater than the current score.
 * ZADD_LT:   Perform the operation on existing elements only if the new score is 
 *            less than the current score.
 *
 * When ZADD_INCR is used, the new score of the element is stored in
 * '*newscore' if 'newscore' is not NULL.
 *
 * The returned flags are the following:
 *
 * ZADD_NAN:     The resulting score is not a number.
 * ZADD_ADDED:   The element was added (not present before the call).
 * ZADD_UPDATED: The element score was updated.
 * ZADD_NOP:     No operation was performed because of NX or XX.
 *
 * Return value:
 *
 * The function returns 1 on success, and sets the appropriate flags
 * ADDED or UPDATED to signal what happened during the operation (note that
 * none could be set if we re-added an element using the same score it used
 * to have, or in the case a zero increment is used).
 *
 * The function returns 0 on error, currently only when the increment
 * produces a NAN condition, or when the 'score' value is NAN since the
 * start.
 *
 * The command as a side effect of adding a new element may convert the sorted
 * set internal encoding from ziplist to hashtable+skiplist.
 *
 * Memory management of 'ele':
 *
 * The function does not take ownership of the 'ele' SDS string, but copies
 * it if needed. */
int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore) {
    /* Turn options into simple to check vars. */
    int incr = (in_flags & ZADD_IN_INCR) != 0;
    int nx = (in_flags & ZADD_IN_NX) != 0;
    int xx = (in_flags & ZADD_IN_XX) != 0;
    int gt = (in_flags & ZADD_IN_GT) != 0;
    int lt = (in_flags & ZADD_IN_LT) != 0;
    *out_flags = 0; /* We'll return our response flags. */
    double curscore;

    /* NaN as input is an error regardless of all the other parameters. */
    if (isnan(score)) {
        *out_flags = ZADD_OUT_NAN;
        return 0;
    }

    /* Update the sorted set according to its encoding. */
    // 1 采用 ziplist
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        // 1.1 获取 ele 成员对应列表项，如果有匹配的，还会将对应的分数保存在 curscore
        // 处理已存在的情况
        if ((eptr = zzlFind(zobj->ptr, ele, &curscore)) != NULL) {
            /* NX? Return, same element already exists. */
            if (nx) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            /* Prepare the score for the increment if needed. */
            // 如果需要，准备增量的分数
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *out_flags |= ZADD_OUT_NAN;
                    return 0;
                }
            }

            /* GT/LT? Only update if score is greater/less than current. */
            /// 只有更新分数大于/小于当前。
            if ((lt && score >= curscore) || (gt && score <= curscore)) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            if (newscore) *newscore = score;

            /* Remove and re-insert when score changed. */
            // 分数改变，则先删除再插入
            if (score != curscore) {
                zobj->ptr = zzlDelete(zobj->ptr, eptr);
                zobj->ptr = zzlInsert(zobj->ptr, ele, score);
                *out_flags |= ZADD_OUT_UPDATED;
            }
            return 1;

            // 1.2 不存在，将成员和分数添加到 ziplist 中
        } else if (!xx) {
            /* Optimize: check if the element is too large or the list
             * becomes too long *before* executing zzlInsert. */
            zobj->ptr = zzlInsert(zobj->ptr, ele, score);

            // 添加之后，如果 ziplist 存的跳表节点的元素个数 > zset 使用 ziplist 存储的最大项数，
            // 或 成员 ele 的长度 > zset 使用 ziplist 保存成员的大小上限
            if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries ||
                sdslen(ele) > server.zset_max_ziplist_value)
                // 将 ziplist 转为 skiplist
                zsetConvert(zobj, OBJ_ENCODING_SKIPLIST);

            if (newscore) *newscore = score;
            *out_flags |= ZADD_OUT_ADDED;
            return 1;
        } else {
            *out_flags |= ZADD_OUT_NOP;
            return 1;
        }

        // 采用 skiplist
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplistNode *znode;
        dictEntry *de;

        // 从跳表的哈希表中查找元素是否存在
        de = dictFind(zs->dict, ele);

        // 元素已经存在
        if (de != NULL) {
            /* NX? Return, same element already exists. */
            if (nx) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            // 从哈希表中查询元素的权重
            curscore = *(double *) dictGetVal(de);

            /* Prepare the score for the increment if needed. */
            // 如果要更新元素权重值
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *out_flags |= ZADD_OUT_NAN;
                    return 0;
                }
            }

            /* GT/LT? Only update if score is greater/less than current. */
            if ((lt && score >= curscore) || (gt && score <= curscore)) {
                *out_flags |= ZADD_OUT_NOP;
                return 1;
            }

            if (newscore) *newscore = score;

            /* Remove and re-insert when score changes. */
            // 如果分数发生变化了
            if (score != curscore) {
                // 更新跳表节点
                znode = zslUpdateScore(zs->zsl, curscore, ele, score);
                /* Note that we did not removed the original element from
                 * the hash table representing the sorted set, so we just
                 * update the score. */
                // 让哈希表元素的值指向跳表节点的权重
                dictGetVal(de) = &znode->score; /* Update score ptr. */
                *out_flags |= ZADD_OUT_UPDATED;
            }
            return 1;

            // 不存在，将元素先后插入到跳表和哈希表中
            // 注意，Redis 并没有把哈希表的操作嵌入到跳表本身的操作函数中，而是在 zsetAdd 函数中依次执行以上两个函数。
            // 这样设计的好处是保持了跳表和哈希表两者操作的独立性。
        } else if (!xx) {
            ele = sdsdup(ele);

            // 调用跳表元素插入函数 zslInsert
            znode = zslInsert(zs->zsl, score, ele);

            // 调用哈希表元素插入函数 dictAdd
            serverAssert(dictAdd(zs->dict, ele, &znode->score) == DICT_OK);
            *out_flags |= ZADD_OUT_ADDED;
            if (newscore) *newscore = score;
            return 1;

        } else {
            *out_flags |= ZADD_OUT_NOP;
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* Never reached. */
}

/* Deletes the element 'ele' from the sorted set encoded as a skiplist+dict,
 * returning 1 if the element existed and was deleted, 0 otherwise (the
 * element was not there). It does not resize the dict after deleting the
 * element.
 *
 * 从 zset 中删除指定的成员
 */
static int zsetRemoveFromSkiplist(zset *zs, sds ele) {
    dictEntry *de;
    double score;

    // 1 从字典中删除 ele （eLe 作为 key ）
    de = dictUnlink(zs->dict, ele);
    if (de != NULL) {
        /* Get the score in order to delete from the skiplist later. */
        score = *(double *) dictGetVal(de);

        /* Delete from the hash table and later from the skiplist.
         * Note that the order is important: deleting from the skiplist
         * actually releases the SDS string representing the element,
         * which is shared between the skiplist and the hash table, so
         * we need to delete from the skiplist as the final step. */
        // 释放 de 的 key 和 value，以及 de
        dictFreeUnlinkedEntry(zs->dict, de);

        /* Delete from skiplist. */
        // 2 从跳表中删除 ele
        int retval = zslDelete(zs->zsl, score, ele, NULL);
        serverAssert(retval);

        return 1;
    }

    return 0;
}

/* Delete the element 'ele' from the sorted set, returning 1 if the element
 * existed and was deleted, 0 otherwise (the element was not there).
 *
 * 从 zset 中删除指定的成员，返回 1 表示删除成功，否则，返回 0
 *
 */
int zsetDel(robj *zobj, sds ele) {

    // 1 如果是 ziplist 编码
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        // 查询存不存指定的 ele
        if ((eptr = zzlFind(zobj->ptr, ele, NULL)) != NULL) {
            // 存在则从 ziplist 中删除
            zobj->ptr = zzlDelete(zobj->ptr, eptr);
            return 1;
        }

        // 2 如果是 siplist 编码
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        if (zsetRemoveFromSkiplist(zs, ele)) {
            if (htNeedsResize(zs->dict)) dictResize(zs->dict);
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* No such element found. */
}

/* Given a sorted set object returns the 0-based rank of the object or
 * -1 if the object does not exist.
 *
 * For rank we mean the position of the element in the sorted collection
 * of elements. So the first element has rank 0, the second rank 1, and so
 * forth up to length-1 elements.
 *
 * If 'reverse' is false, the rank is returned considering as first element
 * the one with the lowest score. Otherwise if 'reverse' is non-zero
 * the rank is computed considering as element with rank 0 the one with
 * the highest score. */
long zsetRank(robj *zobj, sds ele, int reverse) {
    unsigned long llen;
    unsigned long rank;

    llen = zsetLength(zobj);

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        eptr = ziplistIndex(zl, 0);
        serverAssert(eptr != NULL);
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        rank = 1;
        while (eptr != NULL) {
            if (ziplistCompare(eptr, (unsigned char *) ele, sdslen(ele)))
                break;
            rank++;
            zzlNext(zl, &eptr, &sptr);
        }

        if (eptr != NULL) {
            if (reverse)
                return llen - rank;
            else
                return rank - 1;
        } else {
            return -1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        de = dictFind(zs->dict, ele);
        if (de != NULL) {
            score = *(double *) dictGetVal(de);
            rank = zslGetRank(zsl, score, ele);
            /* Existing elements always have a rank. */
            serverAssert(rank != 0);
            if (reverse)
                return llen - rank;
            else
                return rank - 1;
        } else {
            return -1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a sorted set object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
robj *zsetDup(robj *o) {
    robj *zobj;
    zset *zs;
    zset *new_zs;

    serverAssert(o->type == OBJ_ZSET);

    /* Create a new sorted set object that have the same encoding as the original object's encoding */
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        size_t sz = ziplistBlobLen(zl);
        unsigned char *new_zl = zmalloc(sz);
        memcpy(new_zl, zl, sz);
        zobj = createObject(OBJ_ZSET, new_zl);
        zobj->encoding = OBJ_ENCODING_ZIPLIST;
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zobj = createZsetObject();
        zs = o->ptr;
        new_zs = zobj->ptr;
        dictExpand(new_zs->dict, dictSize(zs->dict));
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        sds ele;
        long llen = zsetLength(o);

        /* We copy the skiplist elements from the greatest to the
         * smallest (that's trivial since the elements are already ordered in
         * the skiplist): this improves the load process, since the next loaded
         * element will always be the smaller, so adding to the skiplist
         * will always immediately stop at the head, making the insertion
         * O(1) instead of O(log(N)). */
        ln = zsl->tail;
        while (llen--) {
            ele = ln->ele;
            sds new_ele = sdsdup(ele);
            zskiplistNode *znode = zslInsert(new_zs->zsl, ln->score, new_ele);
            dictAdd(new_zs->dict, new_ele, &znode->score);
            ln = ln->backward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return zobj;
}

/* callback for to check the ziplist doesn't have duplicate recoreds */
static int _zsetZiplistValidateIntegrity(unsigned char *p, void *userdata) {
    struct {
        long count;
        dict *fields;
    } *data = userdata;

    /* Even records are field names, add to dict and check that's not a dup */
    if (((data->count) & 1) == 0) {
        unsigned char *str;
        unsigned int slen;
        long long vll;
        if (!ziplistGet(p, &str, &slen, &vll))
            return 0;
        sds field = str ? sdsnewlen(str, slen) : sdsfromlonglong(vll);;
        if (dictAdd(data->fields, field, NULL) != DICT_OK) {
            /* Duplicate, return an error */
            sdsfree(field);
            return 0;
        }
    }

    (data->count)++;
    return 1;
}

/* Validate the integrity of the data structure.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
int zsetZiplistValidateIntegrity(unsigned char *zl, size_t size, int deep) {
    if (!deep)
        return ziplistValidateIntegrity(zl, size, 0, NULL, NULL);

    /* Keep track of the field names to locate duplicate ones */
    struct {
        long count;
        dict *fields;
    } data = {0, dictCreate(&hashDictType, NULL)};

    int ret = ziplistValidateIntegrity(zl, size, 1, _zsetZiplistValidateIntegrity, &data);

    /* make sure we have an even number of records. */
    if (data.count & 1)
        ret = 0;

    dictRelease(data.fields);
    return ret;
}

/* Create a new sds string from the ziplist entry. */
sds zsetSdsFromZiplistEntry(ziplistEntry *e) {
    return e->sval ? sdsnewlen(e->sval, e->slen) : sdsfromlonglong(e->lval);
}

/* Reply with bulk string from the ziplist entry. */
void zsetReplyFromZiplistEntry(client *c, ziplistEntry *e) {
    if (e->sval)
        addReplyBulkCBuffer(c, e->sval, e->slen);
    else
        addReplyBulkLongLong(c, e->lval);
}


/* Return random element from a non empty zset.
 * 'key' and 'val' will be set to hold the element.
 * The memory in `key` is not to be freed or modified by the caller.
 * 'score' can be NULL in which case it's not extracted. */
void zsetTypeRandomElement(robj *zsetobj, unsigned long zsetsize, ziplistEntry *key, double *score) {
    if (zsetobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zsetobj->ptr;
        dictEntry *de = dictGetFairRandomKey(zs->dict);
        sds s = dictGetKey(de);
        key->sval = (unsigned char *) s;
        key->slen = sdslen(s);
        if (score)
            *score = *(double *) dictGetVal(de);
    } else if (zsetobj->encoding == OBJ_ENCODING_ZIPLIST) {
        ziplistEntry val;
        ziplistRandomPair(zsetobj->ptr, zsetsize, key, &val);
        if (score) {
            if (val.sval) {
                *score = zzlStrtod(val.sval, val.slen);
            } else {
                *score = (double) val.lval;
            }
        }
    } else {
        serverPanic("Unknown zset encoding");
    }
}

/**-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY.
 *
 * zset 添加元素
 */
void zaddGenericCommand(client *c, int flags) {
    static char *nanerr = "resulting score is not a number (NaN)";
    robj *key = c->argv[1];
    robj *zobj;
    sds ele;
    double score = 0, *scores = NULL;
    int j, elements, ch = 0;
    int scoreidx = 0;
    /* The following vars are used in order to track what the command actually
     * did during the execution, to reply to the client and to trigger the
     * notification of keyspace change. */
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    int processed = 0;  /* Number of elements processed, may remain zero with
                           options like XX. */

    /* Parse options. At the end 'scoreidx' is set to the argument position
     * of the score of the first score-element pair. */
    scoreidx = 2;
    while (scoreidx < c->argc) {
        char *opt = c->argv[scoreidx]->ptr;
        if (!strcasecmp(opt, "nx")) flags |= ZADD_IN_NX;
        else if (!strcasecmp(opt, "xx")) flags |= ZADD_IN_XX;
        else if (!strcasecmp(opt, "ch")) ch = 1; /* Return num of elements added or updated. */
        else if (!strcasecmp(opt, "incr")) flags |= ZADD_IN_INCR;
        else if (!strcasecmp(opt, "gt")) flags |= ZADD_IN_GT;
        else if (!strcasecmp(opt, "lt")) flags |= ZADD_IN_LT;
        else break;
        scoreidx++;
    }

    /* Turn options into simple to check vars. */
    int incr = (flags & ZADD_IN_INCR) != 0;
    int nx = (flags & ZADD_IN_NX) != 0;
    int xx = (flags & ZADD_IN_XX) != 0;
    int gt = (flags & ZADD_IN_GT) != 0;
    int lt = (flags & ZADD_IN_LT) != 0;

    /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. */
    elements = c->argc - scoreidx;

    // 输入的 score -member 参数必须是成对出现
    if (elements % 2 || !elements) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    elements /= 2; /* Now this holds the number of score-element pairs. */

    /* Check for incompatible options. */
    if (nx && xx) {
        addReplyError(c,
                      "XX and NX options at the same time are not compatible");
        return;
    }

    if ((gt && nx) || (lt && nx) || (gt && lt)) {
        addReplyError(c,
                      "GT, LT, and/or NX options at the same time are not compatible");
        return;
    }
    /* Note that XX is compatible with either GT or LT */

    if (incr && elements > 1) {
        addReplyError(c,
                      "INCR option supports a single increment-element pair");
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    // 取出所有输入的 score 分值
    scores = zmalloc(sizeof(double) * elements);
    for (j = 0; j < elements; j++) {
        // 每对的第 2 个
        if (getDoubleFromObjectOrReply(c, c->argv[scoreidx + j * 2], &scores[j], NULL)
            != C_OK)
            goto cleanup;
    }

    /* Lookup the key and create the sorted set if does not exist. */
    // 取出 key 对应的有序集合对象，
    // todo 这个 key 到底是啥？
    zobj = lookupKeyWrite(c->db, key);
    if (checkType(c, zobj, OBJ_ZSET)) goto cleanup;

    // 有序集合不存在，创建新有序集合
    if (zobj == NULL) {
        if (xx) goto reply_to_client; /* No key + XX option: nothing to do. */

        // 使用 zset 结构
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(c->argv[scoreidx + 1]->ptr)) {
            zobj = createZsetObject();

            // 使用 ziplist 结构
        } else {
            zobj = createZsetZiplistObject();
        }

        // 关联对象到 db
        dbAdd(c->db, key, zobj);
    }

    // 处理所有元素
    for (j = 0; j < elements; j++) {
        double newscore;
        score = scores[j];
        int retflags = 0;

        // 获取成员
        ele = c->argv[scoreidx + 1 + j * 2]->ptr;

        // todo 将成员和分数添加到 zobj 数据结构中
        int retval = zsetAdd(zobj, score, ele, flags, &retflags, &newscore);

        if (retval == 0) {
            addReplyError(c, nanerr);
            goto cleanup;
        }
        if (retflags & ZADD_OUT_ADDED) added++;
        if (retflags & ZADD_OUT_UPDATED) updated++;
        if (!(retflags & ZADD_OUT_NOP)) processed++;
        score = newscore;
    }
    server.dirty += (added + updated);

    reply_to_client:
    if (incr) { /* ZINCRBY or INCR option. */
        if (processed)
            addReplyDouble(c, score);
        else
            addReplyNull(c);
    } else { /* ZADD. */
        addReplyLongLong(c, ch ? added + updated : added);
    }

    cleanup:
    zfree(scores);
    if (added || updated) {
        signalModifiedKey(c, c->db, key);
        notifyKeyspaceEvent(NOTIFY_ZSET,
                            incr ? "zincr" : "zadd", key, c->db->id);
    }
}

void zaddCommand(client *c) {
    zaddGenericCommand(c, ZADD_IN_NONE);
}

void zincrbyCommand(client *c) {
    zaddGenericCommand(c, ZADD_IN_INCR);
}

/*
 * zset 删除元素
 */
void zremCommand(client *c) {
    // 获取 key 对象
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    // 根据 key 获取有序集合对象
    if ((zobj = lookupKeyWriteOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    // 遍历所有输入元素
    for (j = 2; j < c->argc; j++) {
        // 从 zobj 中删除成员
        if (zsetDel(zobj, c->argv[j]->ptr)) deleted++;

        // zobj 已清空，将有序集合从数据库中删除
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db, key);
            keyremoved = 1;
            break;
        }
    }

    // 如果有至少一个元素被删除的话，那么执行以下代码
    if (deleted) {
        notifyKeyspaceEvent(NOTIFY_ZSET, "zrem", key, c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
        signalModifiedKey(c, c->db, key);
        server.dirty += deleted;
    }

    // 回复被删除元素的数量
    addReplyLongLong(c, deleted);
}

typedef enum {
    ZRANGE_AUTO = 0,
    ZRANGE_RANK,
    ZRANGE_SCORE,
    ZRANGE_LEX,
} zrange_type;

/* Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX commands. */
void zremrangeGenericCommand(client *c, zrange_type rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted = 0;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;
    char *notify_type = NULL;

    /* Step 1: Parse the range. */
    if (rangetype == ZRANGE_RANK) {
        notify_type = "zremrangebyrank";
        if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
            (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK))
            return;
    } else if (rangetype == ZRANGE_SCORE) {
        notify_type = "zremrangebyscore";
        if (zslParseRange(c->argv[2], c->argv[3], &range) != C_OK) {
            addReplyError(c, "min or max is not a float");
            return;
        }
    } else if (rangetype == ZRANGE_LEX) {
        notify_type = "zremrangebylex";
        if (zslParseLexRange(c->argv[2], c->argv[3], &lexrange) != C_OK) {
            addReplyError(c, "min or max not valid string range item");
            return;
        }
    } else {
        serverPanic("unknown rangetype %d", (int) rangetype);
    }

    /* Step 2: Lookup & range sanity checks if needed. */
    if ((zobj = lookupKeyWriteOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        goto cleanup;

    if (rangetype == ZRANGE_RANK) {
        /* Sanitize indexes. */
        llen = zsetLength(zobj);
        if (start < 0) start = llen + start;
        if (end < 0) end = llen + end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen) {
            addReply(c, shared.czero);
            goto cleanup;
        }
        if (end >= llen) end = llen - 1;
    }

    /* Step 3: Perform the range deletion operation. */
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        switch (rangetype) {
            case ZRANGE_AUTO:
            case ZRANGE_RANK:
                zobj->ptr = zzlDeleteRangeByRank(zobj->ptr, start + 1, end + 1, &deleted);
                break;
            case ZRANGE_SCORE:
                zobj->ptr = zzlDeleteRangeByScore(zobj->ptr, &range, &deleted);
                break;
            case ZRANGE_LEX:
                zobj->ptr = zzlDeleteRangeByLex(zobj->ptr, &lexrange, &deleted);
                break;
        }
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(c->db, key);
            keyremoved = 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        switch (rangetype) {
            case ZRANGE_AUTO:
            case ZRANGE_RANK:
                deleted = zslDeleteRangeByRank(zs->zsl, start + 1, end + 1, zs->dict);
                break;
            case ZRANGE_SCORE:
                deleted = zslDeleteRangeByScore(zs->zsl, &range, zs->dict);
                break;
            case ZRANGE_LEX:
                deleted = zslDeleteRangeByLex(zs->zsl, &lexrange, zs->dict);
                break;
        }
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db, key);
            keyremoved = 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* Step 4: Notifications and reply. */
    if (deleted) {
        signalModifiedKey(c, c->db, key);
        notifyKeyspaceEvent(NOTIFY_ZSET, notify_type, key, c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
    }
    server.dirty += deleted;
    addReplyLongLong(c, deleted);

    cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
}

void zremrangebyrankCommand(client *c) {
    zremrangeGenericCommand(c, ZRANGE_RANK);
}

void zremrangebyscoreCommand(client *c) {
    zremrangeGenericCommand(c, ZRANGE_SCORE);
}

void zremrangebylexCommand(client *c) {
    zremrangeGenericCommand(c, ZRANGE_LEX);
}

typedef struct {
    robj *subject;
    int type; /* Set, sorted set */
    int encoding;
    double weight;

    union {
        /* Set iterators. */
        union _iterset {
            struct {
                intset *is;
                int ii;
            } is;
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        union _iterzset {
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;


/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. The dirty flag for the long long value is
 * special, since long long values don't need cleanup. Instead, it means that
 * we already checked that "ell" holds a long long, or tried to convert another
 * representation into a long long value. When this was successful,
 * OPVAL_VALID_LL is set as well. */
#define OPVAL_DIRTY_SDS 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* Private buffer. */
    sds ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    double score;
} zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;

void zuiInitIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            it->is.is = op->subject->ptr;
            it->is.ii = 0;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        /* Sorted sets are traversed in reverse order to optimize for
         * the insertion of the elements in a new list as in
         * ZDIFF/ZINTER/ZUNION */
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = ziplistIndex(it->zl.zl, -2);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = ziplistNext(it->zl.zl, it->zl.eptr);
                serverAssert(it->zl.sptr != NULL);
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->tail;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

void zuiClearIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            UNUSED(it); /* skip */
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

unsigned long zuiLength(zsetopsrc *op) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            return intsetLen(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            return dictSize(ht);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            return zzlLength(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            return zs->zsl->length;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort. */
int zuiNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL)
        return 0;

    if (val->flags & OPVAL_DIRTY_SDS)
        sdsfree(val->ele);

    memset(val, 0, sizeof(zsetopval));

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            int64_t ell;

            if (!intsetGet(it->is.is, it->is.ii, &ell))
                return 0;
            val->ell = ell;
            val->score = 1.0;

            /* Move to next element. */
            it->is.ii++;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            if (it->ht.de == NULL)
                return 0;
            val->ele = dictGetKey(it->ht.de);
            val->score = 1.0;

            /* Move to next element. */
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            /* No need to check both, but better be explicit. */
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;
            serverAssert(ziplistGet(it->zl.eptr, &val->estr, &val->elen, &val->ell));
            val->score = zzlGetScore(it->zl.sptr);

            /* Move to next element (going backwards, see zuiInitIterator). */
            zzlPrev(it->zl.zl, &it->zl.eptr, &it->zl.sptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            if (it->sl.node == NULL)
                return 0;
            val->ele = it->sl.node->ele;
            val->score = it->sl.node->score;

            /* Move to next element. (going backwards, see zuiInitIterator) */
            it->sl.node = it->sl.node->backward;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
    return 1;
}

int zuiLongLongFromValue(zsetopval *val) {
    if (!(val->flags & OPVAL_DIRTY_LL)) {
        val->flags |= OPVAL_DIRTY_LL;

        if (val->ele != NULL) {
            if (string2ll(val->ele, sdslen(val->ele), &val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else if (val->estr != NULL) {
            if (string2ll((char *) val->estr, val->elen, &val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else {
            /* The long long was already set, flag as valid. */
            val->flags |= OPVAL_VALID_LL;
        }
    }
    return val->flags & OPVAL_VALID_LL;
}

sds zuiSdsFromValue(zsetopval *val) {
    if (val->ele == NULL) {
        if (val->estr != NULL) {
            val->ele = sdsnewlen((char *) val->estr, val->elen);
        } else {
            val->ele = sdsfromlonglong(val->ell);
        }
        val->flags |= OPVAL_DIRTY_SDS;
    }
    return val->ele;
}

/* This is different from zuiSdsFromValue since returns a new SDS string
 * which is up to the caller to free. */
sds zuiNewSdsFromValue(zsetopval *val) {
    if (val->flags & OPVAL_DIRTY_SDS) {
        /* We have already one to return! */
        sds ele = val->ele;
        val->flags &= ~OPVAL_DIRTY_SDS;
        val->ele = NULL;
        return ele;
    } else if (val->ele) {
        return sdsdup(val->ele);
    } else if (val->estr) {
        return sdsnewlen((char *) val->estr, val->elen);
    } else {
        return sdsfromlonglong(val->ell);
    }
}

int zuiBufferFromValue(zsetopval *val) {
    if (val->estr == NULL) {
        if (val->ele != NULL) {
            val->elen = sdslen(val->ele);
            val->estr = (unsigned char *) val->ele;
        } else {
            val->elen = ll2string((char *) val->_buf, sizeof(val->_buf), val->ell);
            val->estr = val->_buf;
        }
    }
    return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise. */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            if (zuiLongLongFromValue(val) &&
                intsetFind(op->subject->ptr, val->ell)) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            zuiSdsFromValue(val);
            if (dictFind(ht, val->ele) != NULL) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        zuiSdsFromValue(val);

        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            if (zzlFind(op->subject->ptr, val->ele, score) != NULL) {
                /* Score is already set by zzlFind. */
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            dictEntry *de;
            if ((de = dictFind(zs->dict, val->ele)) != NULL) {
                *score = *(double *) dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

int zuiCompareByCardinality(const void *s1, const void *s2) {
    unsigned long first = zuiLength((zsetopsrc *) s1);
    unsigned long second = zuiLength((zsetopsrc *) s2);
    if (first > second) return 1;
    if (first < second) return -1;
    return 0;
}

static int zuiCompareByRevCardinality(const void *s1, const void *s2) {
    return zuiCompareByCardinality(s1, s2) * -1;
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

inline static void zunionInterAggregate(double *target, double val, int aggregate) {
    if (aggregate == REDIS_AGGR_SUM) {
        *target = *target + val;
        /* The result of adding two doubles is NaN when one variable
         * is +inf and the other is -inf. When these numbers are added,
         * we maintain the convention of the result being 0.0. */
        if (isnan(*target)) *target = 0.0;
    } else if (aggregate == REDIS_AGGR_MIN) {
        *target = val < *target ? val : *target;
    } else if (aggregate == REDIS_AGGR_MAX) {
        *target = val > *target ? val : *target;
    } else {
        /* safety net */
        serverPanic("Unknown ZUNION/INTER aggregate type");
    }
}

static int zsetDictGetMaxElementLength(dict *d) {
    dictIterator *di;
    dictEntry *de;
    size_t maxelelen = 0;

    di = dictGetIterator(d);

    while ((de = dictNext(di)) != NULL) {
        sds ele = dictGetKey(de);
        if (sdslen(ele) > maxelelen) maxelelen = sdslen(ele);
    }

    dictReleaseIterator(di);

    return maxelelen;
}

static void zdiffAlgorithm1(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen) {
    /* DIFF Algorithm 1:
     *
     * We perform the diff by iterating all the elements of the first set,
     * and only adding it to the target set if the element does not exist
     * into all the other sets.
     *
     * This way we perform at max N*M operations, where N is the size of
     * the first set, and M the number of sets.
     *
     * There is also a O(K*log(K)) cost for adding the resulting elements
     * to the target set, where K is the final size of the target set.
     *
     * The final complexity of this algorithm is O(N*M + K*log(K)). */
    int j;
    zsetopval zval;
    zskiplistNode *znode;
    sds tmp;

    /* With algorithm 1 it is better to order the sets to subtract
     * by decreasing size, so that we are more likely to find
     * duplicated elements ASAP. */
    qsort(src + 1, setnum - 1, sizeof(zsetopsrc), zuiCompareByRevCardinality);

    memset(&zval, 0, sizeof(zval));
    zuiInitIterator(&src[0]);
    while (zuiNext(&src[0], &zval)) {
        double value;
        int exists = 0;

        for (j = 1; j < setnum; j++) {
            /* It is not safe to access the zset we are
             * iterating, so explicitly check for equal object.
             * This check isn't really needed anymore since we already
             * check for a duplicate set in the zsetChooseDiffAlgorithm
             * function, but we're leaving it for future-proofing. */
            if (src[j].subject == src[0].subject ||
                zuiFind(&src[j], &zval, &value)) {
                exists = 1;
                break;
            }
        }

        if (!exists) {
            tmp = zuiNewSdsFromValue(&zval);
            znode = zslInsert(dstzset->zsl, zval.score, tmp);
            dictAdd(dstzset->dict, tmp, &znode->score);
            if (sdslen(tmp) > *maxelelen) *maxelelen = sdslen(tmp);
        }
    }
    zuiClearIterator(&src[0]);
}


static void zdiffAlgorithm2(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen) {
    /* DIFF Algorithm 2:
     *
     * Add all the elements of the first set to the auxiliary set.
     * Then remove all the elements of all the next sets from it.
     *

     * This is O(L + (N-K)log(N)) where L is the sum of all the elements in every
     * set, N is the size of the first set, and K is the size of the result set.
     *
     * Note that from the (L-N) dict searches, (N-K) got to the zsetRemoveFromSkiplist
     * which costs log(N)
     *
     * There is also a O(K) cost at the end for finding the largest element
     * size, but this doesn't change the algorithm complexity since K < L, and
     * O(2L) is the same as O(L). */
    int j;
    int cardinality = 0;
    zsetopval zval;
    zskiplistNode *znode;
    sds tmp;

    for (j = 0; j < setnum; j++) {
        if (zuiLength(&src[j]) == 0) continue;

        memset(&zval, 0, sizeof(zval));
        zuiInitIterator(&src[j]);
        while (zuiNext(&src[j], &zval)) {
            if (j == 0) {
                tmp = zuiNewSdsFromValue(&zval);
                znode = zslInsert(dstzset->zsl, zval.score, tmp);
                dictAdd(dstzset->dict, tmp, &znode->score);
                cardinality++;
            } else {
                tmp = zuiSdsFromValue(&zval);
                if (zsetRemoveFromSkiplist(dstzset, tmp)) {
                    cardinality--;
                }
            }

            /* Exit if result set is empty as any additional removal
                * of elements will have no effect. */
            if (cardinality == 0) break;
        }
        zuiClearIterator(&src[j]);

        if (cardinality == 0) break;
    }

    /* Redize dict if needed after removing multiple elements */
    if (htNeedsResize(dstzset->dict)) dictResize(dstzset->dict);

    /* Using this algorithm, we can't calculate the max element as we go,
     * we have to iterate through all elements to find the max one after. */
    *maxelelen = zsetDictGetMaxElementLength(dstzset->dict);
}

static int zsetChooseDiffAlgorithm(zsetopsrc *src, long setnum) {
    int j;

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M + K*log(K)) where N is the size of the
     * first set, M the total number of sets, and K is the size of the
     * result set.
     *
     * Algorithm 2 is O(L + (N-K)log(N)) where L is the total number of elements
     * in all the sets, N is the size of the first set, and K is the size of the
     * result set.
     *
     * We compute what is the best bet with the current input here. */
    long long algo_one_work = 0;
    long long algo_two_work = 0;

    for (j = 0; j < setnum; j++) {
        /* If any other set is equal to the first set, there is nothing to be
         * done, since we would remove all elements anyway. */
        if (j > 0 && src[0].subject == src[j].subject) {
            return 0;
        }

        algo_one_work += zuiLength(&src[0]);
        algo_two_work += zuiLength(&src[j]);
    }

    /* Algorithm 1 has better constant times and performs less operations
     * if there are elements in common. Give it some advantage. */
    algo_one_work /= 2;
    return (algo_one_work <= algo_two_work) ? 1 : 2;
}

static void zdiff(zsetopsrc *src, long setnum, zset *dstzset, size_t *maxelelen) {
    /* Skip everything if the smallest input is empty. */
    if (zuiLength(&src[0]) > 0) {
        int diff_algo = zsetChooseDiffAlgorithm(src, setnum);
        if (diff_algo == 1) {
            zdiffAlgorithm1(src, setnum, dstzset, maxelelen);
        } else if (diff_algo == 2) {
            zdiffAlgorithm2(src, setnum, dstzset, maxelelen);
        } else if (diff_algo != 0) {
            serverPanic("Unknown algorithm");
        }
    }
}

uint64_t dictSdsHash(const void *key);

int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);

dictType setAccumulatorDictType = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        NULL,                      /* key destructor */
        NULL,                      /* val destructor */
        NULL                       /* allow to expand */
};

/* The zunionInterDiffGenericCommand() function is called in order to implement the
 * following commands: ZUNION, ZINTER, ZDIFF, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE.
 *
 * 'numkeysIndex' parameter position of key number. for ZUNION/ZINTER/ZDIFF command,
 * this value is 1, for ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE command, this value is 2.
 *
 * 'op' SET_OP_INTER, SET_OP_UNION or SET_OP_DIFF.
 */
void zunionInterDiffGenericCommand(client *c, robj *dstkey, int numkeysIndex, int op) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    sds tmp;
    size_t maxelelen = 0;
    robj *dstobj;
    zset *dstzset;
    zskiplistNode *znode;
    int withscores = 0;

    /* expect setnum input keys to be given */
    if ((getLongFromObjectOrReply(c, c->argv[numkeysIndex], &setnum, NULL) != C_OK))
        return;

    if (setnum < 1) {
        addReplyErrorFormat(c,
                            "at least 1 input key is needed for %s", c->cmd->name);
        return;
    }

    /* test if the expected number of keys would overflow */
    if (setnum > (c->argc - (numkeysIndex + 1))) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    /* read keys to be used for input */
    src = zcalloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = numkeysIndex + 1; i < setnum; i++, j++) {
        robj *obj = dstkey ?
                    lookupKeyWrite(c->db, c->argv[j]) :
                    lookupKeyRead(c->db, c->argv[j]);
        if (obj != NULL) {
            if (obj->type != OBJ_ZSET && obj->type != OBJ_SET) {
                zfree(src);
                addReplyErrorObject(c, shared.wrongtypeerr);
                return;
            }

            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;
        } else {
            src[i].subject = NULL;
        }

        /* Default all weights to 1. */
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    if (j < c->argc) {
        int remaining = c->argc - j;

        while (remaining) {
            if (op != SET_OP_DIFF &&
                remaining >= (setnum + 1) &&
                !strcasecmp(c->argv[j]->ptr, "weights")) {
                j++;
                remaining--;
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (getDoubleFromObjectOrReply(c, c->argv[j], &src[i].weight,
                                                   "weight value is not a float") != C_OK) {
                        zfree(src);
                        return;
                    }
                }
            } else if (op != SET_OP_DIFF &&
                       remaining >= 2 &&
                       !strcasecmp(c->argv[j]->ptr, "aggregate")) {
                j++;
                remaining--;
                if (!strcasecmp(c->argv[j]->ptr, "sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr, "min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr, "max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReplyErrorObject(c, shared.syntaxerr);
                    return;
                }
                j++;
                remaining--;
            } else if (remaining >= 1 &&
                       !dstkey &&
                       !strcasecmp(c->argv[j]->ptr, "withscores")) {
                j++;
                remaining--;
                withscores = 1;
            } else {
                zfree(src);
                addReplyErrorObject(c, shared.syntaxerr);
                return;
            }
        }
    }

    if (op != SET_OP_DIFF) {
        /* sort sets from the smallest to largest, this will improve our
        * algorithm's performance */
        qsort(src, setnum, sizeof(zsetopsrc), zuiCompareByCardinality);
    }

    dstobj = createZsetObject();
    dstzset = dstobj->ptr;
    memset(&zval, 0, sizeof(zval));

    if (op == SET_OP_INTER) {
        /* Skip everything if the smallest input is empty. */
        if (zuiLength(&src[0]) > 0) {
            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */
            zuiInitIterator(&src[0]);
            while (zuiNext(&src[0], &zval)) {
                double score, value;

                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    if (src[j].subject == src[0].subject) {
                        value = zval.score * src[j].weight;
                        zunionInterAggregate(&score, value, aggregate);
                    } else if (zuiFind(&src[j], &zval, &value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score, value, aggregate);
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                if (j == setnum) {
                    tmp = zuiNewSdsFromValue(&zval);
                    znode = zslInsert(dstzset->zsl, score, tmp);
                    dictAdd(dstzset->dict, tmp, &znode->score);
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                }
            }
            zuiClearIterator(&src[0]);
        }
    } else if (op == SET_OP_UNION) {
        dict *accumulator = dictCreate(&setAccumulatorDictType, NULL);
        dictIterator *di;
        dictEntry *de, *existing;
        double score;

        if (setnum) {
            /* Our union is at least as large as the largest set.
             * Resize the dictionary ASAP to avoid useless rehashing. */
            dictExpand(accumulator, zuiLength(&src[setnum - 1]));
        }

        /* Step 1: Create a dictionary of elements -> aggregated-scores
         * by iterating one sorted set after the other. */
        for (i = 0; i < setnum; i++) {
            if (zuiLength(&src[i]) == 0) continue;

            zuiInitIterator(&src[i]);
            while (zuiNext(&src[i], &zval)) {
                /* Initialize value */
                score = src[i].weight * zval.score;
                if (isnan(score)) score = 0;

                /* Search for this element in the accumulating dictionary. */
                de = dictAddRaw(accumulator, zuiSdsFromValue(&zval), &existing);
                /* If we don't have it, we need to create a new entry. */
                if (!existing) {
                    tmp = zuiNewSdsFromValue(&zval);
                    /* Remember the longest single element encountered,
                     * to understand if it's possible to convert to ziplist
                     * at the end. */
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                    /* Update the element with its initial score. */
                    dictSetKey(accumulator, de, tmp);
                    dictSetDoubleVal(de, score);
                } else {
                    /* Update the score with the score of the new instance
                     * of the element found in the current sorted set.
                     *
                     * Here we access directly the dictEntry double
                     * value inside the union as it is a big speedup
                     * compared to using the getDouble/setDouble API. */
                    zunionInterAggregate(&existing->v.d, score, aggregate);
                }
            }
            zuiClearIterator(&src[i]);
        }

        /* Step 2: convert the dictionary into the final sorted set. */
        di = dictGetIterator(accumulator);

        /* We now are aware of the final size of the resulting sorted set,
         * let's resize the dictionary embedded inside the sorted set to the
         * right size, in order to save rehashing time. */
        dictExpand(dstzset->dict, dictSize(accumulator));

        while ((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            score = dictGetDoubleVal(de);
            znode = zslInsert(dstzset->zsl, score, ele);
            dictAdd(dstzset->dict, ele, &znode->score);
        }
        dictReleaseIterator(di);
        dictRelease(accumulator);
    } else if (op == SET_OP_DIFF) {
        zdiff(src, setnum, dstzset, &maxelelen);
    } else {
        serverPanic("Unknown operator");
    }

    if (dstkey) {
        if (dstzset->zsl->length) {
            zsetConvertToZiplistIfNeeded(dstobj, maxelelen);
            setKey(c, c->db, dstkey, dstobj);
            addReplyLongLong(c, zsetLength(dstobj));
            notifyKeyspaceEvent(NOTIFY_ZSET,
                                (op == SET_OP_UNION) ? "zunionstore" :
                                (op == SET_OP_INTER ? "zinterstore" : "zdiffstore"),
                                dstkey, c->db->id);
            server.dirty++;
        } else {
            addReply(c, shared.czero);
            if (dbDelete(c->db, dstkey)) {
                signalModifiedKey(c, c->db, dstkey);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", dstkey, c->db->id);
                server.dirty++;
            }
        }
    } else {
        unsigned long length = dstzset->zsl->length;
        zskiplist *zsl = dstzset->zsl;
        zskiplistNode *zn = zsl->header->level[0].forward;
        /* In case of WITHSCORES, respond with a single array in RESP2, and
         * nested arrays in RESP3. We can't use a map response type since the
         * client library needs to know to respect the order. */
        if (withscores && c->resp == 2)
            addReplyArrayLen(c, length * 2);
        else
            addReplyArrayLen(c, length);

        while (zn != NULL) {
            if (withscores && c->resp > 2) addReplyArrayLen(c, 2);
            addReplyBulkCBuffer(c, zn->ele, sdslen(zn->ele));
            if (withscores) addReplyDouble(c, zn->score);
            zn = zn->level[0].forward;
        }
    }
    decrRefCount(dstobj);
    zfree(src);
}

void zunionstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_UNION);
}

void zinterstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_INTER);
}

void zdiffstoreCommand(client *c) {
    zunionInterDiffGenericCommand(c, c->argv[1], 2, SET_OP_DIFF);
}

void zunionCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_UNION);
}

void zinterCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_INTER);
}

void zdiffCommand(client *c) {
    zunionInterDiffGenericCommand(c, NULL, 1, SET_OP_DIFF);
}

typedef enum {
    ZRANGE_DIRECTION_AUTO = 0,
    ZRANGE_DIRECTION_FORWARD,
    ZRANGE_DIRECTION_REVERSE
} zrange_direction;

typedef enum {
    ZRANGE_CONSUMER_TYPE_CLIENT = 0,
    ZRANGE_CONSUMER_TYPE_INTERNAL
} zrange_consumer_type;

typedef struct zrange_result_handler zrange_result_handler;

typedef void (*zrangeResultBeginFunction)(zrange_result_handler *c);

typedef void (*zrangeResultFinalizeFunction)(
        zrange_result_handler *c, size_t result_count);

typedef void (*zrangeResultEmitCBufferFunction)(
        zrange_result_handler *c, const void *p, size_t len, double score);

typedef void (*zrangeResultEmitLongLongFunction)(
        zrange_result_handler *c, long long ll, double score);

void zrangeGenericCommand(zrange_result_handler *handler, int argc_start, int store,
                          zrange_type rangetype, zrange_direction direction);

/* Interface struct for ZRANGE/ZRANGESTORE generic implementation.
 * There is one implementation of this interface that sends a RESP reply to clients.
 * and one implementation that stores the range result into a zset object. */
struct zrange_result_handler {
    zrange_consumer_type type;
    client *client;
    robj *dstkey;
    robj *dstobj;
    void *userdata;
    int withscores;
    int should_emit_array_length;
    zrangeResultBeginFunction beginResultEmission;
    zrangeResultFinalizeFunction finalizeResultEmission;
    zrangeResultEmitCBufferFunction emitResultFromCBuffer;
    zrangeResultEmitLongLongFunction emitResultFromLongLong;
};

/* Result handler methods for responding the ZRANGE to clients. */
static void zrangeResultBeginClient(zrange_result_handler *handler) {
    handler->userdata = addReplyDeferredLen(handler->client);
}

static void zrangeResultEmitCBufferToClient(zrange_result_handler *handler,
                                            const void *value, size_t value_length_in_bytes, double score) {
    if (handler->should_emit_array_length) {
        addReplyArrayLen(handler->client, 2);
    }

    addReplyBulkCBuffer(handler->client, value, value_length_in_bytes);

    if (handler->withscores) {
        addReplyDouble(handler->client, score);
    }
}

static void zrangeResultEmitLongLongToClient(zrange_result_handler *handler,
                                             long long value, double score) {
    if (handler->should_emit_array_length) {
        addReplyArrayLen(handler->client, 2);
    }

    addReplyBulkLongLong(handler->client, value);

    if (handler->withscores) {
        addReplyDouble(handler->client, score);
    }
}

static void zrangeResultFinalizeClient(zrange_result_handler *handler,
                                       size_t result_count) {
    /* In case of WITHSCORES, respond with a single array in RESP2, and
     * nested arrays in RESP3. We can't use a map response type since the
     * client library needs to know to respect the order. */
    if (handler->withscores && (handler->client->resp == 2)) {
        result_count *= 2;
    }

    setDeferredArrayLen(handler->client, handler->userdata, result_count);
}

/* Result handler methods for storing the ZRANGESTORE to a zset. */
static void zrangeResultBeginStore(zrange_result_handler *handler) {
    handler->dstobj = createZsetZiplistObject();
}

static void zrangeResultEmitCBufferForStore(zrange_result_handler *handler,
                                            const void *value, size_t value_length_in_bytes, double score) {
    double newscore;
    int retflags = 0;
    sds ele = sdsnewlen(value, value_length_in_bytes);
    int retval = zsetAdd(handler->dstobj, score, ele, ZADD_IN_NONE, &retflags, &newscore);
    sdsfree(ele);
    serverAssert(retval);
}

static void zrangeResultEmitLongLongForStore(zrange_result_handler *handler,
                                             long long value, double score) {
    double newscore;
    int retflags = 0;
    sds ele = sdsfromlonglong(value);
    int retval = zsetAdd(handler->dstobj, score, ele, ZADD_IN_NONE, &retflags, &newscore);
    sdsfree(ele);
    serverAssert(retval);
}

static void zrangeResultFinalizeStore(zrange_result_handler *handler, size_t result_count) {
    if (result_count) {
        setKey(handler->client, handler->client->db, handler->dstkey, handler->dstobj);
        addReplyLongLong(handler->client, result_count);
        notifyKeyspaceEvent(NOTIFY_ZSET, "zrangestore", handler->dstkey, handler->client->db->id);
        server.dirty++;
    } else {
        addReply(handler->client, shared.czero);
        if (dbDelete(handler->client->db, handler->dstkey)) {
            signalModifiedKey(handler->client, handler->client->db, handler->dstkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", handler->dstkey, handler->client->db->id);
            server.dirty++;
        }
    }
    decrRefCount(handler->dstobj);
}

/* Initialize the consumer interface type with the requested type. */
static void zrangeResultHandlerInit(zrange_result_handler *handler,
                                    client *client, zrange_consumer_type type) {
    memset(handler, 0, sizeof(*handler));

    handler->client = client;

    switch (type) {
        case ZRANGE_CONSUMER_TYPE_CLIENT:
            handler->beginResultEmission = zrangeResultBeginClient;
            handler->finalizeResultEmission = zrangeResultFinalizeClient;
            handler->emitResultFromCBuffer = zrangeResultEmitCBufferToClient;
            handler->emitResultFromLongLong = zrangeResultEmitLongLongToClient;
            break;

        case ZRANGE_CONSUMER_TYPE_INTERNAL:
            handler->beginResultEmission = zrangeResultBeginStore;
            handler->finalizeResultEmission = zrangeResultFinalizeStore;
            handler->emitResultFromCBuffer = zrangeResultEmitCBufferForStore;
            handler->emitResultFromLongLong = zrangeResultEmitLongLongForStore;
            break;
    }
}

static void zrangeResultHandlerScoreEmissionEnable(zrange_result_handler *handler) {
    handler->withscores = 1;
    handler->should_emit_array_length = (handler->client->resp > 2);
}

static void zrangeResultHandlerDestinationKeySet(zrange_result_handler *handler,
                                                 robj *dstkey) {
    handler->dstkey = dstkey;
}

/* This command implements ZRANGE, ZREVRANGE. */
void genericZrangebyrankCommand(zrange_result_handler *handler,
                                robj *zobj, long start, long end, int withscores, int reverse) {

    client *c = handler->client;
    long llen;
    long rangelen;
    size_t result_cardinality;

    /* Sanitize indexes. */
    llen = zsetLength(zobj);
    if (start < 0) start = llen + start;
    if (end < 0) end = llen + end;
    if (start < 0) start = 0;

    handler->beginResultEmission(handler);

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        handler->finalizeResultEmission(handler, 0);
        return;
    }
    if (end >= llen) end = llen - 1;
    rangelen = (end - start) + 1;
    result_cardinality = rangelen;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score = 0.0;

        if (reverse)
            eptr = ziplistIndex(zl, -2 - (2 * start));
        else
            eptr = ziplistIndex(zl, 2 * start);

        serverAssertWithInfo(c, zobj, eptr != NULL);
        sptr = ziplistNext(zl, eptr);

        while (rangelen--) {
            serverAssertWithInfo(c, zobj, eptr != NULL && sptr != NULL);
            serverAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));

            if (withscores) /* don't bother to extract the score if it's gonna be ignored. */
                score = zzlGetScore(sptr);

            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            if (reverse)
                zzlPrev(zl, &eptr, &sptr);
            else
                zzlNext(zl, &eptr, &sptr);
        }

    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl, llen - start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl, start + 1);
        }

        while (rangelen--) {
            serverAssertWithInfo(c, zobj, ln != NULL);
            sds ele = ln->ele;
            handler->emitResultFromCBuffer(handler, ele, sdslen(ele), ln->score);
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    handler->finalizeResultEmission(handler, result_cardinality);
}

/* ZRANGESTORE <dst> <src> <min> <max> [BYSCORE | BYLEX] [REV] [LIMIT offset count] */
void zrangestoreCommand(client *c) {
    robj *dstkey = c->argv[1];
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_INTERNAL);
    zrangeResultHandlerDestinationKeySet(&handler, dstkey);
    zrangeGenericCommand(&handler, 2, 1, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

/* ZRANGE <key> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count] */
void zrangeCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_AUTO, ZRANGE_DIRECTION_AUTO);
}

/* ZREVRANGE <key> <min> <max> [WITHSCORES] */
void zrevrangeCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_RANK, ZRANGE_DIRECTION_REVERSE);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
void genericZrangebyscoreCommand(zrange_result_handler *handler,
                                 zrangespec *range, robj *zobj, long offset, long limit,
                                 int reverse) {

    client *c = handler->client;
    unsigned long rangelen = 0;

    handler->beginResultEmission(handler);

    /* For invalid offset, return directly. */
    if (offset > 0 && offset >= (long) zsetLength(zobj)) {
        handler->finalizeResultEmission(handler, 0);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInRange(zl, range);
        } else {
            eptr = zzlFirstInRange(zl, range);
        }

        /* Get score pointer for the first element. */
        if (eptr)
            sptr = ziplistNext(zl, eptr);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl, &eptr, &sptr);
            } else {
                zzlNext(zl, &eptr, &sptr);
            }
        }

        while (eptr && limit--) {
            double score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(score, range)) break;
            } else {
                if (!zslValueLteMax(score, range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed */
            serverAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));

            rangelen++;
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl, &eptr, &sptr);
            } else {
                zzlNext(zl, &eptr, &sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInRange(zsl, range);
        } else {
            ln = zslFirstInRange(zsl, range);
        }

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(ln->score, range)) break;
            } else {
                if (!zslValueLteMax(ln->score, range)) break;
            }

            rangelen++;
            handler->emitResultFromCBuffer(handler, ln->ele, sdslen(ln->ele), ln->score);

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    handler->finalizeResultEmission(handler, rangelen);
}

/* ZRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
void zrangebyscoreCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_SCORE, ZRANGE_DIRECTION_FORWARD);
}

/* ZREVRANGEBYSCORE <key> <min> <max> [WITHSCORES] [LIMIT offset count] */
void zrevrangebyscoreCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_SCORE, ZRANGE_DIRECTION_REVERSE);
}

void zcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    if (zslParseRange(c->argv[2], c->argv[3], &range) != C_OK) {
        addReplyError(c, "min or max is not a float");
        return;
    }

    /* Lookup the sorted set */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInRange(zl, &range);

        /* No "first" element */
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        sptr = ziplistNext(zl, eptr);
        score = zzlGetScore(sptr);
        serverAssertWithInfo(c, zobj, zslValueLteMax(score, &range));

        /* Iterate over elements in range */
        while (eptr) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (!zslValueLteMax(score, &range)) {
                break;
            } else {
                count++;
                zzlNext(zl, &eptr, &sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    addReplyLongLong(c, count);
}

void zlexcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    if (zslParseLexRange(c->argv[2], c->argv[3], &range) != C_OK) {
        addReplyError(c, "min or max not valid string range item");
        return;
    }

    /* Lookup the sorted set */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET)) {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInLexRange(zl, &range);

        /* No "first" element */
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        sptr = ziplistNext(zl, eptr);
        serverAssertWithInfo(c, zobj, zzlLexValueLteMax(eptr, &range));

        /* Iterate over elements in range */
        while (eptr) {
            /* Abort when the node is no longer in range. */
            if (!zzlLexValueLteMax(eptr, &range)) {
                break;
            } else {
                count++;
                zzlNext(zl, &eptr, &sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
void genericZrangebylexCommand(zrange_result_handler *handler,
                               zlexrangespec *range, robj *zobj, int withscores, long offset, long limit,
                               int reverse) {
    client *c = handler->client;
    unsigned long rangelen = 0;

    handler->beginResultEmission(handler);

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInLexRange(zl, range);
        } else {
            eptr = zzlFirstInLexRange(zl, range);
        }

        /* Get score pointer for the first element. */
        if (eptr)
            sptr = ziplistNext(zl, eptr);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl, &eptr, &sptr);
            } else {
                zzlNext(zl, &eptr, &sptr);
            }
        }

        while (eptr && limit--) {
            double score = 0;
            if (withscores) /* don't bother to extract the score if it's gonna be ignored. */
                score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zzlLexValueGteMin(eptr, range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr, range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed. */
            serverAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));

            rangelen++;
            if (vstr == NULL) {
                handler->emitResultFromLongLong(handler, vlong, score);
            } else {
                handler->emitResultFromCBuffer(handler, vstr, vlen, score);
            }

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl, &eptr, &sptr);
            } else {
                zzlNext(zl, &eptr, &sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInLexRange(zsl, range);
        } else {
            ln = zslFirstInLexRange(zsl, range);
        }

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslLexValueGteMin(ln->ele, range)) break;
            } else {
                if (!zslLexValueLteMax(ln->ele, range)) break;
            }

            rangelen++;
            handler->emitResultFromCBuffer(handler, ln->ele, sdslen(ln->ele), ln->score);

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    handler->finalizeResultEmission(handler, rangelen);
}

/* ZRANGEBYLEX <key> <min> <max> [LIMIT offset count] */
void zrangebylexCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_LEX, ZRANGE_DIRECTION_FORWARD);
}

/* ZREVRANGEBYLEX <key> <min> <max> [LIMIT offset count] */
void zrevrangebylexCommand(client *c) {
    zrange_result_handler handler;
    zrangeResultHandlerInit(&handler, c, ZRANGE_CONSUMER_TYPE_CLIENT);
    zrangeGenericCommand(&handler, 1, 0, ZRANGE_LEX, ZRANGE_DIRECTION_REVERSE);
}

/**
 * This function handles ZRANGE and ZRANGESTORE, and also the deprecated
 * Z[REV]RANGE[BYPOS|BYLEX] commands.
 *
 * The simple ZRANGE and ZRANGESTORE can take _AUTO in rangetype and direction,
 * other command pass explicit value.
 *
 * The argc_start points to the src key argument, so following syntax is like:
 * <src> <min> <max> [BYSCORE | BYLEX] [REV] [WITHSCORES] [LIMIT offset count]
 */
void zrangeGenericCommand(zrange_result_handler *handler, int argc_start, int store,
                          zrange_type rangetype, zrange_direction direction) {
    client *c = handler->client;
    robj *key = c->argv[argc_start];
    robj *zobj;
    zrangespec range;
    zlexrangespec lexrange;
    int minidx = argc_start + 1;
    int maxidx = argc_start + 2;

    /* Options common to all */
    long opt_start = 0;
    long opt_end = 0;
    int opt_withscores = 0;
    long opt_offset = 0;
    long opt_limit = -1;

    /* Step 1: Skip the <src> <min> <max> args and parse remaining optional arguments. */
    for (int j = argc_start + 3; j < c->argc; j++) {
        int leftargs = c->argc - j - 1;
        if (!store && !strcasecmp(c->argv[j]->ptr, "withscores")) {
            opt_withscores = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "limit") && leftargs >= 2) {
            if ((getLongFromObjectOrReply(c, c->argv[j + 1], &opt_offset, NULL) != C_OK) ||
                (getLongFromObjectOrReply(c, c->argv[j + 2], &opt_limit, NULL) != C_OK)) {
                return;
            }
            j += 2;
        } else if (direction == ZRANGE_DIRECTION_AUTO &&
                   !strcasecmp(c->argv[j]->ptr, "rev")) {
            direction = ZRANGE_DIRECTION_REVERSE;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(c->argv[j]->ptr, "bylex")) {
            rangetype = ZRANGE_LEX;
        } else if (rangetype == ZRANGE_AUTO &&
                   !strcasecmp(c->argv[j]->ptr, "byscore")) {
            rangetype = ZRANGE_SCORE;
        } else {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    /* Use defaults if not overriden by arguments. */
    if (direction == ZRANGE_DIRECTION_AUTO)
        direction = ZRANGE_DIRECTION_FORWARD;
    if (rangetype == ZRANGE_AUTO)
        rangetype = ZRANGE_RANK;

    /* Check for conflicting arguments. */
    if (opt_limit != -1 && rangetype == ZRANGE_RANK) {
        addReplyError(c, "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        return;
    }
    if (opt_withscores && rangetype == ZRANGE_LEX) {
        addReplyError(c, "syntax error, WITHSCORES not supported in combination with BYLEX");
        return;
    }

    if (direction == ZRANGE_DIRECTION_REVERSE &&
        ((ZRANGE_SCORE == rangetype) || (ZRANGE_LEX == rangetype))) {
        /* Range is given as [max,min] */
        int tmp = maxidx;
        maxidx = minidx;
        minidx = tmp;
    }

    /* Step 2: Parse the range. */
    switch (rangetype) {
        case ZRANGE_AUTO:
        case ZRANGE_RANK:
            /* Z[REV]RANGE, ZRANGESTORE [REV]RANGE */
            if ((getLongFromObjectOrReply(c, c->argv[minidx], &opt_start, NULL) != C_OK) ||
                (getLongFromObjectOrReply(c, c->argv[maxidx], &opt_end, NULL) != C_OK)) {
                return;
            }
            break;

        case ZRANGE_SCORE:
            /* Z[REV]RANGEBYSCORE, ZRANGESTORE [REV]RANGEBYSCORE */
            if (zslParseRange(c->argv[minidx], c->argv[maxidx], &range) != C_OK) {
                addReplyError(c, "min or max is not a float");
                return;
            }
            break;

        case ZRANGE_LEX:
            /* Z[REV]RANGEBYLEX, ZRANGESTORE [REV]RANGEBYLEX */
            if (zslParseLexRange(c->argv[minidx], c->argv[maxidx], &lexrange) != C_OK) {
                addReplyError(c, "min or max not valid string range item");
                return;
            }
            break;
    }

    if (opt_withscores || store) {
        zrangeResultHandlerScoreEmissionEnable(handler);
    }

    /* Step 3: Lookup the key and get the range. */
    zobj = handler->dstkey ?
           lookupKeyWrite(c->db, key) :
           lookupKeyRead(c->db, key);
    if (zobj == NULL) {
        addReply(c, shared.emptyarray);
        goto cleanup;
    }

    if (checkType(c, zobj, OBJ_ZSET)) goto cleanup;

    /* Step 4: Pass this to the command-specific handler. */
    switch (rangetype) {
        case ZRANGE_AUTO:
        case ZRANGE_RANK:
            genericZrangebyrankCommand(handler, zobj, opt_start, opt_end,
                                       opt_withscores || store, direction == ZRANGE_DIRECTION_REVERSE);
            break;

        case ZRANGE_SCORE:
            genericZrangebyscoreCommand(handler, &range, zobj, opt_offset,
                                        opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
            break;

        case ZRANGE_LEX:
            genericZrangebylexCommand(handler, &lexrange, zobj, opt_withscores || store,
                                      opt_offset, opt_limit, direction == ZRANGE_DIRECTION_REVERSE);
            break;
    }

    /* Instead of returning here, we'll just fall-through the clean-up. */

    cleanup:

    if (rangetype == ZRANGE_LEX) {
        zslFreeLexRange(&lexrange);
    }
}

void zcardCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    addReplyLongLong(c, zsetLength(zobj));
}

void zscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.null[c->resp])) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    if (zsetScore(zobj, c->argv[2]->ptr, &score) == C_ERR) {
        addReplyNull(c);
    } else {
        addReplyDouble(c, score);
    }
}

void zmscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;
    zobj = lookupKeyRead(c->db, key);
    if (checkType(c, zobj, OBJ_ZSET)) return;

    addReplyArrayLen(c, c->argc - 2);
    for (int j = 2; j < c->argc; j++) {
        /* Treat a missing set the same way as an empty set */
        if (zobj == NULL || zsetScore(zobj, c->argv[j]->ptr, &score) == C_ERR) {
            addReplyNull(c);
        } else {
            addReplyDouble(c, score);
        }
    }
}

void zrankGenericCommand(client *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    long rank;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.null[c->resp])) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    serverAssertWithInfo(c, ele, sdsEncodedObject(ele));
    rank = zsetRank(zobj, ele->ptr, reverse);
    if (rank >= 0) {
        addReplyLongLong(c, rank);
    } else {
        addReplyNull(c);
    }
}

void zrankCommand(client *c) {
    zrankGenericCommand(c, 0);
}

void zrevrankCommand(client *c) {
    zrankGenericCommand(c, 1);
}

void zscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c, c->argv[2], &cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.emptyscan)) == NULL ||
        checkType(c, o, OBJ_ZSET))
        return;
    scanGenericCommand(c, o, cursor);
}

/* This command implements the generic zpop operation, used by:
 * ZPOPMIN, ZPOPMAX, BZPOPMIN and BZPOPMAX. This function is also used
 * inside blocked.c in the unblocking stage of BZPOPMIN and BZPOPMAX.
 *
 * If 'emitkey' is true also the key name is emitted, useful for the blocking
 * behavior of BZPOP[MIN|MAX], since we can block into multiple keys.
 *
 * The synchronous version instead does not need to emit the key, but may
 * use the 'count' argument to return multiple items if available. */
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey, robj *countarg) {
    int idx;
    robj *key = NULL;
    robj *zobj = NULL;
    sds ele;
    double score;
    long count = 1;

    /* If a count argument as passed, parse it or return an error. */
    if (countarg) {
        if (getLongFromObjectOrReply(c, countarg, &count, NULL) != C_OK)
            return;
        if (count <= 0) {
            addReply(c, shared.emptyarray);
            return;
        }
    }

    /* Check type and break on the first error, otherwise identify candidate. */
    idx = 0;
    while (idx < keyc) {
        key = keyv[idx++];
        zobj = lookupKeyWrite(c->db, key);
        if (!zobj) continue;
        if (checkType(c, zobj, OBJ_ZSET)) return;
        break;
    }

    /* No candidate for zpopping, return empty. */
    if (!zobj) {
        addReply(c, shared.emptyarray);
        return;
    }

    void *arraylen_ptr = addReplyDeferredLen(c);
    long arraylen = 0;

    /* We emit the key only for the blocking variant. */
    if (emitkey) addReplyBulk(c, key);

    /* Remove the element. */
    do {
        if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
            unsigned char *zl = zobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            /* Get the first or last element in the sorted set. */
            eptr = ziplistIndex(zl, where == ZSET_MAX ? -2 : 0);
            serverAssertWithInfo(c, zobj, eptr != NULL);
            serverAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen(vstr, vlen);

            /* Get the score. */
            sptr = ziplistNext(zl, eptr);
            serverAssertWithInfo(c, zobj, sptr != NULL);
            score = zzlGetScore(sptr);
        } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *zln;

            /* Get the first or last element in the sorted set. */
            zln = (where == ZSET_MAX ? zsl->tail :
                   zsl->header->level[0].forward);

            /* There must be an element in the sorted set. */
            serverAssertWithInfo(c, zobj, zln != NULL);
            ele = sdsdup(zln->ele);
            score = zln->score;
        } else {
            serverPanic("Unknown sorted set encoding");
        }

        serverAssertWithInfo(c, zobj, zsetDel(zobj, ele));
        server.dirty++;

        if (arraylen == 0) { /* Do this only for the first iteration. */
            char *events[2] = {"zpopmin", "zpopmax"};
            notifyKeyspaceEvent(NOTIFY_ZSET, events[where], key, c->db->id);
            signalModifiedKey(c, c->db, key);
        }

        addReplyBulkCBuffer(c, ele, sdslen(ele));
        addReplyDouble(c, score);
        sdsfree(ele);
        arraylen += 2;

        /* Remove the key, if indeed needed. */
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db, key);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
            break;
        }
    } while (--count);

    setDeferredArrayLen(c, arraylen_ptr, arraylen + (emitkey != 0));
}

/* ZPOPMIN key [<count>] */
void zpopminCommand(client *c) {
    if (c->argc > 3) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    genericZpopCommand(c, &c->argv[1], 1, ZSET_MIN, 0,
                       c->argc == 3 ? c->argv[2] : NULL);
}

/* ZMAXPOP key [<count>] */
void zpopmaxCommand(client *c) {
    if (c->argc > 3) {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }
    genericZpopCommand(c, &c->argv[1], 1, ZSET_MAX, 0,
                       c->argc == 3 ? c->argv[2] : NULL);
}

/* BZPOPMIN / BZPOPMAX actual implementation. */
void blockingGenericZpopCommand(client *c, int where) {
    robj *o;
    mstime_t timeout;
    int j;

    if (getTimeoutFromObjectOrReply(c, c->argv[c->argc - 1], &timeout, UNIT_SECONDS)
        != C_OK)
        return;

    for (j = 1; j < c->argc - 1; j++) {
        o = lookupKeyWrite(c->db, c->argv[j]);
        if (checkType(c, o, OBJ_ZSET)) return;
        if (o != NULL) {
            if (zsetLength(o) != 0) {
                /* Non empty zset, this is like a normal ZPOP[MIN|MAX]. */
                genericZpopCommand(c, &c->argv[j], 1, where, 1, NULL);
                /* Replicate it as an ZPOP[MIN|MAX] instead of BZPOP[MIN|MAX]. */
                rewriteClientCommandVector(c, 2,
                                           where == ZSET_MAX ? shared.zpopmax : shared.zpopmin,
                                           c->argv[j]);
                return;
            }
        }
    }

    /* If we are not allowed to block the client and the zset is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    if (c->flags & CLIENT_DENY_BLOCKING) {
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    blockForKeys(c, BLOCKED_ZSET, c->argv + 1, c->argc - 2, timeout, NULL, NULL, NULL);
}

// BZPOPMIN key [key ...] timeout
void bzpopminCommand(client *c) {
    blockingGenericZpopCommand(c, ZSET_MIN);
}

// BZPOPMAX key [key ...] timeout
void bzpopmaxCommand(client *c) {
    blockingGenericZpopCommand(c, ZSET_MAX);
}

static void zarndmemberReplyWithZiplist(client *c, unsigned int count, ziplistEntry *keys, ziplistEntry *vals) {
    for (unsigned long i = 0; i < count; i++) {
        if (vals && c->resp > 2)
            addReplyArrayLen(c, 2);
        if (keys[i].sval)
            addReplyBulkCBuffer(c, keys[i].sval, keys[i].slen);
        else
            addReplyBulkLongLong(c, keys[i].lval);
        if (vals) {
            if (vals[i].sval) {
                addReplyDouble(c, zzlStrtod(vals[i].sval, vals[i].slen));
            } else
                addReplyDouble(c, vals[i].lval);
        }
    }
}

/* How many times bigger should be the zset compared to the requested size
 * for us to not use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define ZRANDMEMBER_SUB_STRATEGY_MUL 3

/* If client is trying to ask for a very large number of random elements,
 * queuing may consume an unlimited amount of memory, so we want to limit
 * the number of randoms per time. */
#define ZRANDMEMBER_RANDOM_SAMPLE_LIMIT 1000

void zrandmemberWithCountCommand(client *c, long l, int withscores) {
    unsigned long count, size;
    int uniq = 1;
    robj *zsetobj;

    if ((zsetobj = lookupKeyReadOrReply(c, c->argv[1], shared.null[c->resp]))
        == NULL || checkType(c, zsetobj, OBJ_ZSET))
        return;
    size = zsetLength(zsetobj);

    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        count = -l;
        uniq = 0;
    }

    /* If count is zero, serve it ASAP to avoid special cases later. */
    if (count == 0) {
        addReply(c, shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. This case is the only one that also needs to return the
     * elements in random order. */
    if (!uniq || count == 1) {
        if (withscores && c->resp == 2)
            addReplyArrayLen(c, count * 2);
        else
            addReplyArrayLen(c, count);
        if (zsetobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zsetobj->ptr;
            while (count--) {
                dictEntry *de = dictGetFairRandomKey(zs->dict);
                sds key = dictGetKey(de);
                if (withscores && c->resp > 2)
                    addReplyArrayLen(c, 2);
                addReplyBulkCBuffer(c, key, sdslen(key));
                if (withscores)
                    addReplyDouble(c, dictGetDoubleVal(de));
            }
        } else if (zsetobj->encoding == OBJ_ENCODING_ZIPLIST) {
            ziplistEntry *keys, *vals = NULL;
            unsigned long limit, sample_count;
            limit = count > ZRANDMEMBER_RANDOM_SAMPLE_LIMIT ? ZRANDMEMBER_RANDOM_SAMPLE_LIMIT : count;
            keys = zmalloc(sizeof(ziplistEntry) * limit);
            if (withscores)
                vals = zmalloc(sizeof(ziplistEntry) * limit);
            while (count) {
                sample_count = count > limit ? limit : count;
                count -= sample_count;
                ziplistRandomPairs(zsetobj->ptr, sample_count, keys, vals);
                zarndmemberReplyWithZiplist(c, sample_count, keys, vals);
            }
            zfree(keys);
            zfree(vals);
        }
        return;
    }

    zsetopsrc src;
    zsetopval zval;
    src.subject = zsetobj;
    src.type = zsetobj->type;
    src.encoding = zsetobj->encoding;
    zuiInitIterator(&src);
    memset(&zval, 0, sizeof(zval));

    /* Initiate reply count, RESP3 responds with nested array, RESP2 with flat one. */
    long reply_size = count < size ? count : size;
    if (withscores && c->resp == 2)
        addReplyArrayLen(c, reply_size * 2);
    else
        addReplyArrayLen(c, reply_size);

    /* CASE 2:
    * The number of requested elements is greater than the number of
    * elements inside the zset: simply return the whole zset. */
    if (count >= size) {
        while (zuiNext(&src, &zval)) {
            if (withscores && c->resp > 2)
                addReplyArrayLen(c, 2);
            addReplyBulkSds(c, zuiNewSdsFromValue(&zval));
            if (withscores)
                addReplyDouble(c, zval.score);
        }
        return;
    }

    /* CASE 3:
     * The number of elements inside the zset is not greater than
     * ZRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a dict from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requested elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 4 is highly inefficient. */
    if (count * ZRANDMEMBER_SUB_STRATEGY_MUL > size) {
        dict *d = dictCreate(&sdsReplyDictType, NULL);
        dictExpand(d, size);
        /* Add all the elements into the temporary dictionary. */
        while (zuiNext(&src, &zval)) {
            sds key = zuiNewSdsFromValue(&zval);
            dictEntry *de = dictAddRaw(d, key, NULL);
            serverAssert(de);
            if (withscores)
                dictSetDoubleVal(de, zval.score);
        }
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        while (size > count) {
            dictEntry *de;
            de = dictGetRandomKey(d);
            dictUnlink(d, dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d, de);
            size--;
        }

        /* Reply with what's in the dict and release memory */
        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(d);
        while ((de = dictNext(di)) != NULL) {
            if (withscores && c->resp > 2)
                addReplyArrayLen(c, 2);
            addReplyBulkSds(c, dictGetKey(de));
            if (withscores)
                addReplyDouble(c, dictGetDoubleVal(de));
        }

        dictReleaseIterator(di);
        dictRelease(d);
    }

        /* CASE 4: We have a big zset compared to the requested number of elements.
         * In this case we can simply get random elements from the zset and add
         * to the temporary set, trying to eventually get enough unique elements
         * to reach the specified count. */
    else {
        if (zsetobj->encoding == OBJ_ENCODING_ZIPLIST) {
            /* it is inefficient to repeatedly pick one random element from a
             * ziplist. so we use this instead: */
            ziplistEntry *keys, *vals = NULL;
            keys = zmalloc(sizeof(ziplistEntry) * count);
            if (withscores)
                vals = zmalloc(sizeof(ziplistEntry) * count);
            serverAssert(ziplistRandomPairsUnique(zsetobj->ptr, count, keys, vals) == count);
            zarndmemberReplyWithZiplist(c, count, keys, vals);
            zfree(keys);
            zfree(vals);
            return;
        }

        /* Hashtable encoding (generic implementation) */
        unsigned long added = 0;
        dict *d = dictCreate(&hashDictType, NULL);
        dictExpand(d, count);

        while (added < count) {
            ziplistEntry key;
            double score;
            zsetTypeRandomElement(zsetobj, size, &key, withscores ? &score : NULL);

            /* Try to add the object to the dictionary. If it already exists
            * free it, otherwise increment the number of objects we have
            * in the result dictionary. */
            sds skey = zsetSdsFromZiplistEntry(&key);
            if (dictAdd(d, skey, NULL) != DICT_OK) {
                sdsfree(skey);
                continue;
            }
            added++;

            if (withscores && c->resp > 2)
                addReplyArrayLen(c, 2);
            zsetReplyFromZiplistEntry(c, &key);
            if (withscores)
                addReplyDouble(c, score);
        }

        /* Release memory */
        dictRelease(d);
    }
}

/* ZRANDMEMBER [<count> WITHSCORES] */
void zrandmemberCommand(client *c) {
    long l;
    int withscores = 0;
    robj *zset;
    ziplistEntry ele;

    if (c->argc >= 3) {
        if (getLongFromObjectOrReply(c, c->argv[2], &l, NULL) != C_OK) return;
        if (c->argc > 4 || (c->argc == 4 && strcasecmp(c->argv[3]->ptr, "withscores"))) {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        } else if (c->argc == 4)
            withscores = 1;
        zrandmemberWithCountCommand(c, l, withscores);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    if ((zset = lookupKeyReadOrReply(c, c->argv[1], shared.null[c->resp])) == NULL ||
        checkType(c, zset, OBJ_ZSET)) {
        return;
    }

    zsetTypeRandomElement(zset, zsetLength(zset), &ele, NULL);
    zsetReplyFromZiplistEntry(c, &ele);
}
