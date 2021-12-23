/* adlist.h - A generic doubly linked list implementation
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

#ifndef __ADLIST_H__
#define __ADLIST_H__

/* Node, List, and Iterator are the only data structures used currently. */

/*
 * 双向链表节点
 */
typedef struct listNode {
    /*
     * 前置节点
     */
    struct listNode *prev;
    /*
     * 后置节点
     */
    struct listNode *next;
    /*
     * 节点值
     */
    void *value;
} listNode;

/*
 * 双向链表迭代器
 */
typedef struct listIter {
    /*
     * 当前迭代的节点
     */
    listNode *next;
    /*
     * 迭代的方向
     */
    int direction;
} listIter;

/*
 * 双向链表的结构
 */
typedef struct list {
    /*
     * 表头节点
     */
    listNode *head;
    /*
     * 表尾节点
     */
    listNode *tail;
    /*
     * 链表包含的节点数量
     */
    unsigned long len;

    /*
     * 节点值复制函数
     */
    void *(*dup)(void *ptr);
    /*
     * 节点值释放函数
     */
    void (*free)(void *ptr);
    /*
     * 节点值对比函数
     */
    int (*match)(void *ptr, void *key);


} list;

/* Functions implemented as macros */

// 返回给定链表所包含的节点数量
// T = O(1)
#define listLength(l) ((l)->len)

// 返回给定链表的头节点
#define listFirst(l) ((l)->head)

// 返回给定链表的表尾节点
// T = O(1)
#define listLast(l) ((l)->tail)

// 返回给定节点的前置节点
#define listPrevNode(n) ((n)->prev)

// 返回给定节点的后置节点
#define listNextNode(n) ((n)->next)

// 返回给定节点的值
// T = O(1)
#define listNodeValue(n) ((n)->value)

/*
 * 设置链表的操作函数
 */
#define listSetDupMethod(l,m) ((l)->dup = (m))
#define listSetFreeMethod(l,m) ((l)->free = (m))
#define listSetMatchMethod(l,m) ((l)->match = (m))

/*
 * 返回链表的操作函数
 */
#define listGetDupMethod(l) ((l)->dup)
#define listGetFreeMethod(l) ((l)->free)
#define listGetMatchMethod(l) ((l)->match)

/* Prototypes */
/*
 * 创建一个新的链表
 */
list *listCreate(void);
/*
 * 释放指定的链表
 */
void listRelease(list *list);
/*
 * 清空指定的链表，还原成一个新的链表
 */
void listEmpty(list *list);
/*
 * 添加元素到链表的头
 */
list *listAddNodeHead(list *list, void *value);
/*
 * 添加元素到链表的尾
 */
list *listAddNodeTail(list *list, void *value);
/*
 * 在指定元素插入到指定节点的前面后后面
 */
list *listInsertNode(list *list, listNode *old_node, void *value, int after);
/*
 * 从链表中删除指定的节点 node
 */
void listDelNode(list *list, listNode *node);
/*
 * 为链表创建一个迭代器
 */
listIter *listGetIterator(list *list, int direction);

/*
 * 返回被迭代器迭代到的链表节点
 */
listNode *listNext(listIter *iter);
/*
 * 释放迭代器
 */
void listReleaseIterator(listIter *iter);
/*
 * 复制整个链表
 */
list *listDup(list *orig);
/*
 * 查找链表 list 中值和 key 匹配的节点。
 */
listNode *listSearchKey(list *list, void *key);
/*
 * 返回链表在给定索引上的值。
 */
listNode *listIndex(list *list, long index);
/*
 * 将迭代器的方向设置为 AL_START_HEAD ， 并将迭代指针重新指向表头节点。
 */
void listRewind(list *list, listIter *li);
/*
 * 将迭代器的方向设置为 AL_START_TAIL ，并将迭代指针重新指向表尾节点。
 */
void listRewindTail(list *list, listIter *li);

/*
 * 将表尾节点移动到表头，成为新的表头节点
 */
void listRotateTailToHead(list *list);
/*
 * 将表头节点移动到表尾，成为新的表尾节点
 */
void listRotateHeadToTail(list *list);

void listJoin(list *l, list *o);

/* Directions for iterators */
/*
 * 迭代器遍历的方向
 *
 * 0 - 从头到尾遍历
 */
#define AL_START_HEAD 0
/*
 * 从尾到头遍历
 */
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */
