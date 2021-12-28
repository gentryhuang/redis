//
// Created by 黄立豹 on 2021/12/28.
//

#ifndef REDIS_NEWLIST_H
#define REDIS_NEWLIST_H

#endif //REDIS_NEWLIST_H

/*
 * 自定义单向链表
 */
struct newList {
    struct newListNode *head;
    size_t len;
};

struct newListNode {
    long value;
    struct newListNode *next;
};

struct newList *newListCreate(void);

struct newList *newListAddNodeHead(struct newList *list, void *value);

size_t getNewListLength(struct newList *list);