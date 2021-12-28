//
// Created by 黄立豹 on 2021/12/28.
//

#ifndef REDIS_NEWLIST_H
#define REDIS_NEWLIST_H

#endif //REDIS_NEWLIST_H

/*
 * 自定义单向链表
 */
typedef struct newList {
    struct newListNode *head;
    size_t len;
} newList;

typedef struct newListNode {
    long value;
    struct newListNode *next;
} newListNode;

newList *newListCreate(void);

newList *newListAddNodeHead(newList *list, void *value);

size_t getNewListLength(newList *list);