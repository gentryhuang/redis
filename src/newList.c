//
// Created by 黄立豹 on 2021/12/28.
//
#include <stdlib.h>
#include "newList.h"
#include "zmalloc.h"

newList *newListCreate(void){
    newList *newList = zmalloc(sizeof(*newList));
    newList->head = null;
    newList->len = 0;
    return newList;
}


newList *newListAddNodeHead(newList *list, void *value) {
    newListNode *node;

    // 为传入值分配一个节点空间
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    // 将保存的值放入到节点中
    node->value = value;

    // 如果链表为空，直接作为头节点
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;

        // 链表非空，则将新节点设置为头节点
    } else {
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }

    // 更新链表节点数
    list->len++;
    return list;
}

size_t getNewListLength(newList *list){
    return list->len;
}