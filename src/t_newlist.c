//
// Created by 黄立豹 on 2021/12/28.
//
#include "server.h"

/*
 * 向单向链表中添加元素
 */
void lnewpushCommand(client *c) {
    pushGenericCommand(c);
}

/*
 * 获取链表中的元素个数
 */
void newListLenCommand(client *c) {
    robj *lobj = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, lobj, OBJ_NEW_LIST)) return;

    addReplyLongLong(c, newListObjectLen(o));
}

void pushGenericCommand(client *c) {

    robj *lobj = lookupKeyWrite(c->db, c->argv[1]);
    if (checkType(c, lobj, OBJ_NEW_LIST)) return;

    // 不存在，则创建
    if (!lobj) {
        lobj = createNewlistObject();
        dbAdd(c->db, c->argv[1], lobj);
    }

    // 遍历所有输入值，并将它们添加到列表中
    for (j = 2; j < c->argc; j++) {
        newListTypePush(lobj, c->argv[j]);
        server.dirty++;
    }

    // 返回添加的节点数量
    addReplyLongLong(c, newListObjectLen(lobj));
}

void newListTypePush(robj *subject, robj *value) {
    if (subject->encoding == OBJ_ENCODING_NEWLIST) {
        newListAddNodeHead(subject->ptr, buf);
    } else {
        serverPanic("Unknown list encoding");
    }
}
