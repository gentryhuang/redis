/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
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

#ifndef __AE_H__
#define __AE_H__

#include "monotonic.h"

/*
 * 事件执行状态
 */
#define AE_OK 0  // 成功
#define AE_ERR -1 // 出错

/*
 * 文件事件状态
 */
#define AE_NONE 0       /* No events registered. 没有事件注册*/
#define AE_READABLE 1   /* Fire when descriptor is readable. 当套接字是可读时触发 */
#define AE_WRITABLE 2   /* Fire when descriptor is writable. 当套接字是可写时触发*/
#define AE_BARRIER 4    /* With WRITABLE, never fire the event if the
                           READABLE event already fired in the same event
                           loop iteration. Useful when you want to persist
                           things to disk before sending replies, and want
                           to do that in a group fashion. */



// 文件事件
#define AE_FILE_EVENTS (1<<0)
// 时间事件
#define AE_TIME_EVENTS (1<<1)
// 文件事件或时间事件
#define AE_ALL_EVENTS (AE_FILE_EVENTS|AE_TIME_EVENTS)
// 不阻塞，也不进行等待
#define AE_DONT_WAIT (1<<2)
// 休眠之前回调
#define AE_CALL_BEFORE_SLEEP (1<<3)
// 修改之后回调
#define AE_CALL_AFTER_SLEEP (1<<4)

// 决定时间事件是否要持续执行的 flag
#define AE_NOMORE -1
#define AE_DELETED_EVENT_ID -1

/* Macros */
#define AE_NOTUSED(V) ((void) V)

/*
 * 事件循环
 */
struct aeEventLoop;

/* Types and data structures */
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);

typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);

typedef void aeEventFinalizerProc(struct aeEventLoop *eventLoop, void *clientData);

typedef void aeBeforeSleepProc(struct aeEventLoop *eventLoop);

/* File event structure
 *
 * 文件事件结构，以文件作为唯一标志。
 *
 * 每一个文件描述符可能对应多个 IO 事件，即：
 * 1 eventLoop 中的 IO 事件数组是通过文件描述符进行定位的，也就是文件描述符号作为数组的下标
 * 2 每个 IO 事件都对应一个文件描述符（套接字）相关联的监听事件类型和回调函数。
 */
typedef struct aeFileEvent {
    // 事件类型的掩码
    // 主要有 AE_READABLE、AE_WRITABLE 和 AE_BARRIER 三种类型事件。事件驱动框架在分发事件时，依赖的就是结构体中的事件类型
    int mask; /* one of AE_(READABLE|WRITABLE|BARRIER) */

    // 读事件处理器器，指向 AE_READABLE 事件的处理函数 （Reactor 中的 handler）
    aeFileProc *rfileProc;

    // 写事件处理器，指向 AE_WRITABLE 事件的处理函数（Reactor 中的 handler）
    aeFileProc *wfileProc;

    // 多路复用库的私有数据（指向客户端私有数据的指针）
    void *clientData;

} aeFileEvent;

/* Time event structure
 *
 * 时间事件结构
 *
 * 服务器将所有时间事件都放在一个链表中，每当时间事件执行器运行时，它就遍历整个链表，
 * 查找所有已到达的时间事件，并调用相应的事件处理器。
 */
typedef struct aeTimeEvent {
    // 时间事件的唯一标志
    long long id; /* time event identifier. */

    // 事件的到达时间，毫秒精度的 UNIX 时间戳
    monotime when;

    // 时间事件处理函数，当时间事件到达时，服务器会调用该函数处理
    aeTimeProc *timeProc;

    // 事件结束后的释放函数
    aeEventFinalizerProc *finalizerProc;

    // 多路复用库的私有数据
    void *clientData;


    /*   时间事件是以链表的形式组织起来的  */
    // 指向上一个时间事件结构
    struct aeTimeEvent *prev;
    // 指向下一个时间事件结构
    struct aeTimeEvent *next;


    int refcount; /* refcount to prevent timer events from being
  		   * freed in recursive time event calls. */
} aeTimeEvent;

/* A fired event
 *
 * 已就绪事件
 */
typedef struct aeFiredEvent {
    // 已就绪文件描述符
    int fd;

    // 事件类型掩码
    int mask; /* one of AE_(READABLE|WRITABLE|BARRIER) */
} aeFiredEvent;

/* State of an event based program
 *
 * 事件循环结构体，记录了事件驱动框架循环运行过程中的信息
 */
typedef struct aeEventLoop {
    // 目前已注册的最大文件描述符
    int maxfd;   /* highest file descriptor currently registered */

    // 目前已追踪的最大描述符
    int setsize; /* max number of file descriptors tracked */

    // 用于生成时间事件 id
    long long timeEventNextId;

    // 已注册的文件事件（IO事件，因为所有IO事件都会用文件描述符进行标识）数组
    // eg: aeFileEvent *fe = &eventLoop->events[fd] ,
    aeFileEvent *events; /* Registered events */

    // 已就绪的文件事件（记录已触发事件对应的文件描述符信息）数组
    // eg:  eventLoop->fired[j].fd = e->data.fd;
    aeFiredEvent *fired; /* Fired events */

    // 时间事件链表的头指针，按一定时间周期触发的事件
    aeTimeEvent *timeEventHead;

    // 事件处理器的开关， 1 -> 停止事件处理器
    int stop;

    // 多路复用库的私有数据，如使用 Linux 下的 epoll 时，ae_epoll.c 中封装的 aeApiState 结构，它保存了 epoll 实例的信息
    void *apidata; /* This is used for polling API specific data */

    // 在处理事件前要执行的函数
    aeBeforeSleepProc *beforesleep;

    // 在处理事件后要执行的函数
    aeBeforeSleepProc *aftersleep;


    int flags;
} aeEventLoop;



/* Prototypes */
/**
 * 初始化事件循环结构体，包括 IO 事件数组和时间事件链表
 * @param setsize
 * @return
 */
aeEventLoop *aeCreateEventLoop(int setsize);

void aeDeleteEventLoop(aeEventLoop *eventLoop);

void aeStop(aeEventLoop *eventLoop);

/**
 * 负责事件和 handler 注册的 aeCreateFileEvent 函数
 * @param eventLoop
 * @param fd
 * @param mask
 * @param proc
 * @param clientData
 * @return
 */
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                      aeFileProc *proc, void *clientData);

void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask);

int aeGetFileEvents(aeEventLoop *eventLoop, int fd);

/**
 * 创建时间事件
 * @param eventLoop
 * @param milliseconds  所创建时间事件的触发时间距离当前时间的时长，是用毫秒表示的
 * @param proc 所创建时间事件触发后的回调函数。
 * @param clientData
 * @param finalizerProc
 * @return
 */
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
                            aeTimeProc *proc, void *clientData,
                            aeEventFinalizerProc *finalizerProc);

int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id);

/**
 * 负责事件捕获与分发的函数
 * @param eventLoop
 * @param flags
 * @return
 */
int aeProcessEvents(aeEventLoop *eventLoop, int flags);

int aeWait(int fd, int mask, long long milliseconds);

/**
 * 事件驱动框架主循环函数
 * @param eventLoop
 */
void aeMain(aeEventLoop *eventLoop);


char *aeGetApiName(void);

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep);

void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep);

int aeGetSetSize(aeEventLoop *eventLoop);

int aeResizeSetSize(aeEventLoop *eventLoop, int setsize);

void aeSetDontWait(aeEventLoop *eventLoop, int noWait);

#endif
