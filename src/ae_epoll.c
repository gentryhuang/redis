/* Linux epoll(2) based ae.c module
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


#include <sys/epoll.h>

/**
 * epoll 实例私有数据
 */
typedef struct aeApiState {
    // epoll 实例的描述符
    int epfd;

    // 事件表
    struct epoll_event *events;
} aeApiState;

/*
 * 创建一个新的 epoll 实例
 *
 * 1 保存 epoll 的文件描述符
 * 2 分配 epoll_event 数组，该数组用来接收内核返回的文件描述符信息
 */
static int aeApiCreate(aeEventLoop *eventLoop) {

    // 为 epoll 多路复用库的私有数据 分配内存空间
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;

    // 初始化事件表， 将 epoll 的 epoll_event 数组保存到 aeApiState 结构体变量 state 中
    state->events = zmalloc(sizeof(struct epoll_event)*eventLoop->setsize);


    if (!state->events) {
        zfree(state);
        return -1;
    }

    // 调用 epoll 接口创建 epoll 实例，并返回 epoll 的文件描述符，将该描述符保存到 aeApiState 结构体变量 state 中
    state->epfd = epoll_create(1024); /* 1024 is just a hint for the kernel */

    // 如果创建 epoll 失败，则释放相关内存空间
    if (state->epfd == -1) {
        zfree(state->events);
        zfree(state);
        return -1;
    }


    anetCloexec(state->epfd);

    // 把 state 变量赋值给 eventLoop 中的 apidata。这样一来，eventLoop 结构体中就有了 epoll 实例和 epoll_event 数组的信息，这样就可以用来基于 epoll 创建和处理事件了。
    // 即将 epoll 的关键信息通过 aeApiState 结构体绑定到事件循环中
    eventLoop->apidata = state;
    return 0;
}

/*
 * 调整事件表
 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;

    state->events = zrealloc(state->events, sizeof(struct epoll_event)*setsize);
    return 0;
}

/*
 * 释放 epoll 实例
 */
static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;

    close(state->epfd);
    zfree(state->events);
    zfree(state);
}

/**
 * 关联给定事件到 fd
 *
 * @param eventLoop 事件循环
 * @param fd 要监听的文件描述符
 * @param mask 事件类型掩码
 * @return
 */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {

    // 1 获取 epoll 实例数据,epoll 实例是通过 aeApiCreate 函数创建的，保存在 eventLoop 结构体的 apidata 属性中，类型是 aeApiState
    aeApiState *state = eventLoop->apidata;

    // 2 准备一个 epoll 的 epoll_event ，然后下面逻辑会设置 ee 中的监听事件类型和监听文件描述符
    struct epoll_event ee = {0}; /* avoid valgrind warning */

    /* If the fd was already monitored for some event, we need a MOD
     * operation. Otherwise we need an ADD operation.
     *
     * 如果 fd 没有关联任何事件（初始值是 AE_NONE），那么这是一个 ADD 操作，
     * 如果已经关联了某个/某些事件，那么这是一个 MOD 操作。
     */
    // 3 判断操作类型，是新增还是修改
    int op = eventLoop->events[fd].mask == AE_NONE ?
            EPOLL_CTL_ADD : EPOLL_CTL_MOD;

    // 2.1 注册感兴趣的事件
    ee.events = 0;

    // 将可读或可写IO事件类型转换为epoll监听的类型EPOLLIN或EPOLLOUT
    // 说明：根据事件类型掩码是 可读（AE_READABLE）或可写（AE_WRITABLE）事件来来设置 ee 监听的事件类型是 EPOLLIN 还是 EPOLLOUT。
    // 这样一来，Redis 事件驱动框架中的读写事件就能够和 epoll 机制中的读写事件对应上来
    mask |= eventLoop->events[fd].mask; /* Merge old events */
    if (mask & AE_READABLE) ee.events |= EPOLLIN;
    if (mask & AE_WRITABLE) ee.events |= EPOLLOUT;

    // 2.2 设置文件描述符
    ee.data.fd = fd;

    // 4 通过调用 Linux 下的 epoll_ctl 函数向 epoll 对象中添加、删除、修改感兴趣的文件描述符事件信息
    if (epoll_ctl(state->epfd,op,fd,&ee) == -1) return -1;

    return 0;
}

/*
 * 从 fd 中删除给定事件
 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int delmask) {
    // 获取 epoll 实例数据
    aeApiState *state = eventLoop->apidata;
    struct epoll_event ee = {0}; /* avoid valgrind warning */
    int mask = eventLoop->events[fd].mask & (~delmask);

    ee.events = 0;
    if (mask & AE_READABLE) ee.events |= EPOLLIN;
    if (mask & AE_WRITABLE) ee.events |= EPOLLOUT;
    ee.data.fd = fd;


    if (mask != AE_NONE) {
        epoll_ctl(state->epfd,EPOLL_CTL_MOD,fd,&ee);
    } else {
        /* Note, Kernel < 2.6.9 requires a non null event pointer even for
         * EPOLL_CTL_DEL. */
        epoll_ctl(state->epfd,EPOLL_CTL_DEL,fd,&ee);
    }
}

/*
 * 获取可执行事件
 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    // 获取 epoll 实例数据
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;

    // 调用 epoll 的接口获取监听到事件
    // 成功时返回就绪的事件数目，调用失败时返回 -1，等待超时返回 0。
    retval = epoll_wait(state->epfd,state->events,eventLoop->setsize,
            tvp ? (tvp->tv_sec*1000 + (tvp->tv_usec + 999)/1000) : -1);

    // 至少一个事件就绪
    if (retval > 0) {
        int j;

        // 获得监听到的事件数量
        numevents = retval;

        // 针对每个事件进行处理
        // 遍历 events ，将对应文件描述符发生的事件加入到 eventLoop 的 fired 数组中
        for (j = 0; j < numevents; j++) {
            int mask = 0;
            struct epoll_event *e = state->events+j;

            // 读取发生的事件
            if (e->events & EPOLLIN) mask |= AE_READABLE;
            if (e->events & EPOLLOUT) mask |= AE_WRITABLE;
            if (e->events & EPOLLERR) mask |= AE_WRITABLE|AE_READABLE;
            if (e->events & EPOLLHUP) mask |= AE_WRITABLE|AE_READABLE;

            // 设置就绪事件的 fd
            eventLoop->fired[j].fd = e->data.fd;
            // 设置就绪事件的事件类型
            eventLoop->fired[j].mask = mask;
        }
    }

    // 返回已就绪事件个数
    return numevents;
}

/*
 * 返回当前正在使用的 IO 多路复用库的名字
 */
static char *aeApiName(void) {
    return "epoll";
}
