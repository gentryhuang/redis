/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "ae.h"
#include "anet.h"

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "zmalloc.h"
#include "config.h"

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
#ifdef HAVE_EPOLL
#include "ae_epoll.c"
#else
#ifdef HAVE_KQUEUE

#include "ae_kqueue.c"

#else
#include "ae_select.c"
#endif
#endif
#endif

/*
 * 创建事件循环
 *
 * 说明：
 * 1 创建一个 aeEventLoop 结构，并存储到 server 中。
 * 2 事件循环的执行依赖系统底层的 IO多路复用，如 epoll
 * 3 Redis 事件的核心是基于系统底层的 IO多路复用 封装了一套
 * 注意：
 * 1 setsize 表示可以处理的文件描述符的总数，事件驱动框架监听的 IO 事件数组大小等于 setsize ，这样决定了和 Redis server 连接的客户端数量。当客户端连接 Redis 超过最大连接数可以修改
 *   redis.conf 文件修改 maxclients 配置项
 */
aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    // 防止调用应用程序没有初始化
    monotonicInit();    /* just in case the calling app didn't initialize */

    // 给eventLoop变量分配内存空间
    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;

    // 分别初始化文件事件结构数组、已就绪文件事件结构数组
    eventLoop->events = zmalloc(sizeof(aeFileEvent) * setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent) * setsize);
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;

    // 设置数组的大小
    eventLoop->setsize = setsize;

    // 初始化时间事件结构
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;


    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;
    eventLoop->flags = 0;

    // 创建多路复用程序，实际调用操作系统提供的IO多路复用函数
    // 这一步是关联操作系统的IO多路函数的核心，即去持有操作系统关键属性
    if (aeApiCreate(eventLoop) == -1) goto err;
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it.
     *
     * 初始化监听事件，初始的事件类型为 AE_NONE ，表示暂时不监听任何事件
     */
    // 将所有网络IO事件对应文件描述符的掩码设置为AE_NONE
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;

    // 返回事件程序
    return eventLoop;

    err:
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

/* Return the current set size.
 *
 * 返回当前事件表大小
 */
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}

/* Tells the next iteration/s of the event processing to set timeout of 0. */
void aeSetDontWait(aeEventLoop *eventLoop, int noWait) {
    if (noWait)
        eventLoop->flags |= AE_DONT_WAIT;
    else
        eventLoop->flags &= ~AE_DONT_WAIT;
}

/* Resize the maximum set size of the event loop.
 *
 * 调整事件表的大小
 *
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * 如果尝试调整大小为 setsize, 但有 >= setsize 的文件描述符存在，那么返回 AE_ERR，不进行任何动作
 *
 * Otherwise AE_OK is returned and the operation is successful.
 *
 * 否则，执行大小调整操作，并返回 AE_OK
 */
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    int i;

    if (setsize == eventLoop->setsize) return AE_OK;
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    if (aeApiResize(eventLoop, setsize) == -1) return AE_ERR;

    eventLoop->events = zrealloc(eventLoop->events, sizeof(aeFileEvent) * setsize);
    eventLoop->fired = zrealloc(eventLoop->fired, sizeof(aeFiredEvent) * setsize);
    eventLoop->setsize = setsize;

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask. */
    for (i = eventLoop->maxfd + 1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}

/*
 * 删除事件程序状态
 */
void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    aeApiFree(eventLoop);
    zfree(eventLoop->events);
    zfree(eventLoop->fired);

    /* Free the time events list. */
    aeTimeEvent *next_te, *te = eventLoop->timeEventHead;
    while (te) {
        next_te = te->next;
        zfree(te);
        te = next_te;
    }
    zfree(eventLoop);
}

/*
 * 停止事件处理器
 */
void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}


/**
 * IO 事件的创建是通过该函数来完成的，它通过 aeApiAddEvent 函数来完成事件注册。
 *
 * 根据 mask 参数的值(文件事件类型)，监听 fd 对应的套接字的状态，并将事件和事件处理器关联。当套接字可用时，执行处理器。
 *
 * 即：
 * 1 将给定套接字的给定事件注册到 I/O 多路复用程序。这样，当套接字发生事件时，I/O 多路复用程序就可以监听到。
 * 2 将给定套接字的给定事件和事件处理器进行关联。
 *
 *
 * 说明：套接字和套接字的事件是一起的
 *
 * @param eventLoop 事件程序状态
 * @param fd IO事件对应的文件描述符
 * @param mask 事件类型掩码
 * @param proc 事件处理回调函数
 * @param clientData 多路复用库私有数据
 * @return
 */
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                      aeFileProc *proc, void *clientData) {
    if (fd >= eventLoop->setsize) {
        errno = ERANGE;
        return AE_ERR;
    }

    // 1 取出文件描述符 fd 对应的 IO事件
    aeFileEvent *fe = &eventLoop->events[fd];

    // 2 监听指定 fd(套接字) 上的指定事件，即添加要监听的事件
    // 特别说明：Redis 的 I/O 多路复用程序的所有功能都是通过包装常见的 select、epoll、evport 和 kqueue 这些 I/O 多路复用函数来实现的，
    // 该函数实际上会调用操作系统提供的 IO 多路复用函数来完成事件的添加。
    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;

    // 3 设置文件事件类型，以及为（套接字的）事件关联处理器
    fe->mask |= mask;
    if (mask & AE_READABLE) fe->rfileProc = proc;
    if (mask & AE_WRITABLE) fe->wfileProc = proc;

    // 私有数据
    fe->clientData = clientData;

    // 如果有需要，更新事件程序中维护的文件描述符的最大值
    if (fd > eventLoop->maxfd)
        eventLoop->maxfd = fd;

    return AE_OK;
}

/**
 * 将套接字 fd 从 mask 指定的监听队列中删除
 *
 * @param eventLoop 事件程序的状态
 * @param fd  套接字描述符
 * @param mask 事件类型
 */
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask) {
    if (fd >= eventLoop->setsize) return;

    // 1 取出文件描述符 fd 对应的事件结构
    aeFileEvent *fe = &eventLoop->events[fd];

    // 文件描述符上未设置监听的事件类型，直接返回
    if (fe->mask == AE_NONE) return;

    /* We want to always remove AE_BARRIER if set when AE_WRITABLE
     * is removed. */
    if (mask & AE_WRITABLE) mask |= AE_BARRIER;

    // 让 I/O 多路复用程序取消对给定套接字 fd 的给定事件的监视
    aeApiDelEvent(eventLoop, fd, mask);


    fe->mask = fe->mask & (~mask);
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* Update the max fd */
        int j;

        for (j = eventLoop->maxfd - 1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
}

/**
 *
 * @param eventLoop 事件程序
 * @param fd 套接字描述符
 * @return
 */
int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;

    // 返回当前文件描述符 fd 正在被监听的事件类型
    aeFileEvent *fe = &eventLoop->events[fd];

    // 事件类型掩码
    // 如果套接字没有任何事件被监听，那么函数返回 AE_NONE
    // 如果套接字的读事件正在被监听，那么函数返回 AE_READABLE
    // 如果套接字的写事件正在被监听，那么函数返回 AE_WRITABLE
    // 如果套接字的读事件和写事件正在被监视，那么函数返回 AE_READABLE|AE_WRITABLE
    return fe->mask;
}

/*
 * 创建时间事件，头插法
 *
 * 创建的时间事件将在当前时间的 milliseconds 毫秒之后到达，到达后由 proc 处理
 */
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
                            aeTimeProc *proc, void *clientData,
                            aeEventFinalizerProc *finalizerProc) {

    // 更新时间计数器
    long long id = eventLoop->timeEventNextId++;

    // 创建时间事件结构
    aeTimeEvent *te;
    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;

    // 设置时间事件唯一标志
    te->id = id;

    // 设置处理事件的时间
    te->when = getMonotonicUs() + milliseconds * 1000;

    // 设置时间事件处理器
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    // 设置私有数据
    te->clientData = clientData;


    // 将新事件放入表头，构建双向链表
    te->prev = NULL;
    te->next = eventLoop->timeEventHead;
    te->refcount = 0;
    if (te->next)
        te->next->prev = te;
    eventLoop->timeEventHead = te;


    return id;
}

/*
 * 从服务器中删除 id 对应的时间事件
 */
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id) {
    aeTimeEvent *te = eventLoop->timeEventHead;
    while (te) {
        if (te->id == id) {
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* How many microseconds until the first timer should fire.
 * If there are no timers, -1 is returned.
 *
 * 寻找时间事件链表中最近的时间事件，没有则返回 -1
 *
 * Note that's O(N) since time events are unsorted.
 *
 * 注意，因为链表中的时间事件是未排序的，所以查找的时间复杂度为 O(N)
 *
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
static int64_t usUntilEarliestTimer(aeEventLoop *eventLoop) {
    aeTimeEvent *te = eventLoop->timeEventHead;
    if (te == NULL) return -1;

    aeTimeEvent *earliest = NULL;
    while (te) {
        if (!earliest || te->when < earliest->when)
            earliest = te;
        te = te->next;
    }

    monotime now = getMonotonicUs();

    // 如果事件已到达，那么返回 0，否则返回到达的倒计时时间
    return (now >= earliest->when) ? 0 : earliest->when - now;
}

/* Process time events
 *
 * 时间事件的执行器
 *
 * 遍历所有已到达的时间事件，并调用这些事件的处理器。
 * 已到达：时间事件的 when 属性记录的 UNIX 时间戳等于或小于当前时间的 UNIX 时间戳
 *
 * 步骤：
 *
 * 1 遍历服务器中的所有时间事件
 * 2 检查事件是否是待删除的事件或无效事件
 * 3 判断事件是否到达，事件没有到达则遍历下一个
 * 4 时间事件到达，使用事件处理器处理，并获取返回值
 *   - 4.1 如果是一个定时事件，那么标记删除，这样下次遍历就可以删除了
 *   - 4.2 如果是一个周期性事件，那么按照事件处理器的返回值更新时间事件的 when 属性，让这个事件在指定的时间之后再次到达
 */
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;

    // 遍历链表，执行那些已到达的时间事件
    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId - 1;
    monotime now = getMonotonicUs();
    while (te) {
        long long id;

        /* Remove events scheduled for deletion. */
        // 删除当前 te 时间事件
        if (te->id == AE_DELETED_EVENT_ID) {
            aeTimeEvent *next = te->next;
            /* If a reference exists for this timer event,
             * don't free it. This is currently incremented
             * for recursive timerProc calls */
            if (te->refcount) {
                te = next;
                continue;
            }
            if (te->prev)
                te->prev->next = te->next;
            else
                eventLoop->timeEventHead = te->next;
            if (te->next)
                te->next->prev = te->prev;
            if (te->finalizerProc) {
                te->finalizerProc(eventLoop, te->clientData);
                now = getMonotonicUs();
            }
            zfree(te);
            te = next;
            continue;
        }

        /* Make sure we don't process time events created by time events in
         * this iteration. Note that this check is currently useless: we always
         * add new timers on the head, however if we change the implementation
         * detail, this check may be useful again: we keep it here for future
         * defense. */
        // 跳过无效事件
        if (te->id > maxId) {
            te = te->next;
            continue;
        }

        // 当前 te 时间事件到达了，需要交给对应的处理器执行
        // 时间事件的 when 属性记录的 UNIX 时间戳等于或小于当前时间的 UNIX 时间戳
        if (te->when <= now) {
            int retval;

            id = te->id;
            te->refcount++;

            // 调用注册的回调函数处理，并获取返回值
            retval = te->timeProc(eventLoop, id, te->clientData);

            te->refcount--;
            processed++;
            now = getMonotonicUs();

            // 根据返回值 retval ，判断记录是否有需要循环执行这个事件时间
            if (retval != AE_NOMORE) {
                // 需要， retval 毫秒之后继续执行这个时间事件
                te->when = now + retval * 1000;
            } else {
                // 不需要，将这个事件删除
                te->id = AE_DELETED_EVENT_ID;
            }
        }

        //获取下一个时间事件
        te = te->next;
    }
    return processed;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * 处理所有已到达的时间事件，以及所有已就绪的文件事件
 *
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurs (if any).
 * 如果不传入特殊 flags 的话，那么函数休眠直到文件事件就绪
 *
 * If flags is 0, the function does nothing and returns.
 * 如果 flags 为 0，那么函数不作动作，直接返回
 *
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * 如果 flags 包含 AE_ALL_EVENTS ，所有类型的事件都会被处理
 *
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * 如果 flags 包含 AE_FILE_EVENTS ，那么处理文件事件
 *
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * 如果 flags 包含 AE_TIME_EVENTS ，那么处理时间事件
 *
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * the events that's possible to process without to wait are processed.
 * 如果 flags 包含 AE_DONT_WAIT ，那么函数在处理完所有无需等待的事件后，即可返回
 *
 * if flags has AE_CALL_AFTER_SLEEP set, the aftersleep callback is called.
 * 如果flags设置了AE_CALL_AFTER_SLEEP，则调用aftersleep回调。
 *
 * if flags has AE_CALL_BEFORE_SLEEP set, the beforesleep callback is called.
 * 如果flags设置了AE_CALL_BEFORE_SLEEP，则调用beforesleep回调函数。
 *
 * The function returns the number of events processed.
 * 函数的返回值为已处理事件的数量
 *
 * 事件分配器，过程：
 * 1 获取到达时间离当前时间最接近的时间事件，并计算最接近的时间事件距离到达还有多少毫秒
 * 2 阻塞并等待文件事件的产生，最大阻塞时间：
 *   - 存在最近时间事件，阻塞时间就是 usUntilEarliestTimer 返回的毫秒值
 *   - 不存在最近时间事件，阻塞时间根据 flags 的值，决定不阻塞还是阻塞到事件时间到来。
 * 3 调用 aeApiPoll 阻塞等待文件事件产生
 * 4 如果有文件事件，则处理已产生的文件事件
 * 5 处理所有已到达的时间事件
 **/
int aeProcessEvents(aeEventLoop *eventLoop, int flags) {
    int processed = 0, numevents;

    /* Nothing to do? return ASAP */
    // 一、如果没有事件处理，则直接返回
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want to call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire. */
    // 二、如果有 IO 事件发生，或者紧急的时间事件发生，则开始处理
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        struct timeval tv, *tvp;
        int64_t usUntilTimer = -1;

        // 1 获取最近的时间事件
        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            usUntilTimer = usUntilEarliestTimer(eventLoop);

        // 1.1 使用 usUntilTimer 来决定文件事件的阻塞时间
        if (usUntilTimer >= 0) {
            tv.tv_sec = usUntilTimer / 1000000;
            tv.tv_usec = usUntilTimer % 1000000;
            tvp = &tv;

            // 1.2 执行到这里，说明没有时间事件。那么根据 AE_DONT_WAIT 是否设置来决定是否阻塞，以及阻塞的时间长度
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to set the timeout
             * to zero */
            if (flags & AE_DONT_WAIT) {
                // 设置文件事件不阻塞
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                // 文件事件阻塞直到有事件到达为止
                tvp = NULL; /* wait forever */
            }
        }

        if (eventLoop->flags & AE_DONT_WAIT) {
            tv.tv_sec = tv.tv_usec = 0;
            tvp = &tv;
        }

        /*------------------------- IO 线程激活代码区 ----------------------------------*/

        /* 2 前置回调函数 - beforeSleep （server.c 中的方法，服务器初始化时绑定的 - initServer方法 */
        if (eventLoop->beforesleep != NULL && flags & AE_CALL_BEFORE_SLEEP)
            eventLoop->beforesleep(eventLoop);

        /* Call the multiplexing API, will return only on timeout or when
         * some event fires.
         *
         * 3 调用多路复用 API，只会在超时或某些事件触发时返回。即等待事件产生。
         *    用于获取就绪的描述符
         */
        // 捕获事件，检查是否有新的连接、读写事件发生，依赖操作系统底层提供的 IO多路复用机制。
        // 为了适配不同的操作系统，Redis 对不同操作系统实现的网络 IO 多路复用函数，都进行了统一的封装，封装后的代码分别在不同的文件中：
        // - ae_epoll.c，对应 Linux 上的 IO 复用函数 epoll；
        // - ae_evport.c，对应 Solaris 上的 IO 复用函数 evport；
        // - ae_kqueue.c，对应 macOS 或 FreeBSD 上的 IO 复用函数 kqueue；
        // - ae_select.c，对应 Linux（或 Windows）的 IO 复用函数 select。
        // 有了这些封装代码后，Redis 在不同的操作系统上调用 IO 多路复用 API 时，就可以通过统一的接口来进行调用了。
        numevents = aeApiPoll(eventLoop, tvp);

        /* After sleep callback. */
        /* 4 后置回调函数 - beforeSleep  （server.c 中的方法，服务器初始化时绑定的 - initServer方法）*/
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
            eventLoop->aftersleep(eventLoop);

        /*--------------------------IO 线程激活代码区 ---------------------------------*/


        // 分发事件
        // 5 遍历所有已产生的事件，并调用相应的事件处理器来处理这些事件
        for (j = 0; j < numevents; j++) {

            // 5.1 从已就绪数组中获取文件描述符信息
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;
            int fired = 0; /* Number of events fired for current fd. */

            /* Normally we execute the readable event first, and the writable
             * event later. This is useful as sometimes we may be able
             * to serve the reply of a query immediately after processing the
             * query.
             *
             * However if AE_BARRIER is set in the mask, our application is
             * asking us to do the reverse: never fire the writable event
             * after the readable. In such a case, we invert the calls.
             * This is useful when, for instance, we want to do things
             * in the beforeSleep() hook, like fsyncing a file to disk,
             * before replying to a client. */
            int invert = fe->mask & AE_BARRIER;

            /* Note the "fe->mask & mask & ..." code: maybe an already
             * processed event removed an element that fired and we still
             * didn't processed, so we check if the event is still valid.
             *
             * Fire the readable event if the call sequence is not
             * inverted. */

            // 处理事件
            // 事件具体由哪个函数来处理的呢？这就和框架中的 aeCreateFileEvents 函数有关了。

            // 5.2 如果是套接字上发生读事件，调用事件注册时设置的读事件回调处理函数
            if (!invert && fe->mask & mask & AE_READABLE) {
                fe->rfileProc(eventLoop, fd, fe->clientData, mask);
                fired++;
                fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
            }

            /* Fire the writable event. */
            // 5.3 如果是套接字上发生写事件，调用事件注册时设置的写事件回调处理函数
            if (fe->mask & mask & AE_WRITABLE) {
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    fe->wfileProc(eventLoop, fd, fe->clientData, mask);
                    fired++;
                }
            }

            /* If we have to invert the call, fire the readable event now
             * after the writable one.
             *
             * 如果需要反转调用，在可写事件之后触发可读事件
             */
            if (invert) {
                fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
                if ((fe->mask & mask & AE_READABLE) &&
                    (!fired || fe->wfileProc != fe->rfileProc)) {
                    fe->rfileProc(eventLoop, fd, fe->clientData, mask);
                    fired++;
                }
            }

            processed++;
        }
    }
    /* Check time events */
    // 三、6 检测时间事件是否触发
    if (flags & AE_TIME_EVENTS)
        // 执行时间事件
        processed += processTimeEvents(eventLoop);

    // 返回已经处理的文件或时间
    return processed; /* return the number of processed file/time events */
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception
 *
 * 在给定毫秒内等待 fd 的给定类型事件产生，直到事件成功产生，或者等待超时
 *
 * @ fd  文件描述符
 * @ mask 事件类型
 * @ 毫秒数
 */
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds)) == 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}

/**
 * 事件处理器的主循环
 * 说明：
 * 1 用一个循环不停地判断事件循环的停止标记，如果事件循环的停止标记被设置为 true ，那么针对事件捕获、分发和处理的整个主循环就停止了；否则主循环会一直执行。
 * 2 按照事件驱动框架的编程规范来说，框架主循环是在服务器程序初始化完成后，就会开始执行
 * @param eventLoop
 */
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;

    // 主线程进入主循环，一直处理事件，直到服务器关闭
    while (!eventLoop->stop) {
       // 负责事件捕获、分发和处理
        aeProcessEvents(eventLoop, AE_ALL_EVENTS |
                                   AE_CALL_BEFORE_SLEEP |
                                   AE_CALL_AFTER_SLEEP);
    }
}

/*
 * 返回所使用的多路复用库的名称
 */
char *aeGetApiName(void) {
    return aeApiName();
}

/*
 * 设置处理事件前需要被执行的函数
 */
void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}

/*
 * 设置处理事件后需要被执行的函数
 */
void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep) {
    eventLoop->aftersleep = aftersleep;
}
