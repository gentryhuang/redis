/* Background I/O service for Redis.
 *
 * This file implements operations that we need to perform in the background.
 * Currently there is only a single operation, that is a background close(2)
 * system call. This is needed as when the process is the last owner of a
 * reference to a file closing it means unlinking it, and the deletion of the
 * file is slow, blocking the server.
 *
 * In the future we'll either continue implementing new things we need or
 * we'll switch to libeio. However there are probably long term uses for this
 * file as we may want to put here Redis specific background tasks (for instance
 * it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
 * implementation).
 *
 * DESIGN
 * ------
 *
 * The design is trivial, we have a structure representing a job to perform
 * and a different thread and job queue for every job type.
 * Every thread waits for new jobs in its queue, and process every job
 * sequentially.
 *
 * Jobs of the same type are guaranteed to be processed from the least
 * recently inserted to the most recently inserted (older jobs processed
 * first).
 *
 * Currently there is no way for the creator of the job to be notified about
 * the completion of the operation, this will only be added when/if needed.
 *
 * ----------------------------------------------------------------------------
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
#include "bio.h"

/* 后台线程，互斥和条件变量 */

// 后台线程数组，用于保存创建的线程描述符
static pthread_t bio_threads[BIO_NUM_OPS];
// 互斥锁数组
static pthread_mutex_t bio_mutex[BIO_NUM_OPS];

// 保存条件变量的两个数组
static pthread_cond_t bio_newjob_cond[BIO_NUM_OPS];
static pthread_cond_t bio_step_cond[BIO_NUM_OPS];

// 存放任务的队列（链表）数组，每个队列保存对应 bio 线程要处理的任务
// 队列中每个元素是 bio_job 结构体类型，用来表示后台任务
static list *bio_jobs[BIO_NUM_OPS];

/* The following array is used to hold the number of pending jobs for every
 * OP type. This allows us to export the bioPendingJobsOfType() API that is
 * useful when the main thread wants to perform some operation that may involve
 * objects shared with the background thread. The main thread will just wait
 * that there are no longer jobs of this type to be executed before performing
 * the sensible operation. This data is also useful for reporting. */
// 记录每种类型 job 队列中等待执行的 job 个数
static unsigned long long bio_pending[BIO_NUM_OPS];

/* This structure represents a background Job. It is only used locally to this
 * file as the API does not expose the internals at all.
 *
 * 表示后台任务的数据结构。该结构只由 API 使用，不会被暴露给外部。
 */
struct bio_job {
    // 任务创建的时间
    time_t time; /* Time at which the job was created. */

    /* Job specific arguments.*/

    // 文件描述符
    int fd; /* Fd for file based background jobs */

    // 释放所提供参数的函数
    lazy_free_fn *free_fn; /* Function that will free the provided arguments */

    // 参数列表
    void *free_args[]; /* List of arguments to be passed to the free function */
};

// 任务处理函数
void *bioProcessBackgroundJobs(void *arg);

/* Make sure we have enough stack to perform all the things we do in the
 * main thread.
 *
 * 子线程栈大小
 */
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

/* Initialize the background system, spawning the thread.
 *
 * 初始化后台任务系统，生成 bio 线程。即 调用 pthread_create 函数创建多个后台线程。
 * 说明：
 * 该函数是在 server 初始化后调用的，也就是说Redis 在完成 server 初始化后就会创建线程来执行后台任务。
 */
void bioInit(void) {
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    /* Initialization of state vars and objects
     *
     * 初始化 job 队列，以及线程状态
     */
    for (j = 0; j < BIO_NUM_OPS; j++) {

        // 线程互斥锁初始化为 NULL
        pthread_mutex_init(&bio_mutex[j], NULL);

        // 条件变量对应数组初始化为 NULL
        pthread_cond_init(&bio_newjob_cond[j], NULL);
        pthread_cond_init(&bio_step_cond[j], NULL);

        // 为每类线程创建一个任务列表（对应 bio_jobs 数组中的元素）
        bio_jobs[j] = listCreate();

        // 初始化时，各种类型的任务队列中没有等待执行的任务
        bio_pending[j] = 0;
    }

    /* Set the stack size as by default it may be small in some system*/
    // 初始化线程的属性变量 attr
    pthread_attr_init(&attr);

    // 获取线程的栈大小属性值，并根据当前栈大小和 REDIS_THREAD_STACK_SIZE 宏定义的大小（默认值为 4MB）来计算最终的栈大小属性值
    pthread_attr_getstacksize(&attr, &stacksize);
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes 针对Solaris系统做处理 */
    while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;

    // 设置栈大小这一属性值。
    pthread_attr_setstacksize(&attr, stacksize);

    /* Ready to spawn our threads. We use the single argument the thread
     * function accepts in order to pass the job ID the thread is
     * responsible of.
     *
     * 调用 phread_create 函数，为每种后台任务创建一个线程。
     * 1 循环的次数由 BIO_NUM_OPS 定义，也就是 3 次，相应的，bioInit 函数就会调用 3 次 pthread_create 函数，并创建 3 个线程。
     * 2 bioInit 函数让这 3 个线程执行的函数都是 bioProcessBackgroundJobs。
     * 3 注意，传给 bio 线程运行的函数 bioProcessBackgroundJobs 的参数分别是 0、1、2 ，这个是和任务类型对应的。
     */
    for (j = 0; j < BIO_NUM_OPS; j++) {
        void *arg = (void *) (unsigned long) j;
        // 调用 pthread_create 函数创建线程，任务体为 bioProcessBackgroundJobs
        // 参数分别为：
        // 指向线程数据结构 phread_t 的指针、
        // 指向线程属性结构 phread_attr_t 的指针、
        // 线程所要运行的函数的起始地址（也是指向函数的指针）、
        // 传给运行函数的参数
        if (pthread_create(&thread, &attr, bioProcessBackgroundJobs, arg) != 0) {
            serverLog(LL_WARNING, "Fatal: Can't initialize Background Jobs.");
            exit(1);
        }

        // 将创建的线程放到 bio_threads 数组中
        bio_threads[j] = thread;
    }
}

/**
 * 为类型为 type 的 BIO 线程添加任务
 */
void bioSubmitJob(int type, struct bio_job *job) {
    job->time = time(NULL);
    pthread_mutex_lock(&bio_mutex[type]);

    // 将新任务加入到对应类型的队列中
    listAddNodeTail(bio_jobs[type], job);

    // 记录当前类型队列中待执行的任务数
    bio_pending[type]++;

    pthread_cond_signal(&bio_newjob_cond[type]);
    pthread_mutex_unlock(&bio_mutex[type]);
}

/**
 * 创建 lazyfree 异步任务
 */
void bioCreateLazyFreeJob(lazy_free_fn free_fn, int arg_count, ...) {
    va_list valist;
    /* Allocate memory for the job structure and all required
     * arguments */
    struct bio_job *job = zmalloc(sizeof(*job) + sizeof(void *) * (arg_count));
    job->free_fn = free_fn;

    va_start(valist, arg_count);

    // 处理参数
    for (int i = 0; i < arg_count; i++) {
        job->free_args[i] = va_arg(valist, void *);
    }
    va_end(valist);
    bioSubmitJob(BIO_LAZY_FREE, job);
}

/**
 * 创建关闭文件异步任务
 */
void bioCreateCloseJob(int fd) {
    struct bio_job *job = zmalloc(sizeof(*job));
    job->fd = fd;

    bioSubmitJob(BIO_CLOSE_FILE, job);
}

/**
 * 创建 AOF异步刷盘任务 并加入到对应的任务队列中
 */
void bioCreateFsyncJob(int fd) {
    struct bio_job *job = zmalloc(sizeof(*job));
    job->fd = fd;

    bioSubmitJob(BIO_AOF_FSYNC, job);
}

/**
 * bio 线程运行的函数，也就是任务体，用于处理后台任务。
 * 说明：
 *  1 负责轮询并执行后台任务
 *  2 bio 工作模型采用生产者与消费者模型
 *
 * @param arg  从 0 开始
 * @return
 */
void *bioProcessBackgroundJobs(void *arg) {
    struct bio_job *job;

    // 将接收到的参数 arg 转成 unsigned long 类型，其实就是当前 BIO 线程的类型
    // 因为后台任务类型对应的操作码分别是 0、1、2
    unsigned long type = (unsigned long) arg;
    sigset_t sigset;

    /* Check that the type is within the right interval. */
    if (type >= BIO_NUM_OPS) {
        serverLog(LL_WARNING,
                  "Warning: bio thread started with wrong type %lu", type);
        return NULL;
    }

    // 判断 BIO 线程类型
    switch (type) {
        case BIO_CLOSE_FILE:
            redis_set_thread_title("bio_close_file");
            break;
        case BIO_AOF_FSYNC:
            redis_set_thread_title("bio_aof_fsync");
            break;

        case BIO_LAZY_FREE:
            redis_set_thread_title("bio_lazy_free");
            break;
    }

    redisSetCpuAffinity(server.bio_cpulist);

    makeThreadKillable();

    pthread_mutex_lock(&bio_mutex[type]);
    /* Block SIGALRM so we are sure that only the main thread will
     * receive the watchdog signal. */
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
        serverLog(LL_WARNING,
                  "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));


    // 死循环
    // 1 传给 bioProcessBackgroundJobs 函数的参数，分别是 0、1、2，对应了三种任务类型，所以在这个循环中 bioProcessBackgroundJobs 函数会一直不停地从某一种任务队列中取出任务来执行。
    // 2 bioProcessBackgroundJobs 函数会根据传入的任务操作类型调用相应函数来处理任务
    //  - 任务类型是 BIO_CLOSE_FILE，则调用 close 函数
    //  - 任务类型是 BIO_AOF_FSYNC，则调用 redis_fsync 函数
    //  - 任务类型是 BIO_LAZY_FREE，则再根据参数个数等情况，分别调用不同的函数：
    //    - lazyfreeFreeObjectFromBioThread
    //    - lazyfreeFreeDatabaseFromBioThread
    //    - lazyfreeFreeSlotsMapFromBioThread
    while (1) {
        listNode *ln;

        /* The loop always starts with the lock hold.*/
        // 等待被激活。条件是 bio_jobs[type] 任务队列中任务数 不为 0。尽量避免线程空转 CPU
        if (listLength(bio_jobs[type]) == 0) {
            // 阻塞等待任务的到来
            pthread_cond_wait(&bio_newjob_cond[type], &bio_mutex[type]);
            continue;
        }

        /* Pop the job from the queue. */
        // 从当前 type 类型的 bio 线程的任务队列中取出任务
        ln = listFirst(bio_jobs[type]);
        job = ln->value;
        /* It is now possible to unlock the background system as we know have
         * a stand alone job structure to process.*/
        pthread_mutex_unlock(&bio_mutex[type]);

        /* Process the job accordingly to its type. 根据作业类型处理作业, */
        /**---------  判断当前处理的后台任务类型是哪一种 ------*/

        // 1 关闭文件任务，则调用close函数
        if (type == BIO_CLOSE_FILE) {
            close(job->fd);

            // 2 AOF 异步刷盘任务，则调用redis_fsync函数
        } else if (type == BIO_AOF_FSYNC) {
            /* The fd may be closed by main thread and reused for another
             * socket, pipe, or file. We just ignore these errno because
             * aof fsync did not really fail. */
            if (redis_fsync(job->fd) == -1 &&
                errno != EBADF && errno != EINVAL) {
                int last_status;
                atomicGet(server.aof_bio_fsync_status, last_status);
                atomicSet(server.aof_bio_fsync_status, C_ERR);
                atomicSet(server.aof_bio_fsync_errno, errno);
                if (last_status == C_OK) {
                    serverLog(LL_WARNING,
                              "Fail to fsync the AOF file: %s", strerror(errno));
                }
            } else {
                atomicSet(server.aof_bio_fsync_status, C_OK);
            }

            // 3 惰性删除任务，那根据任务的参数分别调用不同的惰性删除函数执行
        } else if (type == BIO_LAZY_FREE) {
            job->free_fn(job->free_args);


        } else {
            serverPanic("Wrong job type in bioProcessBackgroundJobs().");
        }

        // 任务处理后，释放空间
        zfree(job);

        /* Lock again before reiterating the loop, if there are no longer
         * jobs to process we'll block again in pthread_cond_wait(). */
        pthread_mutex_lock(&bio_mutex[type]);

        // 删除任务节点
        listDelNode(bio_jobs[type], ln);
        // 将对应的等待任务个数减一。
        bio_pending[type]--;

        /* Unblock threads blocked on bioWaitStepOfType() if any. */
        pthread_cond_broadcast(&bio_step_cond[type]);
    }
}

/* Return the number of pending jobs of the specified type. */
unsigned long long bioPendingJobsOfType(int type) {
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* If there are pending jobs for the specified type, the function blocks
 * and waits that the next job was processed. Otherwise the function
 * does not block and returns ASAP.
 *
 * The function returns the number of jobs still to process of the
 * requested type.
 *
 * This function is useful when from another thread, we want to wait
 * a bio.c thread to do more work in a blocking way.
 */
unsigned long long bioWaitStepOfType(int type) {
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    if (val != 0) {
        pthread_cond_wait(&bio_step_cond[type], &bio_mutex[type]);
        val = bio_pending[type];
    }
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* Kill the running bio threads in an unclean way. This function should be
 * used only when it's critical to stop the threads for some reason.
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory. */
void bioKillThreads(void) {
    int err, j;

    for (j = 0; j < BIO_NUM_OPS; j++) {
        if (bio_threads[j] == pthread_self()) continue;
        if (bio_threads[j] && pthread_cancel(bio_threads[j]) == 0) {
            if ((err = pthread_join(bio_threads[j], NULL)) != 0) {
                serverLog(LL_WARNING,
                          "Bio thread for job type #%d can not be joined: %s",
                          j, strerror(err));
            } else {
                serverLog(LL_WARNING,
                          "Bio thread for job type #%d terminated", j);
            }
        }
    }
}
