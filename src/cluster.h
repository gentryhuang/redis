#ifndef __CLUSTER_H
#define __CLUSTER_H

/*-----------------------------------------------------------------------------
 * Redis cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/
// 槽数量
#define CLUSTER_SLOTS 16384

// 集群在线
#define CLUSTER_OK 0          /* Everything looks ok */

// 集群下线
#define CLUSTER_FAIL 1        /* The cluster can't work */

// 集群节点名字的长度
#define CLUSTER_NAMELEN 40    /* sha1 hex length */

// 集群的实际端口号
#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). */

// 检验下线报告的乘法因子
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */

// 撤销主节点 FAIL 状态的乘法因子
#define CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */
#define CLUSTER_FAIL_UNDO_TIME_ADD 10 /* Some additional time. */
#define CLUSTER_FAILOVER_DELAY 5 /* Seconds */
#define CLUSTER_MF_TIMEOUT 5000 /* Milliseconds to do a manual failover. */
#define CLUSTER_MF_PAUSE_MULT 2 /* Master pause manual failover mult. */

// 在进行手动的故障转移之前，需要等待的超时时间
#define CLUSTER_SLAVE_MIGRATION_DELAY 5000 /* Delay for slave migration. */

/* Redirection errors returned by getNodeByQuery(). */

// 节点可以处理这个命令
#define CLUSTER_REDIR_NONE 0          /* Node can serve the request. */

// 键在其他槽
#define CLUSTER_REDIR_CROSS_SLOT 1    /* -CROSSSLOT request. */

// 键所处的槽正在进行 reshard
#define CLUSTER_REDIR_UNSTABLE 2      /* -TRYAGAIN redirection required */

// 需要进行 ASK 转向
#define CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */

// 需要进行 MOVED 转向
#define CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */


#define CLUSTER_REDIR_DOWN_STATE 5    /* -CLUSTERDOWN, global state. */
#define CLUSTER_REDIR_DOWN_UNBOUND 6  /* -CLUSTERDOWN, unbound slot. */
#define CLUSTER_REDIR_DOWN_RO_STATE 7 /* -CLUSTERDOWN, allow reads. */

// 前置定义，防止编译错误
struct clusterNode;

/*
 * 数据结构小结：
 *
 * - clusterState:
 *   （1）集群状态，每个节点都对应一个该结构
 *   （2）该结构记录了在当前节点的视角下，整个 Redis 集群信息。如集群是在线还是下线，集群中包含多少个节点，集群当前的配置纪元
 * - clusterNode:
 *   （1）集群节点，保存了一个节点的状态信息。如 节点的创建时间、名字、ip和port
 *   （2）每个集群模式的下实例，都是使用该数据结构保存自己和自己相关的其他节点
 * - clusterLink:
 *   （1）连接节点所需的有关信息，如：套接字描述符，输入和输出缓冲区，它们用于连接节点的
 *   （2）redisClient 中的连接信息用于连接客户端的
 */

/* clusterLink encapsulates everything needed to talk with a remote node. */
/*
 * clusterLink 封装了与远程节点进行通信所需的所有内容。用于发送和接收消息
 *
 * 注意：
 *     redisClient 结构中的套接字和缓冲区是用于连接客户端的，而 clusterLink 结构中的套接字和缓冲区则是用于连接节点的。
 */
typedef struct clusterLink {
    // 连接的创建时间
    mstime_t ctime;             /* Link creation time */

    // 连接，用于连接远程节点
    connection *conn;           /* Connection to remote node */

    // 输出缓冲区，保存着等待发送给其他节点的消息（message）
    sds sndbuf;                 /* Packet send buffer */

    // 输入缓冲区，保存着从其他节点接收到的消息
    char *rcvbuf;               /* Packet reception buffer */

    size_t rcvbuf_len;          /* Used size of rcvbuf */
    size_t rcvbuf_alloc;        /* Allocated size of rcvbuf */

    // 与这个连接相关联的节点，如果没有的话就为 NULL
    struct clusterNode *node;   /* Node related to this link if any, or NULL */

} clusterLink;

/* Cluster node flags and macros. */

// 标志主节点
#define CLUSTER_NODE_MASTER 1     /* The node is a master */

// 标志从节点
#define CLUSTER_NODE_SLAVE 2      /* The node is a slave */

// 标志节点疑似下线，需要对它的状态进行确认
#define CLUSTER_NODE_PFAIL 4      /* Failure? Need acknowledge */

// 该节点已下线
#define CLUSTER_NODE_FAIL 8       /* The node is believed to be malfunctioning */

// 该节点是当前节点自身
#define CLUSTER_NODE_MYSELF 16    /* This node is myself */

// 该节点还未与当前节点完成第一次 PING - PONG 通信，即处于握手状态
#define CLUSTER_NODE_HANDSHAKE 32 /* We have still to exchange the first ping */

// 该节点没有地址
#define CLUSTER_NODE_NOADDR   64  /* We don't know the address of this node */

// 当前节点还未与该节点进行接触，带有这个标志会让当前节点发送 MEET 命令而不是 PING 命令
#define CLUSTER_NODE_MEET 128     /* Send a MEET message to this node */

// 该节点被选中为新的主节点
#define CLUSTER_NODE_MIGRATE_TO 256 /* Master eligible for replica migration. */

// slave 不会尝试故障转移
#define CLUSTER_NODE_NOFAILOVER 512 /* Slave will not try to failover. */

// 空名字
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"

// 用于判断节点身份和状态的一系列宏
#define nodeIsMaster(n) ((n)->flags & CLUSTER_NODE_MASTER)
#define nodeIsSlave(n) ((n)->flags & CLUSTER_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & CLUSTER_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n) ((n)->flags & CLUSTER_NODE_NOFAILOVER)

/* Reasons why a slave is not able to failover. */
#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD (60*5) /* seconds. */

/* clusterState todo_before_sleep flags. */
// 以下每个 flag 代表了一个服务器在开始下一个事件循环之前要做的事情
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)
#define CLUSTER_TODO_HANDLE_MANUALFAILOVER (1<<4)

/* Message types.
 * 节点间通信的消息类型
 *
 * Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list).
 *
 * 注意：
 * PING、PONG 和 MEET 实际上是同一种消息。PONG 是对 PING 的回复，它的实际格式也为 PING 消息。
 * 而 MEET 则是一种特殊的 PING 消息，用于强制消息的接收者将消息的发送者添加到集群中（如果节点尚未在节点列表中的话）
 */

// PING 消息
#define CLUSTERMSG_TYPE_PING 0          /* Ping 这是一个节点用来向其他节点发送信息的消息类型*/

// PONG (回复 PING)
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) 对 PING 消息的回复*/

// MEET 消息 （请求将节点添加到集群中）
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message 一个节点表示要加入集群的消息类型*/

// FAIL 消息 将某个节点标记为 FAIL
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */

// 通过发布与订阅功能广播消息
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */

// 请求进行故障转移操作，要求消息的接收者通过投票来支持消息的发送者
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */

// 消息的接收者同意向消息的发送者投票
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */

// 槽布局已经发生变化，消息发送者要求消息接收者进行相应的更新
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */

// 为了进行手动故障转移，暂停各个客户端
#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover */

#define CLUSTERMSG_TYPE_MODULE 9        /* Module cluster API message. */
#define CLUSTERMSG_TYPE_COUNT 10        /* Total number of message types. */

/* Flags that a module can set in order to prevent certain Redis Cluster
 * features to be enabled. Useful when implementing a different distributed
 * system on top of Redis Cluster message bus, using modules. */
#define CLUSTER_MODULE_FLAG_NONE 0
#define CLUSTER_MODULE_FLAG_NO_FAILOVER (1<<1)
#define CLUSTER_MODULE_FLAG_NO_REDIRECTION (1<<2)

/* This structure represent elements of node->fail_reports. */
// 每个 clusterNodeFailReport 结构保存了一条其他节点对目标节点的下线报告
typedef struct clusterNodeFailReport {
    // 报告目标节点已经下线的节点
    struct clusterNode *node;  /* Node reporting the failure condition. */

    // 最后一次从 node 节点收到下线报告的时间（程序使用这个时间戳来检查下线报告是否过期）
    mstime_t time;             /* Time of the last report from this node. */

} clusterNodeFailReport;


/*
 * Redis Cluster 的每个集群节点都对应一个 clusterNode 的结构体
 *
 * 1 每个节点都会使用一个 clusterNode 结构来记录自己的状态
 * 2 为集群中的所有其他节点（包括主节点和从节点）都创建一个相应的 clusterNode 结构，以此来记录其他节点的状态
 */
typedef struct clusterNode {
    // 创建节点的时间
    mstime_t ctime; /* Node object creation time. */

    // 节点的名字，由 40 个十六进制字符组成
    char name[CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size */

    // 节点标识。使用各种不同的标识值记录节点的角色（如主节点/从节点），以及节点目前所处的状态（如在线或者下线）
    int flags;      /* CLUSTER_NODE_... */

    // 节点当前的配置纪元，用于实现故障转移
    uint64_t configEpoch; /* Last configEpoch observed for this node */

    // 记录当前节点负责哪些 slots
    // slots[i] = 1 表示节点负责处理槽 i
    // slots[i] = 0 表示节点不负责处理槽 i
    // 说明：因为取出和设置 slots 数组中的任意一个二进制位的值的复杂度仅为 O(1)，因此：
    //  - 对于一个给定节点的 slots 数组来说，程序检查节点是否负责处理某个槽，复杂度是 O(1)
    //  - 将某个槽指派给节点负责，复杂度是 O(1)
    unsigned char slots[CLUSTER_SLOTS/8]; /* slots handled by this node */

    // 槽信息
    sds slots_info; /* Slots info represented by string. */

    // 当前节点负责处理的槽数量，即 slots 数组中值为 1 的二进制位的数量
    int numslots;   /* Number of slots handled by this node */

    // 如果当前节点是主节点，那么用这个属性记录从节点的数量
    int numslaves;  /* Number of slave nodes, if this is a master */

    // 指针数组，指向各个从节点
    struct clusterNode **slaves; /* pointers to slave nodes */

    // 如果当前节点是一个从节点，那么指向主节点
    struct clusterNode *slaveof; /* pointer to the master node. Note that it
                                    may be NULL even if the node is a slave
                                    if we don't have the master node in our
                                    tables. */

    // 当前 Redis 实例最后一次向当前节点（该结构）发送 PING 命令的时间戳
    mstime_t ping_sent;      /* Unix time we sent latest ping */

    // 当前 Redis 实例最后一次接收当前节点(该结构) PONG 回复的时间戳
    mstime_t pong_received;  /* Unix time we received the pong */

    // 当前 Redis 实例接收当前节点(该结构)数据的时间戳
    mstime_t data_received;  /* Unix time we received any data */

    // 最后一次被设置为 FAIL 状态的时间戳
    mstime_t fail_time;      /* Unix time when FAIL flag was set */

    // 最后一次给某个从节点投票的时间
    mstime_t voted_time;     /* Last time we voted for a slave of this master */

    // 最后一次从当前节点接收到复制偏移量的时间
    mstime_t repl_offset_time;  /* Unix time we received offset for this node */
    mstime_t orphaned_time;     /* Starting time of orphaned master condition */

    // 这个节点的复制偏移量
    long long repl_offset;      /* Last known repl offset for this node. */

    // 节点的 IP 地址
    char ip[NET_IP_STR_LEN];  /* Latest known IP address of this node */

    // 节点的端口
    int port;                   /* Latest known clients port (TLS or plain). */
    int pport;                  /* Latest known clients plaintext port. Only used
                                   if the main clients port is for TLS. */
    int cport;                  /* Latest known cluster port of this node. */

    // 保存连接当前节点所需的有关信息，如 套接字描述符、输入/输出缓冲区
    clusterLink *link;          /* TCP/IP link with this node 和该节点的 tcp 连接*/

    // 一个链表，记录了所有其他节点对该节点的下线报告
    list *fail_reports;         /* List of nodes signaling this as failing */

} clusterNode;

/*
 * Redis Cluster 针对整个集群设计了 clusterState 结构体
 *
 * 每个节点都保存着一个 clusterState 结构，这个结构记录了在当前节点的视角下，集群目前所处的状态。
 *
 * 如：集群是在线还是下线，集群包含多少个节点，集群当前的配置纪元
 *
 * 说明：使用这个结构用于将当前节点和集群其他节点关联起来
 */
typedef struct clusterState {
    // 指向当前节点的指针
    clusterNode *myself;  /* This node */

    // 集群当前的配置纪元，用于实现故障转移
    uint64_t currentEpoch;

    // 集群当前的状态：是在线还是下线
    int state;            /* CLUSTER_OK, CLUSTER_FAIL, ... */

    // 集群中处理槽的节点数量
    int size;             /* Num of master nodes with at least one slot */

    // 当前实例知道的集群节点表，包括 myself 节点
    // 字典的键为节点的名字，字典的值为 ClusterNode 结构
    dict *nodes;          /* Hash table of name -> clusterNode structures */

    // 节点黑名单，用于 CLUSTER FORGET 命令
    // 防止被 FORGET 的命令重新被添加到集群里面
    // （不过现在似乎没有在使用的样子，已废弃？还是尚未实现？）
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */

    // 记录要从当前节点迁移到目标节点的槽，以及迁移的目标节点
    // migrating_slots_to[i] = NULL 表示槽 i 未被迁移
    // migrating_slots_to[i] = clusterNode_A 表示槽 i 要从本节点迁移至节点 A
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];

    // 记录要从源节点迁移到本节点的槽，以及进行迁移的源节点
    // importing_slots_from[i] = NULL 表示槽 i 未进行导入
    // importing_slots_from[i] = clusterNode_A 表示正从节点 A 中导入槽 i
    clusterNode *importing_slots_from[CLUSTER_SLOTS];

    // 负责处理各个槽的节点，即记录了集群中所有槽的指派信息
    // 例如 slots[i] = clusterNode_A 表示槽 i 由节点 A 处理
    // 每个数组项都是一个指向 clusterNode 结构的指针。注意和 clusterNode 结构中 slots 的区别：
    // 1 clusterState.slots 数组记录了集群中所有槽的指派信息
    // 2 clusterNode.slots 数组只记录了 clusterNode 结构所代表的节点的槽指派信息
    clusterNode *slots[CLUSTER_SLOTS];

    // 记录某个 slot 中实际的 key 的数量
    uint64_t slots_keys_count[CLUSTER_SLOTS];

    // rax 类型的字典树，用来记录 slot 和 key 的对应关系，可以通过它快速找到 slot 上有哪些 key
    rax *slots_to_keys;

    /* The following fields are used to take the slave state on elections. */
    // 上次执行选举或者下次执行选举的时间
    mstime_t failover_auth_time; /* Time of previous or next election. */

    // 节点获得的投票数量
    int failover_auth_count;    /* Number of votes received so far. */

    // 如果值为 1 ，表示本节点已经向其他节点发送了投票请求
    int failover_auth_sent;     /* True if we already asked for votes. */


    int failover_auth_rank;     /* This slave rank for current auth request. */
    uint64_t failover_auth_epoch; /* Epoch of the current election. */
    int cant_failover_reason;   /* Why a slave is currently not able to
                                   failover. See the CANT_FAILOVER_* macros. */


    /* Manual failover state in common. */

    // 手动故障转移执行的时间限制
    mstime_t mf_end;            /* Manual failover time limit (ms unixtime).
                                   It is zero if there is no MF in progress. */

    /* Manual failover state of master. */
    clusterNode *mf_slave;      /* Slave performing the manual failover. */

    /* Manual failover state of slave. */
    long long mf_master_offset; /* Master offset the slave needs to start MF
                                   or -1 if still not received. */


    // 指示手动故障转移是否可以开始的标志值
    // 值为非 0 时表示各个主服务器可以开始投票
    int mf_can_start;           /* If non-zero signal that the manual failover
                                   can start requesting masters vote. */

    /* The following fields are used by masters to take state on elections. */
    // 集群最后一次进行投票的纪元
    uint64_t lastVoteEpoch;     /* Epoch of the last vote granted. */

    // 在进入下个事件循环之前要做的事情，以各个 flag 来记录
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */


    /* Messages received and sent by type. */
    long long stats_bus_messages_sent[CLUSTERMSG_TYPE_COUNT];
    long long stats_bus_messages_received[CLUSTERMSG_TYPE_COUNT];

    long long stats_pfail_nodes;    /* Number of nodes in PFAIL status,
                                       excluding nodes without address. */


} clusterState;

/* Redis cluster messages header */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages.
 *
 * 发送者发送的 MEET/PING/PONG 消息的消息体
 *
 * 说明：
 * 1 每次发送 MEET、PING、PONG 消息时，发送者都从自己的已知节点列表中随机选出两个节点（可以是主节点或从节点），
 *   并将这两个被选中节点的信息分别保存到两个 clusterMsgDataGossip 结构里面。
 * 2 当接收者接收到 MEET、PING、PONG 消息时，接收者会访问消息正文中的两个 clusterMsgDataGossip 结构，并根据自己是否认识 clusterMsgDataGossip 结构中记录的被选中节点来选择进行哪种操作。
 *   - 如果被选中节点不存在于接收者的已知节点列表，那么说明接收者是第一次接触到被选中节点，接收者将根据结构中记录的 IP地址和PORT等信息，与被选中节点进行握手
 *   - 如果被选中节点存在于接收者的已知节点列表，那么说明接收者之前已经与被选中节点进行过接触，接收者将根据 clusterMsgDataGossip 结构记录的信息，对被选中节点所对应的 clusterNode 结构进行更新。
 * 3 一个 Gossip 消息的大小为 104 字节
 */
typedef struct {

    // 被选中节点的名称
    char nodename[CLUSTER_NAMELEN]; // 40字节

    // 被选中节点最后一次发送 PING 消息的时间戳
    uint32_t ping_sent; // 4 字节

    // 被选中节点最后一次接收到 PONG 消息的时间戳
    uint32_t pong_received; // 4 字节

    // 被选中节点的 IP 地址
    char ip[NET_IP_STR_LEN]; // 46 字节 /* IP address last time it was seen */

    // 被选中节点和客户端端的通信端口
    uint16_t port;  // 2 字节            /* base port last time it was seen */
    // 被选中节点用于集群通信的端口
    uint16_t cport;  // 2 字节           /* cluster port last time it was seen */

    // 被选中节点的标识
    // 说明：这个是直观的消息，即发送者将选中节点的状态设置到该属性中
    uint16_t flags;  // 2 字节           /* node->flags copy */

    uint16_t pport;   // 2 字节          /* plaintext-port, when base port is TLS */
    uint16_t notused1; // 2 字节

} clusterMsgDataGossip;

/*
 * FAIL 消息正文
 *
 * 因为集群中的所有节点都有一个独一无二的名字，所以 FAIL 消息里只需要保存下线节点的名字。
 * 接收到消息的节点就可以根据这个名字来判断是哪个节点下线了。
 */
typedef struct {
    // 下线节点的名字
    char nodename[CLUSTER_NAMELEN];

} clusterMsgDataFail;

/*
 * PUBLISH 消息正文
 */
typedef struct {

    // 频道名长度
    uint32_t channel_len;

    // 消息长度
    uint32_t message_len;

    // 消息内容，格式为 频道名 + 消息
    // bulk_data[0 ～ channel_len-1] 为频道名
    // bulk_data[channel_len ～ channel_len+message_len-1] 为消息
    unsigned char bulk_data[8]; /* 8 bytes just as placeholder. */

} clusterMsgDataPublish;

typedef struct {
    // 节点的配置纪元
    uint64_t configEpoch; /* Config epoch of the specified instance. */

    // 节点的名字
    char nodename[CLUSTER_NAMELEN]; /* Name of the slots owner. */

    // 节点的槽布局
    unsigned char slots[CLUSTER_SLOTS/8]; /* Slots bitmap. */
} clusterMsgDataUpdate;

typedef struct {

    uint64_t module_id;     /* ID of the sender module. */

    uint32_t len;           /* ID of the sender module. */

    uint8_t type;           /* Type from 0 to 255. */

    unsigned char bulk_data[3]; /* 3 bytes just as placeholder. */

} clusterMsgModule;

/*
 * 定义了节点间通信的实际消息体
 *
 * 消息内容，消息内容根据不同的情况分为多种类型：
 *  - ping（MEET、PING、PONG）
 *  - fail
 *  - publish
 *  - update
 *  - module
 *
 * Redis 集群中的各个节点通过 Gossip 协议来交换各自关于不同节点的状态信息，其中 Gossip 协议由 MEET、PING、PONG 三种消息实现，
 * 这三种消息的正文都由两个 clusterMsgDataGossip 结构组成
 */
union clusterMsgData {

    /* PING, MEET and PONG 消息类型对应的数结构
     *
     * 因为 MEET、PING、PONG 三种消息都使用相同的消息正文，所以节点通过消息头的 type 属性来判断一条消息是 MEET 消息、PING 消息还是 PONG 消息。
     */
    struct {
        /* Array of N clusterMsgDataGossip structures */
        clusterMsgDataGossip gossip[1];
    } ping;

    /* FAIL 消息类型对应的数据结构*/
    struct {
        clusterMsgDataFail about;
    } fail;

    /* PUBLISH 消息类型对应的数据结构*/
    struct {
        clusterMsgDataPublish msg;
    } publish;

    /* UPDATE 消息类型对应的数据结构*/
    struct {
        clusterMsgDataUpdate nodecfg;
    } update;

    /* MODULE 消息类型对应的数据结构 */
    struct {
        clusterMsgModule msg;
    } module;
};

#define CLUSTER_PROTO_VER 1 /* Cluster bus protocol version. */

/*
 * 用来表示集群消息的数据结构。主要是消息头信息(消息的基础信息)，消息内容在 clusterMsgData data 属性中。
 * 包含的信息包括发送消息节点的名称、IP、集群通信端口和负责的 slots ，以及消息类型、消息长度和具体的消息体。
 *
 * 说明：
 *  currentEpoch、sender、myslots 等属性记录了发送者自身的节点信息，接收者会根据这些信息，在自己的 clusterState.nodes 字典中找到发送者对应的 clusterNode 结构，
 *  并对结构进行更新。如，接收者可以通过消息中的信息，和自己本地信息对比，确定发送者的槽指派信息以及发送者的状态和角色是否发生了改变。
 */
typedef struct {
    char sig[4];        /* Signature "RCmb" (Redis Cluster message bus). */

    // 消息的长度（包括这个消息头的长度和消息正文的长度）
    uint32_t totlen;    /* Total length of this message */

    uint16_t ver;       /* Protocol version, currently set to 1. */
    uint16_t port;      /* TCP base port number. */

    // 消息的类型（非常重要）
    uint16_t type;      /* Message type */

    // 消息正文包含的节点信息数量
    // 只在发送 MEET 、PING 和 PONG 这三种 Gossip 协议消息时使用
    uint16_t count;     /* Only used for some kind of messages. */

    // 消息发送者的配置纪元
    uint64_t currentEpoch;  /* The epoch accordingly to the sending node. */

    // 如果消息发送者是一个主节点，那么这里记录的是消息发送者的配置纪元
    // 如果消息发送者是一个从节点，那么这里记录的是消息发送者正在复制的主节点的配置纪元
    uint64_t configEpoch;   /* The config epoch if it's a master, or the last
                               epoch advertised by its master if it is a
                               slave. */

    // 节点的复制偏移量
    uint64_t offset;    /* Master replication offset if node is a master or
                           processed replication offset if node is a slave. */

    // 消息发送者的名称(ID)
    char sender[CLUSTER_NAMELEN]; /* Name of the sender node */

    // 消息发送者负责的 slots
    unsigned char myslots[CLUSTER_SLOTS/8];

    // 如果消息发送者是一个从节点，那么这里记录的是消息发送者正在复制的主节点的名字
    // 如果消息发送者是一个主节点，那么这里记录的是 REDIS_NODE_NULL_NAME
    // （一个 40 字节长，值全为 0 的字节数组）
    char slaveof[CLUSTER_NAMELEN];

    // 消息发送者的 IP
    char myip[NET_IP_STR_LEN];    /* Sender IP, if not all zeroed. */
    char notused1[32];  /* 32 bytes reserved for future usage. */
    uint16_t pport;      /* Sender TCP plaintext port, if base port is TLS */
    // 消息发送者的通信端口
    uint16_t cport;      /* Sender TCP cluster bus port */

    // 消息发送者的标识值
    uint16_t flags;      /* Sender node flags */

    // 消息发送者所处集群的状态
    unsigned char state; /* Cluster state from the POV of the sender */

    // 消息标志
    unsigned char mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_... */

    // 消息具体正文，即消息体
    // todo 消息类型很丰富
    union clusterMsgData data;

} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
#define CLUSTERMSG_FLAG0_PAUSED (1<<0) /* Master paused for manual failover. */
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1) /* Give ACK to AUTH_REQUEST even if
                                            master is up. */

/* ---------------------- API exported outside cluster.c -------------------- */
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);
int clusterRedirectBlockedClientIfNeeded(client *c);
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code);
unsigned long getClusterConnectionsCount(void);

#endif /* __CLUSTER_H */
