# MyCache
一个小型的分布式缓存系统。

整个系统整体分为：Memory_Kit(内存组件)、Disk_Kit(磁盘组件)、Lateral_Kit(线性组件)和Paxos_Kit(一致性组件)四部分，
主要实现了基于内存的缓存功效和磁盘的持久化存储，并且可以分布式部署在多个节点，实现消息广播、服务注册与服务发现，并基于2PC理论实现分布式系统下的数据一致性。

内存组件架构：

根据不同的缓存淘汰策略，设计不同的缓存组件：  
FIFOMemoryCache、LRUMemoryCache、MRUMemoryCache、SoftReferenceMemoryCache  

前三种归为一类，组件中不仅要维护一个Map集合，还有维护一个双向链表，不同的缓存淘汰策略会以不同的方式插入和删除双向队列的节点 

SoftReferenceMemoryCache软引用缓存，对象是否被回收取决于内存的紧张程度：如果内存空间足够，垃圾回收器就不会回收这个对象；如果GC后内存还是不足，就会被回收   

![image](https://user-images.githubusercontent.com/69895512/174468399-41ca04a3-c302-4add-bb61-e6b0ebd33f21.png)


磁盘组件架构：

数据在磁盘文件中以Block为单位进行存储，在内存中为每一个Block设计了一个磁盘元素的描述符(内存索引) ：IndexedDiskElementDescriptor，记录为数据在磁盘文件中的位置和大小。主要就是为了快速定位元素在磁盘文件中的位置，快读读取和写入等操作。  

![image](https://user-images.githubusercontent.com/69895512/174468285-14d7de0a-d6db-4d45-892f-78fbd5327bca.png)


线性组件架构：

基于TCP Socket进行机器节点间的网络通信，分别实现了远程数据更新、数据读取、数据删除等相关服务
![image](https://user-images.githubusercontent.com/69895512/174468293-7a879f8c-f38c-4a74-8c0a-abf073318c68.png)


服务注册与发现架构：

基于UDP组播实现服务注册与服务发现，三种广播消息类型：    
REQUEST：服务请求，要求其他节点进行服务广播   
PASSIVE：服务广播      
REMOVE：节点下线导致服务不可用，需要其他节点删除自身服务   

![image](https://user-images.githubusercontent.com/69895512/174468443-7ca7cdb7-ada0-48cc-bff3-f89ff5f2a442.png)


异步事件队列机制：

将函数调用转换成异步事件进行处理，实现数据的异步存储和多任务并发
![image](https://user-images.githubusercontent.com/69895512/174468484-69c6ba51-295d-46b5-9071-2e7eedc61700.png)

线性组件的异步处理机制流程：

![image](https://user-images.githubusercontent.com/69895512/174468598-e563ff0a-b004-4386-b5d4-3b2cf6b1978b.png)

