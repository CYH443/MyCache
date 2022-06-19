# MyCache
一个小型的分布式缓存系统。

整个系统整体分为：Memory_Kit(内存组件)、Disk_Kit(磁盘组件)、Lateral_Kit(线性组件)和Paxos_Kit(一致性组件)四部分，
主要实现了基于内存的缓存功效和磁盘的持久化存储，并且可以分布式部署在多个节点，实现消息广播、服务注册与服务发现，并基于2PC理论实现分布式系统下的数据一致性。

内存组件架构：

![image](https://user-images.githubusercontent.com/69895512/174468399-41ca04a3-c302-4add-bb61-e6b0ebd33f21.png)





磁盘组件架构：

![image](https://user-images.githubusercontent.com/69895512/174468285-14d7de0a-d6db-4d45-892f-78fbd5327bca.png)





线性组件架构：


![image](https://user-images.githubusercontent.com/69895512/174468293-7a879f8c-f38c-4a74-8c0a-abf073318c68.png)






服务注册与发现架构设计

![image](https://user-images.githubusercontent.com/69895512/174468443-7ca7cdb7-ada0-48cc-bff3-f89ff5f2a442.png)




异步事件队列机制：

![image](https://user-images.githubusercontent.com/69895512/174468484-69c6ba51-295d-46b5-9071-2e7eedc61700.png)


