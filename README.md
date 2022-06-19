# MyCache
一个小型的分布式缓存系统。

整个系统整体分为：Memory_Kit(内存组件)、Disk_Kit(磁盘组件)、Lateral_Kit(线性组件)和Paxos_Kit(一致性组件)四部分，
主要实现了基于内存的缓存功效和磁盘的持久化存储，并且可以分布式部署在多个节点，实现消息广播、服务注册与服务发现，并基于2PC理论实现分布式系统下的数据一致性。

内存组件架构：
![image](https://user-images.githubusercontent.com/69895512/174468163-7ae68402-35bd-4a51-a972-54156b284e74.png)

