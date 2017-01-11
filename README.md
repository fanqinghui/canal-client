# canal-client
阿里巴巴 canal客户端封装，与spring无缝衔接，方便开发
##功能介绍
阿里巴巴canal是数据库mysql同步的组件
##原理介绍
canal server相当于模拟mysql数据库Master的Slave
1. master一有binlog日志就把日志推送到slave（canalserver中）
2. canal server 与canal client就相当于MQ 的Broker server与消费者一样。 
3. canal server收到日志，会把binlog日志封装成消息实体，推送给订阅了此server的client
4. 客户端client接收到对应的insert update delete类别的消息实体对象，可以进行相应的处理
5. 注意：canal适合要对数据库数据进行处理存储的情况。如果两个表结构类似，不需要做业务处理，可以考虑用otter！
