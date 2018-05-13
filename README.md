# canal-client
阿里巴巴数据同步组建canal的客户端封装，与spring无缝衔接，方便开发
## 功能介绍
阿里巴巴canal是数据库mysql同步的组件
## 原理介绍
原理相对比较简单：
canal模拟mysql slave的交互协议，伪装自己为mysql slave，向mysql master发送dump协议,这样canal server相当于模拟mysql数据库Master的Slave
1. mysql master收到dump请求，开始推送binary log给slave(也就是canal server)
2. canal server 与canal client就相当于MQ 的Broker server与消费者一样。 
3. canal server收到日志，会解析binary log对象(原始为byte流)封装成消息实体，推送给订阅了此server的client
4. 客户端client接收到对应的insert update delete类别的消息实体对象，可以进行相应的处理
5. 另外考虑到高可用canal sever必须配置zk当注册中心！
6. 注意：canal适合要对数据库数据进行处理存储的情况。如果两个表结构类似，不需要做业务处理，可以考虑用otter！
## 使用-spring配置文件配置
```
    <!--定义一个处理同步的处理类-->
      <bean id="globalCanalInvoke" class="com.xxx.canal.client.GlobalCanalInvoke"/>
    <!--canal客户端配置-->
     <config:canal-config
            id="singleCanalClient"
            destination="example"
            fetchSize="1000"
            host="localhost:2181" hostType="zkCluster">
         <!--
         id:配置id
         destination:扫描的目录。必须与canal server 配置文件里的匹配
         fetchSize:代表每次查询1000条同步记录
         host: localhost：2181代表zookeeper的地址
          -->
        <!-- 处理所有库的所有表IUD-->
        <config:globalInvoke ref="globalCanalInvoke"/>
        <config:tableInvoke>
            <!-- 指定数据库database,和表tableName-->
            <config:invoke database="testDB" tableName="test_table">
                <config:bean ref="dictIndicatorSyncInvoke"/>
            </config:invoke>
        </config:tableInvoke>
    </config:canal-config>
```

## 联系
邮件：fanqinghui100@126.com
