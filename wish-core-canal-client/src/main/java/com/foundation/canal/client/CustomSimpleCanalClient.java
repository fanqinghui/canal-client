package com.foundation.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.foundation.canal.config.CanalConfig;
import com.foundation.canal.config.SingleCanalConfig;
import com.foundation.canal.config.ZkClusterCanalConfig;
import com.foundation.canal.invoke.ICanalInvoke;
import com.foundation.canal.config.SocketsClusterCanalConfig;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author fqh
 * @version 1.0  2016/9/26
 */
public class CustomSimpleCanalClient implements ApplicationContextAware{

    private static final Logger logger = LoggerFactory.getLogger(CustomSimpleCanalClient.class);

    private ApplicationContext applicationContext;

    private CanalConnector connector;

    private ICanalInvoke globalInvoke;

    private Map<String, List<String>> invokeMapIOC;

    private Map<String, List<ICanalInvoke>> invokeMap;
    private LinkedBlockingQueue<Entry> customTableQueue = new LinkedBlockingQueue<Entry>();
    private CanalConfig canalConfig;
    private static final ExecutorService threadPool = Executors.newFixedThreadPool(3);

    @PostConstruct
    public void init() {
        initInvoke();
        initConector();
        startInvoke();
        startClient();
    }

    @PreDestroy
    public void  destroy(){
        threadPool.shutdownNow();
        if(connector!=null){
            connector.disconnect();
        }

    }

    public void initInvoke(){
        if(this.invokeMapIOC==null || this.invokeMapIOC.isEmpty()){
            return;
        }
        invokeMap = Maps.newConcurrentMap();
        for (Map.Entry<String, List<String>> entry : invokeMapIOC.entrySet()) {
            for (String beanId : entry.getValue()) {
                ICanalInvoke tableInvoke = (ICanalInvoke)applicationContext.getBean(beanId,ICanalInvoke.class);
                List<ICanalInvoke> canalInvokeList = invokeMap.get(entry.getKey());
                if(canalInvokeList == null){
                    canalInvokeList = new ArrayList<ICanalInvoke>();
                    invokeMap.put(entry.getKey(),canalInvokeList);
                }
                canalInvokeList.add(tableInvoke);
            }
        }
    }

    public void initConector() {
        if (canalConfig == null) {
            throw new IllegalArgumentException("CustomSimpleCanalClient canalConfig property is empty!");
        }
        if ((invokeMap == null || invokeMap.isEmpty())&& this.globalInvoke==null) {
            throw new IllegalArgumentException("CustomSimpleCanalClient invokeMap property is empty!");
        }
        if (canalConfig instanceof SingleCanalConfig) {
            String socketStr = ((SingleCanalConfig) canalConfig).getSocketAddress();
            logger.info("canal will connection to {}.", socketStr);
            connector = CanalConnectors.newSingleConnector(this.getSocketAddressByString(socketStr),
                    canalConfig.getDestination(), canalConfig.getUsername(), canalConfig.getPassword());
        } else if (canalConfig instanceof SocketsClusterCanalConfig) {
            List<SocketAddress> socketAddressList = new ArrayList<SocketAddress>();
            for (String sok : ((SocketsClusterCanalConfig) canalConfig).getSocketAddress()) {
                logger.info("canal will connection to {}.", sok);
                socketAddressList.add(this.getSocketAddressByString(sok));
            }
            connector = CanalConnectors.newClusterConnector(socketAddressList,
                    canalConfig.getDestination(), canalConfig.getUsername(), canalConfig.getPassword());
        } else if (canalConfig instanceof ZkClusterCanalConfig) {
            String zkAddress = ((ZkClusterCanalConfig) canalConfig).getZkAddress();
            logger.info("canal will connection to {}.", zkAddress);
            connector = CanalConnectors.newClusterConnector(zkAddress,
                    canalConfig.getDestination(), canalConfig.getUsername(), canalConfig.getPassword());
        }
        connector.connect();
        connector.subscribe(canalConfig.getSubscribeChannel());
        connector.rollback();
    }

    /**
     * 启动canal client服务
     * 采用异步线程方式生产数据到内存队列
     */
    public void startClient() {
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                while (!threadPool.isShutdown() && !threadPool.isTerminated()) {
                    Message message = connector.getWithoutAck(canalConfig.getFetchSize(),10L,TimeUnit.SECONDS); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            logger.warn("CustomSimpleCanalClient invoke thread interrupted!");
                        }
                    } else {
                        List<Entry> entryList = message.getEntries();
                        for (Entry entry : entryList) {
                            try {
                                customTableQueue.put(entry);
                            } catch (InterruptedException e) {
                                //do nothing
                                logger.warn("customTableQueue interrupt.", e);
                            }
                        }
                    }
                    connector.ack(batchId); // 提交确认
                }
            }
        });
    }

    /**
     * 启动内存队列消费线程
     */
    public void startInvoke() {
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                Entry entry = null;
                do {
                    try {
                        entry = customTableQueue.take();
                    } catch (InterruptedException e) {
                        //do nothing
                        logger.warn("customTableQueue interrupt.", e);
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e1) {
                        }
                        continue;
                    }
                    if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                        continue;
                    }

                    RowChange rowChage = null;
                    try {
                        rowChage = RowChange.parseFrom(entry.getStoreValue());
                    } catch (Exception e) {
                        logger.warn("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
                        continue;
                    }

                    EventType eventType = rowChage.getEventType();
                    Header header = entry.getHeader();
                    logger.info(String.format("binlog[%s:%s] , name[%s,%s] , eventType : %s",
                            header.getLogfileName(), header.getLogfileOffset(),
                            header.getSchemaName(), header.getTableName(),
                            eventType));
                    //
                    for (RowData rowData : rowChage.getRowDatasList()) {
                        String key = this.getMappingKey(header.getSchemaName(), header.getTableName());
                        //调用全局canal invoke
                        invokeMethod(globalInvoke,eventType,rowData);
                        if(invokeMap==null || invokeMap.isEmpty()){
                            continue;
                        }
                        List<ICanalInvoke> invokeList = invokeMap.get(key);
                        if (invokeList != null && !invokeList.isEmpty()) {
                            for (ICanalInvoke iCanalInvoke : invokeList) {
                                invokeMethod(iCanalInvoke,eventType,rowData);
                            }
                        }
                    }
                } while (!threadPool.isShutdown() && !threadPool.isTerminated());

            }

            /**
             * 反射调用对应处理方法（删除，新增，更新）
             * @param callback
             * @param eventType
             * @param rowData
             */
            private void invokeMethod(ICanalInvoke callback,EventType eventType, RowData rowData) {
                if (callback != null) {
                    try {
                        if (eventType == EventType.DELETE) {
                            callback.onDelete(rowData);
                        } else if (eventType == EventType.INSERT) {
                            callback.onInsert(rowData);
                        } else if (eventType == EventType.UPDATE){
                            callback.onUpdate(rowData);
                        }else{
                            logger.warn("not support eventType:{}",eventType.toString());
                        }
                    } catch (Exception e) {
                        logger.error("invoke method fail!",e);
                    }
                }
            }

            private String getMappingKey(String database, String tableName) {
                return String.format("%s.%s", database, tableName).toLowerCase();
            }
        });
    }


    private SocketAddress getSocketAddressByString(String socketStr) {
        String serverUrl = socketStr.substring(0, socketStr.lastIndexOf(":"));
        String port = socketStr.substring(socketStr.lastIndexOf(":") + 1);
        return new InetSocketAddress(serverUrl, Integer.valueOf(port));
    }

    public void setInvokeMapIOC(Map<String, List<String>> invokeMapIOC) {
        Set<Map.Entry<String, List<String>>> entrySet = invokeMapIOC.entrySet();
        this.invokeMapIOC =Maps.newConcurrentMap();
        for (Map.Entry<String, List<String>> entry : entrySet) {
            this.invokeMapIOC.put(entry.getKey().toLowerCase(), entry.getValue());
        }
    }

    public void setCanalConfig(CanalConfig canalConfig) {
        this.canalConfig = canalConfig;
    }

    public void setGlobalInvoke(ICanalInvoke globalInvoke) {
        this.globalInvoke = globalInvoke;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
