package com.foundation.canal.spring.parser;

import com.foundation.canal.config.CanalConfig;
import com.foundation.canal.client.CustomSimpleCanalClient;
import com.foundation.canal.config.SingleCanalConfig;
import com.foundation.canal.config.SocketsClusterCanalConfig;
import com.foundation.canal.config.ZkClusterCanalConfig;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import java.util.*;

/**
 * @author fqh
 * @version 1.0  2016/9/27
 */
public class CanalConfigBeanDefinitionParser extends AbstractSingleBeanDefinitionParser{

    protected Class getBeanClass(Element element) {
        return CustomSimpleCanalClient.class;
    }

    private final Set<String> acceptHostType = Sets.newHashSet("socketCluster", "single", "zkCluster");


    @Override
    protected void doParse(Element element, ParserContext ctx, BeanDefinitionBuilder builder) {

        String destination = element.getAttribute("destination");
        destination = StringUtils.isBlank(destination) ? "example" : destination;

        String username = element.getAttribute("username");
        username = StringUtils.isBlank(username) ? "" : username;

        String password = element.getAttribute("password");
        password = StringUtils.isBlank(password) ? "" : password;

        String subscribeChannel = element.getAttribute("subscribeChannel");
        subscribeChannel = StringUtils.isBlank(subscribeChannel) ? ".*\\..*" : subscribeChannel;

        String host = element.getAttribute("host");
        if (StringUtils.isBlank(host)) {
            throw new IllegalArgumentException("canalClient config error! host can't be empty!");
        }

        String fetchSize = element.getAttribute("fetchSize");
        fetchSize = StringUtils.isBlank(fetchSize) ? "1000" : fetchSize;
        Integer fetchSizeVal = Integer.valueOf(fetchSize);
        if (fetchSizeVal <= 0) {
            fetchSizeVal = 1000;
        }

        String hostType = element.getAttribute("hostType");
        if (StringUtils.isBlank(hostType)) {
            throw new IllegalArgumentException("canalClient config error! hostType can't be empty!");
        } else if (!acceptHostType.contains(hostType)) {
            throw new IllegalArgumentException("canalClient config error! not support hostType, except " + StringUtils.join(acceptHostType, ","));
        }

        CanalConfig config = null;
        if (hostType.equals("single")) {
            SingleCanalConfig singleCanalConfig = new SingleCanalConfig();
            singleCanalConfig.setSocketAddress(host);
            config = singleCanalConfig;
        } else if (hostType.equals("socketCluster")) {
            SocketsClusterCanalConfig socketsClusterCanalConfig = new SocketsClusterCanalConfig();
            socketsClusterCanalConfig.setSocketAddress(StringUtils.split(host, ","));
            config = socketsClusterCanalConfig;
        } else if (hostType.equals("zkCluster")) {
            ZkClusterCanalConfig zkClusterCanalConfig = new ZkClusterCanalConfig();
            zkClusterCanalConfig.setZkAddress(host);
            config = zkClusterCanalConfig;
        } else {
            throw new IllegalArgumentException("canalClient config error! not support hostType, except " + StringUtils.join(acceptHostType, ","));
        }
        config.setDestination(destination);
        config.setFetchSize(fetchSizeVal);
        config.setPassword(password);
        config.setSubscribeChannel(subscribeChannel);
        config.setUsername(username);

        Element globalInvokeIdElem = DomUtils.getChildElementByTagName(element, "globalInvoke");
        Element tableInvokeMapElem = DomUtils.getChildElementByTagName(element, "tableInvoke");

        String globalInvokeId = null;
        if(globalInvokeIdElem!=null){
            globalInvokeId = globalInvokeIdElem.getAttribute("value");
        }


        Map<String,List<String>> invokeMapIOC = new HashMap<String, List<String>>();
        if(tableInvokeMapElem!=null){
            List<Element> invokeEles= DomUtils.getChildElementsByTagName(tableInvokeMapElem, "invoke");
            if(invokeEles!=null && !invokeEles.isEmpty()){
                for (Element invokeBeanElement : invokeEles) {
                    String tableName = invokeBeanElement.getAttribute("tableName");
                    String database = invokeBeanElement.getAttribute("database");
                    String mapKey = this.getMappingKey(database,tableName);

                    List<Element> refList = DomUtils.getChildElementsByTagName(invokeBeanElement, "bean");

                    if(refList == null || refList.size()==0){
                        continue;
                    }
                    for (Element refBean : refList) {
                        String beanId = refBean.getAttribute("ref");
                        if(StringUtils.isEmpty(beanId)){
                            continue;
                        }
                        List<String> beanIdList = invokeMapIOC.get(mapKey);
                        if(beanIdList == null){
                            beanIdList = new ArrayList<String>();
                            invokeMapIOC.put(mapKey,beanIdList);
                        }
                        beanIdList.add(beanId);
                    }
                }
            }
        }

        if (StringUtils.isBlank(globalInvokeId) && (invokeMapIOC.isEmpty())) {
            throw new IllegalArgumentException("canalClient config error! globalInvoke and invokeMap can't be empty!");
        }




        builder.addPropertyValue("canalConfig", config);
        if (globalInvokeId != null && !globalInvokeId.trim().isEmpty()) {
            builder.addPropertyReference("globalInvoke", globalInvokeId);
        }
        if (!invokeMapIOC.isEmpty()) {
            builder.addPropertyValue("invokeMapIOC", invokeMapIOC);
        }
    }

    private String getMappingKey(String database, String tableName) {
        return String.format("%s.%s", database, tableName).toLowerCase();
    }

}
