package com.foundation.canal.config;

/**
 * @author fqh
 * xml配置类
 * @version 1.0  2016/9/25
 */
public abstract class CanalConfig {
    protected String destination;
    protected String username;
    protected String password;
    protected String subscribeChannel;
    protected int fetchSize;

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSubscribeChannel() {
        return subscribeChannel;
    }

    public void setSubscribeChannel(String subscribeChannel) {
        this.subscribeChannel = subscribeChannel;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }
}
