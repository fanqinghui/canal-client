package com.foundation.canal.config;

/**
 * @author fqh
 * @version 1.0  2016/9/25
 */
public class ZkClusterCanalConfig extends CanalConfig {
    private String zkAddress;

    public ZkClusterCanalConfig() {
    }

    public ZkClusterCanalConfig(String zkAddress, String destination, String username, String password) {
        this.zkAddress = zkAddress;
        this.destination = destination;
        this.username = username;
        this.password = password;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SingleCanalConfig{");
        sb.append("zkAddress='").append(zkAddress).append('\'');
        sb.append(", destination='").append(destination).append('\'');
        sb.append(", username='").append(username).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
