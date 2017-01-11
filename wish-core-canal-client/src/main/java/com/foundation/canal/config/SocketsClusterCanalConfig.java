package com.foundation.canal.config;

import java.util.Arrays;

/**
 * @author fqh
 * @version 1.0  2016/9/25
 */
public class SocketsClusterCanalConfig extends CanalConfig {
    private String[] socketAddress;

    public SocketsClusterCanalConfig() {
    }

    public SocketsClusterCanalConfig(String[] socketAddress, String destination, String username, String password) {
        this.socketAddress = socketAddress;
        this.destination = destination;
        this.username = username;
        this.password = password;
    }

    public String[] getSocketAddress() {
        return socketAddress;
    }

    public void setSocketAddress(String[] socketAddress) {
        this.socketAddress = socketAddress;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SocketsClusterConfig{");
        sb.append("socketAddress=").append(Arrays.toString(socketAddress));
        sb.append(", destination='").append(destination).append('\'');
        sb.append(", username='").append(username).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
