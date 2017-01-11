package com.foundation.canal.config;

/**
 * @author fqh
 * @version 1.0  2016/9/25
 */
public class SingleCanalConfig extends CanalConfig {
    private String socketAddress;

    public SingleCanalConfig() {
    }

    public SingleCanalConfig(String socketAddress, String destination, String username, String password , String subscribeChannel) {
        this.socketAddress = socketAddress;
        this.destination = destination;
        this.username = username;
        this.password = password;
        this.subscribeChannel = subscribeChannel;
    }

    public String getSocketAddress() {
        return socketAddress;
    }

    public void setSocketAddress(String socketAddress) {
        this.socketAddress = socketAddress;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SingleCanalConfig{");
        sb.append("socketAddress='").append(socketAddress).append('\'');
        sb.append(", destination='").append(destination).append('\'');
        sb.append(", username='").append(username).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", subscribeChannel='").append(subscribeChannel).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
