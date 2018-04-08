package com.vv51.vv_common_util.db_connection_pool;

/**
 * Created by Kim on 2017/10/16.
 */
public class ComboPooledDataSourceInfo {
    private String name;
    private int numConnections;
    private int numBusyConnections;
    private int numIdleConnections;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNumConnections() {
        return numConnections;
    }

    public void setNumConnections(int numConnections) {
        this.numConnections = numConnections;
    }

    public int getNumBusyConnections() {
        return numBusyConnections;
    }

    public void setNumBusyConnections(int numBusyConnections) {
        this.numBusyConnections = numBusyConnections;
    }

    public int getNumIdleConnections() {
        return numIdleConnections;
    }

    public void setNumIdleConnections(int numIdleConnections) {
        this.numIdleConnections = numIdleConnections;
    }

    @Override
    public String toString() {
        return "ComboPooledDataSourceInfo[" + name + "]{" +
                "numConnections=" + numConnections +
                ", numBusyConnections=" + numBusyConnections +
                ", numIdleConnections=" + numIdleConnections +
                '}';
    }
}
