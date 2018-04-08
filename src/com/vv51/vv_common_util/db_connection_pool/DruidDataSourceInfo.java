package com.vv51.vv_common_util.db_connection_pool;

/**
 * Created by Kim on 2017/12/25.
 */
public class DruidDataSourceInfo {
    private String name;
    private long activeCount;
    private long closeCount;
    private long createCount;
    private long destroyCount;
    private long errorCount;
    private int poolingCount;
    private long discardCount;
    private long removeAbandonedCount;
    private long resetCount;
    private long connectCount;
    private long connectErrorCount;
    private long recycleCount;
    private long recycleErrorCount;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getActiveCount() {
        return activeCount;
    }

    public void setActiveCount(long activeCount) {
        this.activeCount = activeCount;
    }

    public long getCloseCount() {
        return closeCount;
    }

    public void setCloseCount(long closeCount) {
        this.closeCount = closeCount;
    }

    public long getCreateCount() {
        return createCount;
    }

    public void setCreateCount(long createCount) {
        this.createCount = createCount;
    }

    public long getDestroyCount() {
        return destroyCount;
    }

    public void setDestroyCount(long destroyCount) {
        this.destroyCount = destroyCount;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    public int getPoolingCount() {
        return poolingCount;
    }

    public void setPoolingCount(int poolingCount) {
        this.poolingCount = poolingCount;
    }

    public long getDiscardCount() {
        return discardCount;
    }

    public void setDiscardCount(long discardCount) {
        this.discardCount = discardCount;
    }

    public long getRemoveAbandonedCount() {
        return removeAbandonedCount;
    }

    public void setRemoveAbandonedCount(long removeAbandonedCount) {
        this.removeAbandonedCount = removeAbandonedCount;
    }

    public long getResetCount() {
        return resetCount;
    }

    public void setResetCount(long resetCount) {
        this.resetCount = resetCount;
    }

    public long getConnectCount() {
        return connectCount;
    }

    public void setConnectCount(long connectCount) {
        this.connectCount = connectCount;
    }

    public long getConnectErrorCount() {
        return connectErrorCount;
    }

    public void setConnectErrorCount(long connectErrorCount) {
        this.connectErrorCount = connectErrorCount;
    }

    public long getRecycleCount() {
        return recycleCount;
    }

    public void setRecycleCount(long recycleCount) {
        this.recycleCount = recycleCount;
    }

    public long getRecycleErrorCount() {
        return recycleErrorCount;
    }

    public void setRecycleErrorCount(long recycleErrorCount) {
        this.recycleErrorCount = recycleErrorCount;
    }

    @Override
    public String toString() {
        return "DruidDataSourceInfo{" +
                "name='" + name + '\'' +
                ", activeCount=" + activeCount +
                ", closeCount=" + closeCount +
                ", createCount=" + createCount +
                ", destroyCount=" + destroyCount +
                ", errorCount=" + errorCount +
                ", poolingCount=" + poolingCount +
                ", discardCount=" + discardCount +
                ", removeAbandonedCount=" + removeAbandonedCount +
                ", resetCount=" + resetCount +
                ", connectCount=" + connectCount +
                ", connectErrorCount=" + connectErrorCount +
                ", recycleCount=" + recycleCount +
                ", recycleErrorCount=" + recycleErrorCount +
                '}';
    }
}
