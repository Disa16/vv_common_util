package com.vv51.vv_common_util.db_connection_pool;

import com.alibaba.druid.pool.DruidDataSource;
import com.vv51.vv_common_util.config.Configure;
import com.vv51.vv_common_util.other.ExceptionDump;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created by Kim on 2017/12/22.
 */
public class DruidDBConnectionPool {
    private static Logger logger = LogManager.getLogger(DruidDBConnectionPool.class);

    public static final String USER_KEY = "";
    public static final String PASSWORD_KEY = "";
    public static final String URL_KEY = "";
    public static final String DRIVER_CLASS_NAME_KEY = "";
    public static final String MAX_ACTIVE_KEY = "";
    public static final String INITIAL_SIZE_KEY = "";
    public static final String MIN_IDLE_KEY = "";

    private DruidDataSource druidDataSource = null;
    private DruidDataSourceInfo druidDataSourceInfo = null;
    private String name = "";

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!druidDataSource.isClosed()) {
            druidDataSource.close();
        }
    }

    public DruidDBConnectionPool(String name) {
        this.name = name;
    }

    public int init(Configure configure) {
        int ret = 0;
        try {
            druidDataSource = new DruidDataSource();
            druidDataSourceInfo = new DruidDataSourceInfo();
            String user = configure.getStrVal(USER_KEY, "");
            String password = configure.getStrVal(PASSWORD_KEY, "");
            String url = configure.getStrVal(URL_KEY, "");
            String driverClassName = configure.getStrVal(DRIVER_CLASS_NAME_KEY, "");
            int maxActive = configure.getIntVal(MAX_ACTIVE_KEY, 0);
            int initialSize = configure.getIntVal(INITIAL_SIZE_KEY, 0);
            int minIdle = configure.getIntVal(MIN_IDLE_KEY, 0);

            druidDataSource.setName(name);
            druidDataSource.setUsername(user);
            druidDataSource.setPassword(password);
            druidDataSource.setUrl(url);
            druidDataSource.setDriverClassName(driverClassName);
            druidDataSource.setMaxActive(maxActive);
            druidDataSource.setInitialSize(initialSize);
            druidDataSource.setMinIdle(minIdle);

            druidDataSource.setMaxWait(60000);
            druidDataSource.setTestOnBorrow(false);
            druidDataSource.setTestOnReturn(false);
            druidDataSource.setTestWhileIdle(true);
            druidDataSource.setValidationQuery("select 1");
            druidDataSource.setValidationQueryTimeout(1);
            druidDataSource.setTimeBetweenEvictionRunsMillis(60000);
            druidDataSource.setMinEvictableIdleTimeMillis(300000);
            druidDataSource.setFilters("stat");
            //druidDataSource.setRemoveAbandoned(false);
            //druidDataSource.setRemoveAbandonedTimeout(1800);
            //druidDataSource.setLogAbandoned(true);

            druidDataSource.init();
            logger.info("DruidDBConnectionPool init success! Driver:" + driverClassName + ", Url:" + url + ", User:" + user + ", Password:" + password
                    + " [maxActive:" + maxActive + ", initialSize:" + initialSize + ", minIdle:" + minIdle + "]");
        } catch (Exception e) {
            logger.error("DruidDBConnectionPool init error[Exception]! "
                    + ExceptionDump.getErrorInfoFromException(e));
            ret = -100;
        }
        return ret;
    }

    public int init(String user, String password, String url, String driver, int maxActive, int initialSize, int minIdle) {
        int ret = 0;
        try {
            druidDataSource = new DruidDataSource();
            druidDataSourceInfo = new DruidDataSourceInfo();
            druidDataSource.setName(name);
            druidDataSource.setUsername(user);
            druidDataSource.setPassword(password);
            druidDataSource.setUrl(url);
            druidDataSource.setDriverClassName(driver);
            druidDataSource.setMaxActive(maxActive);
            druidDataSource.setInitialSize(initialSize);
            druidDataSource.setMinIdle(minIdle);

            druidDataSource.setMaxWait(60000);
            druidDataSource.setTestOnBorrow(false);
            druidDataSource.setTestOnReturn(false);
            druidDataSource.setTestWhileIdle(true);
            druidDataSource.setValidationQuery("select 1");
            druidDataSource.setValidationQueryTimeout(1);
            druidDataSource.setTimeBetweenEvictionRunsMillis(60000);
            druidDataSource.setMinEvictableIdleTimeMillis(300000);
            druidDataSource.setFilters("stat");
            //druidDataSource.setRemoveAbandoned(false);
            //druidDataSource.setRemoveAbandonedTimeout(1800);
            //druidDataSource.setLogAbandoned(true);

            druidDataSource.init();
            logger.info("DruidDBConnectionPool init success! Driver:" + driver + ", Url:" + url + ", User:" + user + ", Password:" + password
                    + " [maxActive:" + maxActive + ", initialSize:" + initialSize + ", minIdle:" + minIdle + "]");
        } catch (Exception e) {
            logger.error("DruidDBConnectionPool init error[Exception]! Driver:" + driver + ", Url:" + url + ", User:" + user + ", Password:" + password
                    + " [maxActive:" + maxActive + ", initialSize:" + initialSize + ", minIdle:" + minIdle + "] " + ExceptionDump.getErrorInfoFromException(e));
            ret = -100;
        }
        return ret;
    }

    public void unInit() {
        if (!druidDataSource.isClosed()) {
            druidDataSource.close();
        }
        logger.debug("druidDataSource close! druidDataSource:" + druidDataSource);
    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            connection = druidDataSource.getConnection();
            logger.trace("DruidDBConnectionPool get connection success! connection:" + connection);
        } catch (SQLException e) {
            logger.error("DruidDBConnectionPool get connection error[sql exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("DruidDBConnectionPool get connection error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
        return connection;
    }

    public void freeConnection(Connection connection) {
        try {
            connection.close();
            logger.trace("DruidDBConnectionPool free connection success!");
        } catch (SQLException e) {
            logger.error("DruidDBConnectionPool free connection error[sql exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("DruidDBConnectionPool free connection error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
    }

    public DruidDataSourceInfo getDruidDataSourceInfo() {
        try {
            druidDataSourceInfo.setName(name);
            druidDataSourceInfo.setActiveCount(druidDataSource.getActiveCount());
            druidDataSourceInfo.setCloseCount(druidDataSource.getCloseCount());
            druidDataSourceInfo.setCreateCount(druidDataSource.getCreateCount());
            druidDataSourceInfo.setDestroyCount(druidDataSource.getDestroyCount());
            druidDataSourceInfo.setErrorCount(druidDataSource.getErrorCount());
            druidDataSourceInfo.setPoolingCount(druidDataSource.getPoolingCount());
            druidDataSourceInfo.setDiscardCount(druidDataSource.getDiscardCount());
            druidDataSourceInfo.setRemoveAbandonedCount(druidDataSource.getRemoveAbandonedCount());
            druidDataSourceInfo.setResetCount(druidDataSource.getResetCount());
            druidDataSourceInfo.setConnectCount(druidDataSource.getConnectCount());
            druidDataSourceInfo.setConnectErrorCount(druidDataSource.getConnectErrorCount());
            druidDataSourceInfo.setRecycleCount(druidDataSource.getRecycleCount());
            druidDataSourceInfo.setRecycleErrorCount(druidDataSource.getRecycleErrorCount());
        } catch (Exception e) {
            logger.error("DruidDBConnectionPool get ComboPooledDataSourceInfo error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            return null;
        }
        return druidDataSourceInfo;
    }

    public Map<String, Object> getStatData() {
        return druidDataSource.getStatData();
    }

    @Override
    public String toString() {
        String poolInfo = "";
        try {
//            poolInfo = "DruidDBConnectionPool {" + name +":[ActiveCount:" + druidDataSource.getActiveCount()
//                    + ", CloseCount:" + druidDataSource.getCloseCount()
//                    + ", CreateCount:" + druidDataSource.getCreateCount()
//                    + ", DestroyCount:" + druidDataSource.getDestroyCount()
//                    + ", ErrorCount:" + druidDataSource.getErrorCount()
//                    + ", PoolingCount:" + druidDataSource.getPoolingCount()
//                    + ", DiscardCount:" + druidDataSource.getDiscardCount()
//                    + ", RemoveAbandonedCount:" + druidDataSource.getRemoveAbandonedCount()
//                    + ", ResetCount:" + druidDataSource.getResetCount()
//                    + ", ConnectCount:" + druidDataSource.getConnectCount()
//                    + ", ConnectErrorCount:" + druidDataSource.getConnectErrorCount()
//                    + ", RecycleCount:" + druidDataSource.getRecycleCount()
//                    + ", RecycleErrorCount:" + druidDataSource.getRecycleErrorCount()
//                    + "]}";
            poolInfo = druidDataSource.dump();
        } catch (Exception e) {
            logger.error("DruidDBConnectionPool dump error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
        }
        return poolInfo;
    }

    public static void main(String[] args) {
        DruidDBConnectionPool druidDBConnectionPool = new DruidDBConnectionPool("test");
        druidDBConnectionPool.init("run_chat", "q\"NbPH-wMUT/+_GV", "jdbc:sqlserver://101.251.209.184:24876;", "com.microsoft.sqlserver.jdbc.SQLServerDriver", 32, 32, 32);
        System.out.println(druidDBConnectionPool.getStatData());
        druidDBConnectionPool.unInit();


    }

}
