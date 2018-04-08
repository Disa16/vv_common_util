package com.vv51.vv_common_util.db_connection_pool;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import com.vv51.vv_common_util.config.Configure;
import com.vv51.vv_common_util.other.ExceptionDump;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;

public class C3p0DBConnectionPool {
    private static Logger logger = LogManager.getLogger(C3p0DBConnectionPool.class);

    public static final String USER_KEY = "";
    public static final String PASSWORD_KEY = "";
    public static final String URL_KEY = "";
    public static final String DRIVER_KEY = "";
    public static final String MAX_POOL_SIZE_KEY = "";
    public static final String INITIAL_POOL_SIZE_KEY = "";
    public static final String MIN_POOL_SIZE_KEY = "";
    public static final String MAX_IDLE_TIME_KEY = "";
    public static final String ACQUIRE_INCREMENT_KEY = "";
    public static final String MAX_STATEMENTS = "";

    private ComboPooledDataSource mComboPooledDataSource = null;
    private ComboPooledDataSourceInfo mComboPooledDataSourceInfo = new ComboPooledDataSourceInfo();
    private String mName = "";

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        DataSources.destroy(mComboPooledDataSource);
    }

    public C3p0DBConnectionPool(String name) {
        this.mName = name;
    }

    public int Init(Configure configure) {
        int ret = 0;
        try {
            String user     = configure.getStrVal(USER_KEY, "");
            String password = configure.getStrVal(PASSWORD_KEY, "");
            String url      = configure.getStrVal(URL_KEY, "");
            String driver   = configure.getStrVal(DRIVER_KEY, "");
            int maxPoolSize      = configure.getIntVal(MAX_POOL_SIZE_KEY, 0);
            int initialPoolSize  = configure.getIntVal(INITIAL_POOL_SIZE_KEY, 0);
            int minPoolSize      = configure.getIntVal(MIN_POOL_SIZE_KEY, 0);
            int maxIdleTime      = configure.getIntVal(MAX_IDLE_TIME_KEY, 0);
            int acquireIncrement = configure.getIntVal(ACQUIRE_INCREMENT_KEY, 0);
            int maxStatements    = configure.getIntVal(MAX_STATEMENTS, 50);

            mComboPooledDataSource = new ComboPooledDataSource();
            mComboPooledDataSource.setUser(user);
            mComboPooledDataSource.setPassword(password);
            mComboPooledDataSource.setJdbcUrl(url);
            mComboPooledDataSource.setDriverClass(driver);
            mComboPooledDataSource.setMaxPoolSize(maxPoolSize);
            mComboPooledDataSource.setInitialPoolSize(initialPoolSize);
            mComboPooledDataSource.setMinPoolSize(minPoolSize);
            mComboPooledDataSource.setMaxIdleTime(maxIdleTime);
            mComboPooledDataSource.setAcquireIncrement(acquireIncrement);
            mComboPooledDataSource.setMaxStatements(maxStatements);
//            mComboPooledDataSource.setTestConnectionOnCheckin(false);
//            mComboPooledDataSource.setTestConnectionOnCheckout(false);
            logger.info("C3p0DBConnectionPool init success! Driver:" + driver + ", Url:" + url + ", User:" + user + ", Password:" + password
                    + "[maxPoolSize:" + maxPoolSize + ", initialPoolSize:" + initialPoolSize + ", minPoolSize:" + minPoolSize + ", maxIdleTime:" + maxIdleTime + ", acquireIncrement:" + acquireIncrement + ", acquireIncrement:" + acquireIncrement + ", maxStatements:" + maxStatements + "]");
        } catch (Exception e) {
            logger.error("C3p0DBConnectionPool init error[Exception]! "
                    + ExceptionDump.getErrorInfoFromException(e));
            ret = -100;
        }
        return ret;
    }

    public int Init(String user, String password, String url, String driver, int maxPoolSize, int initialPoolSize, int minPoolSize, int maxIdleTime, int acquireIncrement) {
        int ret = 0;
        try {
            mComboPooledDataSource = new ComboPooledDataSource();
            mComboPooledDataSource.setUser(user);
            mComboPooledDataSource.setPassword(password);
            mComboPooledDataSource.setJdbcUrl(url);
            mComboPooledDataSource.setDriverClass(driver);

            mComboPooledDataSource.setMaxPoolSize(maxPoolSize);
            mComboPooledDataSource.setInitialPoolSize(initialPoolSize);
            mComboPooledDataSource.setMinPoolSize(minPoolSize);
            mComboPooledDataSource.setMaxIdleTime(maxIdleTime);
            mComboPooledDataSource.setAcquireIncrement(acquireIncrement);
            mComboPooledDataSource.setMaxStatements(50);
//            mComboPooledDataSource.setTestConnectionOnCheckin(false);
//            mComboPooledDataSource.setTestConnectionOnCheckout(false);
            logger.info("C3p0DBConnectionPool init success! Driver:" + driver + ", Url:" + url + ", User:" + user + ", Password:" + password
                    + "[maxPoolSize:" + maxPoolSize + ", initialPoolSize:" + initialPoolSize + ", minPoolSize:" + minPoolSize + ", maxIdleTime:" + maxIdleTime + ", acquireIncrement:" + acquireIncrement + ", acquireIncrement:" + acquireIncrement + "]");
        } catch (Exception e) {
            logger.error("C3p0DBConnectionPool init error[Exception]! Driver:" + driver + ", Url:" + url + ", User:" + user + ", Password:" + password
                    + "[maxPoolSize:" + maxPoolSize + ", initialPoolSize:" + initialPoolSize + ", minPoolSize:" + minPoolSize + ", maxIdleTime:" + maxIdleTime + ", acquireIncrement:" + acquireIncrement + ", acquireIncrement:" + acquireIncrement + "] "
                    + ExceptionDump.getErrorInfoFromException(e));
            ret = -100;
        }
        return ret;
    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            connection = mComboPooledDataSource.getConnection();
//            logger.debug("C3p0DBConnectionPool get connection success! connection:" + connection);
        } catch (SQLException e) {       
            logger.error("C3p0DBConnectionPool get connection error[sql exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();      
        } catch (Exception e) {
            logger.error("C3p0DBConnectionPool get connection error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace(); 
        }
        return connection;
    }     
    
    public void freeConnection(Connection connection) {
        try {
            connection.close();
//            logger.debug("C3p0DBConnectionPool free connection success!");
        } catch (SQLException e) {
            logger.error("C3p0DBConnectionPool free connection error[sql exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("C3p0DBConnectionPool free connection error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
    }

    public ComboPooledDataSourceInfo getComboPooledDataSourceInfo() {
        try {
            mComboPooledDataSourceInfo.setNumConnections(mComboPooledDataSource.getNumConnections());
            mComboPooledDataSourceInfo.setNumBusyConnections(mComboPooledDataSource.getNumBusyConnections());
            mComboPooledDataSourceInfo.setNumIdleConnections(mComboPooledDataSource.getNumIdleConnections());
        } catch (Exception e) {
            logger.error("C3p0DBConnectionPool get ComboPooledDataSourceInfo error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            return null;
        }
        return mComboPooledDataSourceInfo;
    }

    @Override
    public String toString() {
        String poolInfo = "";
        try {
            poolInfo = "C3p0DBConnectionPool {" + mName +":[NumConnections:" + mComboPooledDataSource.getNumConnections()
                    + ", NumBusyConnections:" + mComboPooledDataSource.getNumBusyConnections()
                    + ", NumIdleConnections:" + mComboPooledDataSource.getNumIdleConnections()
                    + "]}";
        } catch (Exception e) {
            logger.error("C3p0DBConnectionPool dump error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
        }
        return poolInfo;
    }
}
