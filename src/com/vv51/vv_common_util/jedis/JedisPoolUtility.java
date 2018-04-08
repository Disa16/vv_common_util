package com.vv51.vv_common_util.jedis;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;





//import org.apache.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vv51.vv_common_util.config.Configure;
import com.vv51.vv_common_util.other.ExceptionDump;

public class JedisPoolUtility {
    private static Logger logger = LogManager.getLogger(JedisPoolUtility.class);

    public static final String MAXACTIVE_KEY = "Jedis.MaxActive";
    public static final String MAXIDLE_KEY = "Jedis.MaxIdle";
    public static final String MAXWAIT_KEY = "Jedis.MaxWait";
    public static final String TIMEOUT_KEY = "Jedis.Timeout";
    public static final String TEST_ON_BORROW_KEY = "Jedis.TestOnBorrow";
    public static final String USE_SENTINEL_KEY = "Jedis.UseSentinel";
    public static final String HOST_KEY = "Jedis.Host";
    public static final String PORT_KEY = "Jedis.Port";
    public static final String PASSWD_KEY = "Jedis.Passwd";
    public static final String MASTERNAME_KEY = "Jedis.MasterName";
    public static final String DBID_KEY = "Jedis.DBID";

    private static final int MAXACTIVE = 100;
    private static final int MAXIDLE = 20;
    private static final long MAXWAIT_MILLIS = 2000;
    private static final boolean TEST_ON_BORROW = true;

    private static final String HOST = "localhost";
    private static final int PORT = 6379;

    private Pool<Jedis> mJedisPool = null;
    private int mDefaultDB = -1;

    private String mPoolName = "";

    public JedisPoolUtility(String mPoolName) {
        this.mPoolName = mPoolName;
    }

    @Override
    protected void finalize() throws Throwable {
        mJedisPool.close();
    }

    public boolean isInitialized() {
        return mJedisPool != null;
    }

    public boolean InitJedisPool(Configure configure) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(configure.getIntVal(MAXACTIVE_KEY, MAXACTIVE));
        jedisPoolConfig.setMaxIdle(configure.getIntVal(MAXIDLE_KEY, MAXIDLE));
        jedisPoolConfig.setMaxWaitMillis(configure.getLongVal(MAXWAIT_KEY, MAXWAIT_MILLIS));
        jedisPoolConfig.setTestOnBorrow("true".equalsIgnoreCase(configure.getStrVal(TEST_ON_BORROW_KEY, "false")));

        return InitJedisPool(configure, jedisPoolConfig);
    }

    public boolean InitJedisPool(Configure configure, JedisPoolConfig jedisPoolConfig) {
        if (mJedisPool != null) {
            return false;
        }
        boolean result = true;
        try {
            mDefaultDB = configure.getIntVal(DBID_KEY, -1);
            if (jedisPoolConfig == null) {
                jedisPoolConfig = new JedisPoolConfig();
                jedisPoolConfig.setMaxTotal(MAXACTIVE);
                jedisPoolConfig.setMaxIdle(MAXIDLE);
                jedisPoolConfig.setMaxWaitMillis(MAXWAIT_MILLIS);
                jedisPoolConfig.setTestOnBorrow(TEST_ON_BORROW);
            }

            String redisHost = configure.getStrVal(HOST_KEY, HOST);
            String redisPwd  = configure.getStrVal(PASSWD_KEY, null);
            int redisPort    = configure.getIntVal(PORT_KEY, PORT);
            int timeout      = configure.getIntVal(TIMEOUT_KEY, 2000);
            boolean useSentinel = "true".equalsIgnoreCase(configure.getStrVal(USE_SENTINEL_KEY, "false"));

            if (redisHost == null) {
                redisHost = HOST;
            }

            if (!useSentinel) {
                //普通直接连接
                if (mDefaultDB >= 0) {
                    mJedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout, redisPwd, mDefaultDB);
                } else {
                    mJedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout, redisPwd);
                }
                logger.info("InitJedisPool init success! [Host:" + redisHost + ":" + redisPort + ", defaultDB:" + mDefaultDB + "] MaxTotal:" + jedisPoolConfig.getMaxTotal());
            } else {
                String masterName = configure.getStrVal(MASTERNAME_KEY, "jedis");

                String hosts[] = redisHost.split(";", 0);
                Set<String> sentinels = new HashSet<String>();
                for (String host : hosts) {
                    sentinels.add(host);
                }
                if (mDefaultDB >= 0) {
                    mJedisPool = new JedisSentinelPool(masterName, sentinels, jedisPoolConfig, timeout, redisPwd, mDefaultDB);
                } else {
                    mJedisPool = new JedisSentinelPool(masterName, sentinels, jedisPoolConfig, timeout, redisPwd);
                }
                logger.info("InitJedisPool(Redis Sentinel) init success![MasterName:" + masterName + ", defaultDB:" + mDefaultDB + "] MaxTotal:" + jedisPoolConfig.getMaxTotal());
            }
        } catch (Exception e) {
            result = false;
            logger.error("InitJedisPool error[Exception]! \n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }

        return result;
    }

    public Jedis getJedis() {
        Jedis jedis = null;
        try {
            if (null != mJedisPool) {
                jedis = mJedisPool.getResource();
                logger.debug("Get jedis success! " + dumpJedisPool());
            } else {
                logger.error("Get jedis error[jedis pool not initialized]!");
            }
        } catch (Exception e) {
            if (null != jedis) {
                mJedisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            logger.error("Get jedis error[Exception]! \n" + ExceptionDump.getErrorInfoFromException(e));
        }

        return jedis;
    }

    public void returnJedis(Jedis jedis) {
        if (jedis != null) {
            try {
                mJedisPool.returnResource(jedis);
            } catch (Exception e) {
                logger.error("Return jedis error[Exception]! \n" + ExceptionDump.getErrorInfoFromException(e));
                e.printStackTrace();
            }
        }
    }

    public void closeJedis(Jedis jedis) {
        if (jedis != null) {
            try {
                jedis.close();
                logger.debug("Close jedis success! " + dumpJedisPool());
            } catch (Exception e) {
                logger.error("Close jedis error[Exception]! \n" + ExceptionDump.getErrorInfoFromException(e));
                e.printStackTrace();
            }
        }

    }

    public void returnBrokenJedis(Jedis jedis) {
        if (jedis != null) {
            try {
                mJedisPool.returnBrokenResource(jedis);
            } catch (Exception e) {
                logger.error("Return broken jedis error[Exception]! \n" + ExceptionDump.getErrorInfoFromException(e));
                e.printStackTrace();
            }
        }
    }

    public String dumpJedisPool() {
        String jedisPoolInfo = "JedisPool[" + mPoolName + "] Info [Active:" + mJedisPool.getNumActive() + ", Idle:" + mJedisPool.getNumIdle() + ", Waiters:" + mJedisPool.getNumWaiters() + "]";
        return jedisPoolInfo;
    }

    public static void main(String[] args) {

    }

}
