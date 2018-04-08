package com.vv51.vv_common_util.config;

import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vv51.vv_common_util.other.ExceptionDump;

public class Configure {
    private static Logger logger = LogManager.getLogger(Configure.class);
    private  Properties mProperties = null;
    
    public int getIntVal(String key, int defaultValue) {
        int value = defaultValue;
        
        try {
            value = Integer.parseInt(mProperties.getProperty(key));
        } catch (Exception e) {
            logger.error("Configure get int value error[Exception]! key:" + key + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
        return value;
    }
    
    public long getLongVal(String key, long defaultValue) {
        long value = defaultValue;
        try {
            value = Long.parseLong(mProperties.getProperty(key));
        } catch (Exception e) {
            logger.error("Configure get long value error[Exception]! key:" + key + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
        
        return value;
    }
    
    public String getStrVal(String key, String defaultValue) {
        String value = defaultValue;
        try {
            value = mProperties.getProperty(key, defaultValue);
        } catch (Exception e) {
            logger.error("Configure get String value error[Exception]! key:" + key + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
        
        return value;
    }

    public Properties getProperties() {
        return mProperties;
    }

    public void setProperties(Properties properties) {
        this.mProperties = properties;
    }
    
}
