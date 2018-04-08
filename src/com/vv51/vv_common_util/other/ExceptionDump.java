package com.vv51.vv_common_util.other;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionDump {
    public static String getErrorInfoFromException(Exception e) {  
        try {  
            StringWriter sw = new StringWriter();  
            PrintWriter pw = new PrintWriter(sw);  
            e.printStackTrace(pw);  
            return "StackTrace:" + sw.toString() + "\n";  
        } catch (Exception e2) {  
            return "bad getErrorInfoFromException";  
        }  
    }
    
    public static String getErrorInfoFromThrowable(Throwable cause) {  
        try {  
            StringWriter sw = new StringWriter();  
            PrintWriter pw = new PrintWriter(sw);  
            cause.printStackTrace(pw);  
            return "StackTrace:" + sw.toString() + "\n";  
        } catch (Exception e2) {  
            return "bad getErrorInfoFromThrowable";  
        }  
    }    
    
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
