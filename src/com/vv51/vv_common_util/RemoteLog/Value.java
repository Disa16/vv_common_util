package com.vv51.vv_common_util.RemoteLog;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Kim on 2016/8/9.
 */
class Value
{
    public Value()
    {
        this.value = null;
    }

    public Value(Object o)
    {
        this.value = o;
    }

    static String escapedString(String data) throws IOException{
        Writer writer = new CharArrayWriter();

        char[] var2 = data.toCharArray();

        for(int var3 = 0; var3 < var2.length; ++var3) {
            if(var2[var3] == 34) {
                writer.append("\\\"");
            } else if(var2[var3] == 39) {
                writer.append("\'");
            } else if(var2[var3] == 92) {
                writer.append("\\\\");
            } else if(var2[var3] == 47) {
                writer.append("\\/");
            } else if(var2[var3] == 7) {
                writer.append("\\a");
            } else if(var2[var3] == 8) {
                writer.append("\\b");
            } else if(var2[var3] == 9) {
                writer.append("\\t");
            } else if(var2[var3] == 10) {
                writer.append("\\n");
            } else if(var2[var3] == 11) {
                writer.append("\\v");
            } else if(var2[var3] == 12) {
                writer.append("\\f");
            } else if(var2[var3] == 13) {
                writer.append("\\r");
            } else if(var2[var3] == 0) {
                writer.append("\\0");
            } else if(var2[var3] > 127 && var2[var3] < '\uffff') {
                writer.append("\\u");
                writer.append(String.format("%04X", new Object[]{Integer.valueOf(var2[var3])}));
            } else {
                writer.append(var2[var3]);
            }
        }
        return writer.toString();
    }

    public void putValue(String key, Value value)
    {
        if (subvalue == null)
        {
            subvalue = new HashMap<>();
        }
        this.subvalue.put(key,value);
    }

    public String toString()
    {
        try {
            return to_string();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "null";
    }

    private String to_string() throws IOException
    {
        if (subvalue == null)
        {
            if (value == null)
                return  "{}";
            if (value instanceof String)
            {
                return "\"" + escapedString((String) value) + "\"";
            }
            else {
                return value.toString();
            }
        }
        Writer writer = new CharArrayWriter();
        writer.append("{");

        Iterator iter = subvalue.entrySet().iterator();

        boolean first = false;
        while(iter.hasNext())
        {
            if (first)
                writer.append(",");
            Map.Entry pair = (Map.Entry)iter.next();
            writer.append("\"");
            writer.append(escapedString((String)pair.getKey()));
            writer.append("\":");
            Value v = (Value) pair.getValue();
            if (v == null)
                writer.append("null");
            else
                writer.append(v.toString());
            first = true;
        }
        writer.append("}");
        return writer.toString();
    }
    private Map<String, Value> subvalue;
    private Object value;
}
