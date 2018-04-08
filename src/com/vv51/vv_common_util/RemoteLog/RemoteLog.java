package com.vv51.vv_common_util.RemoteLog;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

/**
 * Created by Kim on 2016/8/9.
 */
public class RemoteLog implements Runnable{

    public static final int NONE = 0;
    public static final int INFO = 1;
    public static final int WARN = 2;
    public static final int ERROR = 3;
    public static final int FATAL = 4;

    private static String RC4_KEY = "D262B18F-413E-4D56-8A89-B7BDEE63B3A1";
    static final int remote_port = 9000;
    static final byte VERSION = 1;

    private String server_name;

    private DatagramSocket socket;
    private InetAddress address;

    private Thread thread;

    private SynchronousQueue<byte []> queue = new SynchronousQueue<>();

    private RC4 rc4;

    public RemoteLog(String remote_host, String server_name) throws IOException
    {
        this.server_name = server_name;
        this.socket = new DatagramSocket();
        this.address = InetAddress.getByName(remote_host);
        this.rc4 = new RC4();
        this.rc4.setKey(RC4_KEY);
    }

    @Override
    public void run()
    {
        while (true) {
            try {
                byte[] data = queue.take();
                if (data.length == 0)
                    return;
                socket.send(new DatagramPacket(data, data.length, address, remote_port));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void start()
    {
        this.thread = new Thread(this);
        this.thread.start();
    }

    public void stop() throws InterruptedException
    {
        if (thread == null)
            return;
        queue.put(new byte[0]);
        this.thread.join();
    }

    private byte[] packet(byte[] data){
        data = rc4.encrypt(data);
        ByteBuffer byteBuffer = ByteBuffer.allocate(1 + data.length);
        byteBuffer.put(VERSION);
        byteBuffer.put(data);
        return byteBuffer.array();
    }

    public void report(int level, String event, Object...args) throws InterruptedException {
        Value data = new Value();

        data.putValue("server", new Value(server_name));
        data.putValue("level", new Value(level));
        data.putValue("event", new Value(event));
        data.putValue("type", new Value("java"));
        data.putValue("level", new Value(level));

        if (args.length  % 2 != 0)
        {
            throw new Error();
        }
        Value subdata = new Value();
        for (int i = 0; i < args.length ; i+=2)
        {
            subdata.putValue((String)args[i], new Value(args[i+1]));
        }
        data.putValue("data", subdata);
        queue.put(packet(data.toString().getBytes()));
    }

    public void report(int level, String event, Map<String, Object> datas) throws InterruptedException {
        Value data = new Value();

        data.putValue("server", new Value(server_name));
        data.putValue("level", new Value(level));
        data.putValue("event", new Value(event));
        data.putValue("type", new Value("java"));
        data.putValue("level", new Value(level));

        Value subdata = new Value();
        Iterator iter = datas.keySet().iterator();
        while (iter.hasNext())
        {
            String key = (String) iter.next();
            Object value = datas.get(key);
            subdata.putValue(key, new Value(value));
        }
        data.putValue("data", subdata);
        queue.put(packet(data.toString().getBytes()));
    }
}
