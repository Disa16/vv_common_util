package com.vv51.vv_common_util.zkagent;

import com.vv51.vv_common_util.other.ExceptionDump;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.nodes.PersistentNodeListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Kim on 2018/1/18.
 */
public class CuratorWapper {
    private static Logger logger = LogManager.getLogger(CuratorWapper.class);

    private CuratorFramework curatorFramework = null;
    private PersistentNode persistentNode = null;

    private String serverAddr = "";
    private String registerPath;
    private String registerData;
    private boolean persistent;
    private boolean sequential;
    private List<ACL> acl;

    public interface NODE_CREATED_CALLBACK {
        void onNodeCreateCallback(String path);
    }

    public interface NODE_CHANGED_CALLBACK {
        void onNodeChangedCallback(String path, String data);
    }

    public interface CHILDREN_CHANGED_CALLBACK {
        void onChildrenChangedCallback(String childPath, String childData, PathChildrenCacheEvent.Type type);
    }

    private NODE_CREATED_CALLBACK node_created_callback = null;

    public CuratorWapper(String serverAddr, String registerPath, String registerData, boolean persistent, boolean
            sequential, List<ACL> acl, NODE_CREATED_CALLBACK node_created_callback) {
        this.serverAddr = serverAddr;
        this.registerPath = registerPath;
        this.registerData = registerData;
        this.persistent = persistent;
        this.sequential = sequential;
        this.acl = acl;
        this.node_created_callback = node_created_callback;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        uninit();
    }

    public int init() {
        int ret = 0;
        try {
            curatorFramework = CuratorFrameworkFactory.newClient(serverAddr, new ExponentialBackoffRetry(1000, 3));
            curatorFramework.start();
            if (null != registerPath && !registerPath.equals("")) {
                if (persistent) {
                    if (sequential) {
                        persistentNode = new PersistentNode(curatorFramework, CreateMode.PERSISTENT_SEQUENTIAL, false, registerPath, registerData.getBytes());

                    } else {
                        persistentNode = new PersistentNode(curatorFramework, CreateMode.PERSISTENT, false, registerPath, registerData.getBytes());
                    }
                } else {
                    if (sequential) {
                        persistentNode = new PersistentNode(curatorFramework, CreateMode.EPHEMERAL_SEQUENTIAL, false, registerPath, registerData.getBytes());
                    } else {
                        persistentNode = new PersistentNode(curatorFramework, CreateMode.EPHEMERAL, false, registerPath, registerData.getBytes());
                    }
                }
                persistentNode.getListenable().addListener(new PersistentNodeListener() {
                    @Override
                    public void nodeCreated(String s) throws Exception {
                        if (null != node_created_callback) {
                            node_created_callback.onNodeCreateCallback(s);
                        }
                    }
                });
                persistentNode.start();
            }
        } catch (Exception e) {
            if (null != curatorFramework) {
                curatorFramework.close();
            }
            ret = -100;
            logger.error("");
        }
        return ret;
    }

    public void uninit() {
        if (null != curatorFramework) {
            curatorFramework.close();
        }
    }

    public int setData(String path, String data) {
        int ret = 0;
        try {
            Stat stat = curatorFramework.setData().forPath(path, data.getBytes());
            logger.debug("CuratorWapper [SET_DATA] success! path:" + path + ", data:" + data + ", stat:" + stat.toString());
        } catch (Exception e) {
            logger.error("CuratorWapper [SET_DATA] error[Exception]! path:" + path + ", data:" + data + " " + ExceptionDump.getErrorInfoFromException(e));
        }
        return ret;
    }

    public String getData(String path) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            byte[] data = curatorFramework.getData().forPath(path);
            stringBuilder.append(new String(data));
            logger.debug("CuratorWapper [GET_DATA] success! path:" + path + ", data:" + stringBuilder.toString());
        } catch (Exception e) {
            logger.error("CuratorWapper [GET_DATA] error[Exception]! path:" + path + " " + ExceptionDump.getErrorInfoFromException(e));
        }
        return stringBuilder.toString();
    }

    public HashMap<String, String> getChildren(String path) {
        HashMap<String, String> childrenHashMap = null;
        try {
            List<String> childrenList = curatorFramework.getChildren().forPath(path);
            if (null != childrenList) {
                childrenHashMap = new HashMap<>();
                for (String child : childrenList) {
                    String childPath = path + "/" + child;
                    String childData = getData(childPath);
                    childrenHashMap.put(childPath, childData);
                }
            }
        } catch (Exception e) {
            logger.error("CuratorWapper [GET_CHILDREN] error[Exception]! path:" + path + " " + ExceptionDump.getErrorInfoFromException(e));
        }
        return childrenHashMap;
    }

    public int watchNode(String path, NODE_CHANGED_CALLBACK nodeChangedCallback) {
        int ret = 0;
        try {
            NodeCache nodeCache = new NodeCache(curatorFramework, path, false);
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    String currentPath = nodeCache.getPath();
                    String data = new String(nodeCache.getCurrentData().getData());
                    nodeChangedCallback.onNodeChangedCallback(currentPath, data);
                }
            });
            nodeCache.start();
        } catch (Exception e) {
            ret = -100;
            logger.error("CuratorWapper [WATCH_NODE] error[Exception]! path:" + path + " " + ExceptionDump.getErrorInfoFromException(e));
        }
        return ret;
    }

    public int watchChildren(String path, CHILDREN_CHANGED_CALLBACK childrenChangedCallback) {
        int ret = 0;
        try {
            PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, path, true);
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent
                        pathChildrenCacheEvent) throws Exception {
                    if (null != pathChildrenCacheEvent && null != pathChildrenCacheEvent.getData()) {
                        String childPath = pathChildrenCacheEvent.getData().getPath();
                        String childData = null;
                        byte[] childDataBytes = pathChildrenCacheEvent.getData().getData();
                        if (null != childDataBytes) {
                            childData = new String(childDataBytes);
                        } else {
                            childData = "";
                        }
                        childrenChangedCallback.onChildrenChangedCallback(childPath, childData, pathChildrenCacheEvent.getType());
                    }
                }
            });
            pathChildrenCache.start();
        } catch (Exception e) {
            ret = -100;
            logger.error("CuratorWapper [WATCH_CHILDREN] error[Exception]! path:" + path + " " + ExceptionDump.getErrorInfoFromException(e));
        }
        return ret;
    }

    public static void main(String[] args) {
        CuratorWapper curatorWapper = new CuratorWapper("182.118.27.56:2181,182.118.27.58:2181,182.118.27.59:2181", "/test/curator_test/test", "test", false, true, null, new NODE_CREATED_CALLBACK() {
            @Override
            public void onNodeCreateCallback(String path) {
                System.out.println("onNodeCreateCallback CALLED! path:" + path);
            }
        });
        int ret = curatorWapper.init();
        System.out.println("ret:" + ret);

        curatorWapper.watchNode("/test/curator_test", new NODE_CHANGED_CALLBACK() {
            @Override
            public void onNodeChangedCallback(String path, String data) {
                System.out.println("Path:" + path + ", Data:" + data);
            }
        });
        System.out.println("----------------------------------------------------------------------");
        //System.out.println("GET_CHILDRED:" + curatorWapper.getChildren("/test"));
        curatorWapper.watchChildren("/chatroom/new_cms/config_data/business", new CHILDREN_CHANGED_CALLBACK() {
            @Override
            public void onChildrenChangedCallback(String childPath, String childData, PathChildrenCacheEvent.Type type) {
                System.out.println("childPath:" + childPath + ", childData:" + childData + ", type:" + type);
            }
        });
        System.out.println("----------------------------------------------------------------------");


        try {
            Thread.sleep(300000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
