package com.vv51.vv_common_util.zkagent;
/**
 *
 * @author JinDi
 * @date 2016年5月9日
 * @file ZookeeperWapper.java
 * @Description: Zookeeper 操作类
 *
 */

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vv51.vv_common_util.other.ExceptionDump;

public class ZookeeperWapper {
    private static Logger logger = LogManager.getLogger(ZookeeperWapper.class);

    private ZooKeeper mZooKeeper = null;
    private Object mZooKeeperLockObject = new Object();

    private long mSessionID = 0;

    private String mSvrList = "";
    private int mSessionTimeout = 0;

//    private boolean mRegister = false;
    private String mRegisterPath = "";
    private String mRegisterData = "";
    private RegisterCallback mRegisterCallback = null;

    private HashMap<String, WatchNodeCallback> mWatchNodeHashMap = new HashMap<>();
    private Object mWatchNodeLock = new Object();

    private HashMap<String, WatchChildrenCallback> mWatchChildrenHashMap = new HashMap<>();
    private Object mWatchChildrenLock = new Object();

    public interface WatchNodeCallback {
        public void onNodeDataResponse(String path, String nodeData);
    }

    public interface WatchChildrenCallback {
        public void onChildrenResponse(HashMap<String, String> childrenHashMap);
    }

    public interface RegisterCallback {
        public void onRegisterResponse(String registerInfo);
        public void onError(int errorcode);
    }

    private Watcher mWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            logger.info("Zookeeper Watcher process [type:" + watchedEvent.getType() + ", state:" + watchedEvent.getState() + "]");
            if(watchedEvent.getType() == EventType.NodeDataChanged) {
                WatchNodeCallback customCallback = null;
                synchronized (mWatchNodeLock) {
                    customCallback = mWatchNodeHashMap.get(watchedEvent.getPath());
                }
                if(null != customCallback) {
                    mZooKeeper.getData(watchedEvent.getPath(), true, mDataCallback, customCallback);
                } else {
                    logger.error("Zookeeper watcher process node data changed error, custom callback is null...");
                }
            } else if(watchedEvent.getType() == EventType.NodeChildrenChanged) {
                WatchChildrenCallback customCallback = null;
                synchronized (mWatchChildrenLock) {
                    customCallback = mWatchChildrenHashMap.get(watchedEvent.getPath());
                }
                if(null != customCallback) {
                    mZooKeeper.getChildren(watchedEvent.getPath(), true, mChildren2Callback, customCallback);
                } else {
                    logger.error("Zookeeper watcher process node children changed error, custom callback is null...");
                }
            } else {
                if(watchedEvent.getState() == KeeperState.AuthFailed) {
                    logger.error("Zookeeper authentication failed, shutdown! sessionid:" + mZooKeeper.getSessionId());
                    synchronized (mZooKeeperLockObject) {
                        uninit();
                    }
                } else if(watchedEvent.getState() == KeeperState.Expired) {
                    synchronized (mZooKeeperLockObject) {
                        try {
                            long oldSessionID = mZooKeeper.getSessionId();
                            //uninit();
                            close();
                            int ret = reInit();
                            if(0 != ret) {
                                logger.error("Zookeeper session expired, reconnect failed! oldSessionid:" + oldSessionID + ", svrList:" + mSvrList + ", sessionTimeout:" + mSessionTimeout + ", registerPath:" + mRegisterPath + ", registerData:" + mRegisterData);
                            } else {
                                logger.warn("Zookeeper session expired, reconnect success! oldSessionid:" + oldSessionID + ", svrList:" + mSvrList + ", sessionTimeout:" + mSessionTimeout + ", registerPath:" + mRegisterPath + ", registerData:" + mRegisterData);
                            }

//                            if (null == mRegisterPath || mRegisterPath.equals("")) {
//                                init("", mSvrList, mSessionTimeout);
//                            } else {
//                                initAndRegister("", mSvrList, mSessionTimeout, mRegisterPath, mRegisterData, mRegisterCallback);
//                            }


                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } else if(watchedEvent.getState() == KeeperState.SyncConnected) {
                    final long sessionID = mZooKeeper.getSessionId();
                    if(0 != sessionID && mSessionID != sessionID) {
                        logger.info("New session connected! sessionid:" + sessionID + ", oldSessionid:" + mSessionID + ", mRegisterPath:" + mRegisterPath + ", mRegisterData:" + mRegisterData);
                        sessionBegin();
                    }
                }
            }
        }
    };

    DataCallback mDataCallback = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            logger.info("Zookeeper WatchNode callback [rc:" + rc + ", path:" + path + ", ctx:" + ctx + ", data:" + data + ", stat:" + stat);

            if(0 != rc) {
                logger.error("Zookeeper WatchNode callback error! sessionid:" + mZooKeeper.getSessionId() + ", path:" + path + ", rc:" + rc);
                return;
            }

            WatchNodeCallback customCallback = (WatchNodeCallback)ctx;
            customCallback.onNodeDataResponse(path, new String(data));
        }
    };

    Children2Callback mChildren2Callback = new Children2Callback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            logger.info("Zookeeper WatchChildren callback [rc:" + rc + ", path:" + path + ", ctx:" + ctx + ", children:" + children + ", stat:" + stat);

            if(0 != rc) {
                logger.error("Zookeeper WatchChildren callback error! sessionid:" + mZooKeeper.getSessionId() + ", path:" + path + ", rc:" + rc);
                return;
            }

            HashMap<String, String> childrenStrings = new HashMap<>();
            for(String child : children) {
                try {
                    String subPathString = path + "/" + child;
                    byte[] byteBuffer = mZooKeeper.getData(subPathString, false, null);
                    childrenStrings.put(subPathString, new String(byteBuffer));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            WatchChildrenCallback customCallback = (WatchChildrenCallback)ctx;
            customCallback.onChildrenResponse(childrenStrings);
        }
    };

    public ZookeeperWapper() {

    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        uninit();
    }

    private int reInit() {
        int ret = 0;
        try {
            mZooKeeper = new ZooKeeper(mSvrList, mSessionTimeout, mWatcher);
            if(null == mZooKeeper) {
                logger.error("Zookeeper create failed!");
                return -1;
            }
            logger.info(String.format("Zookeeper reInit and register with parameters[svrList:%s, sessionTimeout:%d, registerPath:%s, registerData:%s]", mSvrList, mSessionTimeout, mRegisterPath, mRegisterData));
        } catch (Exception e) {
            logger.error("Zookeeper reInit and register error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
            ret = -101;
        }
        return ret;
    }

    public int initAndRegister(String logFile, String svrList, int sessionTimeout, String registerPath, String registerData, RegisterCallback registerCallback) {
        int ret = 0;
        try {
            mSvrList = svrList;
            mSessionTimeout = sessionTimeout;

            mZooKeeper = new ZooKeeper(mSvrList, mSessionTimeout, mWatcher);
            if(null == mZooKeeper) {
                logger.error("Zookeeper create failed!");
                return -1;
            }
//            mRegister = true;
            mRegisterPath = registerPath;
            mRegisterData = registerData;
            mRegisterCallback = registerCallback;
            logger.info(String.format("Zookeeper init and register with parameters[logFile:%s, svrList:%s, sessionTimeout:%d, registerPath:%s, registerData:%s]", logFile, svrList, sessionTimeout, registerPath, registerData));

        } catch (Exception e) {
            logger.error("Zookeeper init and register error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
            ret = -101;
        }
        return ret;
    }

    public int init(String logFile, String svrList, int sessionTimeout) {
        int ret = 0;
        try {
            mSvrList = svrList;
            mSessionTimeout = sessionTimeout;

            mZooKeeper = new ZooKeeper(mSvrList, mSessionTimeout, mWatcher);
            if(null == mZooKeeper) {
                logger.error("Zookeeper create failed!");
                ret = -1;
            }
//            mRegister = true;
            logger.info(String.format("Zookeeper init with parameters[logFile:%s, svrList:%s, sessionTimeout:%d]", logFile, svrList, sessionTimeout));
        } catch (Exception e) {
            logger.error("Zookeeper init error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
            ret = -101;
        }
        return ret;
    }

    public int add_auth(String scheme, String auth) {
        try {
            mZooKeeper.addAuthInfo(scheme, auth.getBytes());
        } catch (Exception e) {
            logger.error("Zookeeper add auth error[Exception]! scheme:" + scheme + ", auth:" + auth + "\n" + ExceptionDump.getErrorInfoFromException(e));
        }
        return 0;
    }

    public void uninit() {
        try {
            mZooKeeper.close();
            mZooKeeper = null;

            mSessionID = 0;
            mSvrList = null;
            mSessionTimeout = 0;

//            mRegister = false;
            mRegisterPath = null;
            mRegisterData = null;
            mRegisterCallback = null;

            logger.info("Zookeeper uninit...");
        } catch (Exception e) {
            logger.error("Zookeeper uninit error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            logger.info("Zookeeper close... sessionId:" + mZooKeeper.getSessionId());
            mZooKeeper.close();
            mZooKeeper = null;
            mSessionID = 0;
        } catch (Exception e) {
            logger.error("Zookeeper close error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
    }

    public void sessionBegin() {
//        if (!mRegister) {
//            logger.info("Zookeeper session begin return! This is not a register zk client!");
//            return;
//        }
        try {
            if (null == mRegisterPath || mRegisterPath.equals("")) {
                logger.info("Zookeeper(UnCreate Mode) will create with registerPath:" + mRegisterPath + ", registerData:" + mRegisterData);
                synchronized (mWatchNodeLock) {
                    Iterator<Entry<String, WatchNodeCallback>> watchNodeIterator = mWatchNodeHashMap.entrySet().iterator();
                    while (watchNodeIterator.hasNext()) {
                        Entry<String, WatchNodeCallback> entry = (Entry<String, WatchNodeCallback>) watchNodeIterator.next();
                        logger.info("Zookeeper SessionBegin process mWatchNodeHashMap [" + entry.getKey() + ":" + entry.getValue() + "]");
                        String registerPath = entry.getKey().toString();
                        WatchNodeCallback customCallback = (WatchNodeCallback)entry.getValue();
                        mZooKeeper.getData(registerPath, true, mDataCallback, customCallback);
                    }
                }

                synchronized (mWatchChildrenLock) {
                    Iterator<Entry<String, WatchChildrenCallback>> watchChildIterator = mWatchChildrenHashMap.entrySet().iterator();
                    while(watchChildIterator.hasNext()) {
                        Entry<String, WatchChildrenCallback> entry = (Entry<String, WatchChildrenCallback>) watchChildIterator.next();
                        logger.info("Zookeeper SessionBegin process mWatchChildrenHashMap [" + entry.getKey() + ":" + entry.getValue()  + "]");
                        String registerPath = entry.getKey().toString();
                        WatchChildrenCallback customCallback = (WatchChildrenCallback)entry.getValue();
                        mZooKeeper.getChildren(registerPath, true, mChildren2Callback, customCallback);
                    }
                }
            } else {
                logger.info("Zookeeper will create with registerPath:" + mRegisterPath + ", registerData:" + mRegisterData);
                mZooKeeper.create(mRegisterPath, mRegisterData.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, new StringCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, String name) {
                        if(0 == rc) {
                            mSessionID = mZooKeeper.getSessionId();
                            logger.info("Zookeeper create success! sessionid:" + mZooKeeper.getSessionId() + ", rc:" + rc + ", path:" + path + ", name:" + name);

                            synchronized (mWatchNodeLock) {
                                Iterator<Entry<String, WatchNodeCallback>> watchNodeIterator = mWatchNodeHashMap.entrySet().iterator();
                                while (watchNodeIterator.hasNext()) {
                                    Entry<String, WatchNodeCallback> entry = (Entry<String, WatchNodeCallback>) watchNodeIterator.next();
                                    logger.info("Zookeeper SessionBegin process mWatchNodeHashMap [" + entry.getKey() + ":" + entry.getValue() + "]");
                                    String registerPath = entry.getKey().toString();
                                    WatchNodeCallback customCallback = (WatchNodeCallback)entry.getValue();
                                    mZooKeeper.getData(registerPath, true, mDataCallback, customCallback);
                                }
                            }

                            synchronized (mWatchChildrenLock) {
                                Iterator<Entry<String, WatchChildrenCallback>> watchChildIterator = mWatchChildrenHashMap.entrySet().iterator();
                                while(watchChildIterator.hasNext()) {
                                    Entry<String, WatchChildrenCallback> entry = (Entry<String, WatchChildrenCallback>) watchChildIterator.next();
                                    logger.info("Zookeeper SessionBegin process mWatchChildrenHashMap [" + entry.getKey() + ":" + entry.getValue()  + "]");
                                    String registerPath = entry.getKey().toString();
                                    WatchChildrenCallback customCallback = (WatchChildrenCallback)entry.getValue();
                                    mZooKeeper.getChildren(registerPath, true, mChildren2Callback, customCallback);
                                }
                            }

                            if(null != mRegisterCallback) {
                                int lastSpliter = name.lastIndexOf("/");
                                String registerInfo = lastSpliter > 0 ? name.substring(lastSpliter + 1) : "";
                                mRegisterCallback.onRegisterResponse(registerInfo);
                            }
                        } else {
                            logger.error("Zookeeper create node failed! sessionid:" + mZooKeeper.getSessionId() + ", rc:" + rc + ", path:" + path + ", name:" + name);
                            mRegisterCallback.onError(rc);
                        }
                    }
                }, null);
            }
        } catch (Exception e) {
            logger.error("Zookeeper session begin error[Exception]! " + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
    }

    public void watchNode(String path, WatchNodeCallback callback) {
        synchronized (mWatchNodeLock) {
            mWatchNodeHashMap.put(path, callback);
        }
        mZooKeeper.getData(path, true, mDataCallback, callback);
    }

    public void watchChildren(String path, WatchChildrenCallback callback) {
        synchronized (mWatchChildrenLock) {
            mWatchChildrenHashMap.put(path, callback);
        }
        mZooKeeper.getChildren(path, true, mChildren2Callback, callback);
    }

    public void cancelWatchNode(String path) {
        synchronized (mWatchNodeLock) {
            mWatchNodeHashMap.remove(path);
            logger.error("Zookeeper cancel watch node:" + path);
        }
    }

    public void cancelWatchChildren(String path) {
        synchronized (mWatchChildrenLock) {
            mWatchChildrenHashMap.remove(path);
            logger.error("Zookeeper cancel watch child:" + path);
        }
    }

    public int setNodeData(String path, String data) {
        try {
            mRegisterData = data;
            Stat stat = mZooKeeper.setData(path, mRegisterData.getBytes(), -1);
            logger.info("Zookeeper set data [path:" + path + ", data:" + mRegisterData + ", stat:" + stat +"]");
        } catch (Exception e) {
            logger.error("Zookeeper set node data error[Exception]! path:" + path + ", data:" + data + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public String getNodeData(String path) {
        String nodeData = null;
        try {
            byte[] nodeDataBytes = mZooKeeper.getData(path, false, null);
            nodeData = new String(nodeDataBytes);
        } catch (KeeperException e) {
            logger.error("Zookeeper get node data error[KeeperException]! path:" + path + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.error("Zookeeper get node data error[InterruptedException]! path:" + path + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("Zookeeper get node data error[Exception]! path:" + path + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }
        return nodeData;
    }

    public List<String> getChildList(String path) {
        List<String> childList = null;

        try {
            childList = mZooKeeper.getChildren(path, false);
        } catch (KeeperException e) {
            logger.error("Zookeeper get child list error[KeeperException]! path:" + path + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.error("Zookeeper get child list error[InterruptedException]! path:" + path + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("Zookeeper get child list error[Exception]! path:" + path + "\n" + ExceptionDump.getErrorInfoFromException(e));
            e.printStackTrace();
        }

        return childList;
    }
}
