package com.vv51.vv_common_util.http_util;

/**
 * Created by Kim on 2016/10/27.
 */
public class Test {
    private int roomid;
    private long userid;
    private String ip;
    private String pcid;
    private String ver;
    private int dev_type;
    private int hallsvrid;

    public Test(int roomid, long userid, String ip, String pcid, String ver, int dev_type, int hallsvrid) {
        this.roomid = roomid;
        this.userid = userid;
        this.ip = ip;
        this.pcid = pcid;
        this.ver = ver;
        this.dev_type = dev_type;
        this.hallsvrid = hallsvrid;
    }

    public int getRoomid() {
        return roomid;
    }
    public void setRoomid(int roomid) {
        this.roomid = roomid;
    }

    public long getUserid() {
        return userid;
    }
    public void setUserid(long userid) {
        this.userid = userid;
    }

    public String getIp() {
        return ip;
    }
    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPcid() {
        return pcid;
    }
    public void setPcid(String pcid) {
        this.pcid = pcid;
    }

    public String getVer() {
        return ver;
    }
    public void setVer(String ver) {
        this.ver = ver;
    }

    public int getDev_type() {
        return dev_type;
    }
    public void setDev_type(int dev_type) {
        this.dev_type = dev_type;
    }

    public int getHallsvrid() {
        return hallsvrid;
    }
    public void setHallsvrid(int hallsvrid) {
        this.hallsvrid = hallsvrid;
    }

    @Override
    public String toString() {
        return "MicUpPowerQueryRequest{" +
                "roomid=" + roomid +
                ", userid=" + userid +
                ", ip='" + ip + '\'' +
                ", pcid='" + pcid + '\'' +
                ", ver='" + ver + '\'' +
                ", dev_type=" + dev_type +
                ", hallsvrid=" + hallsvrid +
                '}';
    }
}
