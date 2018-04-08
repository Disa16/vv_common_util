package com.vv51.vv_common_util.RemoteLog;

/**
 * Created by Kim on 2016/8/9.
 */
public class RC4 {
    private int[] box = new int[256];
    private String key = null;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        byte[] k = key.getBytes();
        int i = 0, x = 0, t = 0, l = k.length;

        for (i = 0; i < 256; i++) {
            box[i] = i;
        }

        for (i = 0; i < 256; i++) {
            x = (x + box[i] + k[i % l]) % 256;

            t = box[x];
            box[x] = box[i];
            box[i] = t;
        }
        this.key = key;
    }

    private byte[] make(byte[] data) {
        int t, o, i = 0, j = 0, l = data.length;
        byte[] out = new byte[l];
        int[] ibox = new int[256];
        System.arraycopy(box, 0, ibox, 0, 256);

        for (int c = 0; c < l; c++) {
            i = (i + 1) % 256;
            j = (j + ibox[i]) % 256;

            t = ibox[j];
            ibox[j] = ibox[i];
            ibox[i] = t;

            o = ibox[(ibox[i] + ibox[j]) % 256];
            out[c] = (byte) (data[c] ^ o);
        }
        return out;
    }

    public byte[] encrypt(byte data[]) {
        return make(data);
    }

    public byte[] decrypt(byte data[]) {
        return make(data);
    }
}
