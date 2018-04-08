package com.vv51.vv_common_util.encrypt;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

public class EncryptUtil {
    //private static Logger logger = LogManager.getLogger(EncryptUtil.class);
    
    private SecretKeySpec mSecretKeySpec = null;
    IvParameterSpec mIvParameterSpec     = null;
    
    public EncryptUtil(String key, String ivector) {
        mSecretKeySpec   = new SecretKeySpec(key.getBytes(), "AES");
        mIvParameterSpec = new IvParameterSpec(ivector.getBytes());
    }    
    
    /**  
     * 文件file进行加密并保存目标文件destFile�? 
     *  
     * @param file   要加密的文件
     * @param destFile 加密后文�?
     */   
    public void encryptFile(String file, String destFile) throws Exception {   
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, mSecretKeySpec, mIvParameterSpec);   
        InputStream inputStream = new FileInputStream(file);   
        OutputStream outputStream = new FileOutputStream(destFile);   
        CipherInputStream cipherInputStream = new CipherInputStream(inputStream, cipher);   
        byte[] buffer = new byte[1024];   
        int r;   
        while ((r = cipherInputStream.read(buffer)) > 0) {   
            outputStream.write(buffer, 0, r);   
        }   
        cipherInputStream.close();   
        inputStream.close();   
        outputStream.close();   
    }
    
    /**  
     * 加密流
     *  
     * @param inputStream 要加密的流
     */   
    public InputStream encryptInputStream(InputStream inputStream) throws Exception {   
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");  
        cipher.init(Cipher.ENCRYPT_MODE, mSecretKeySpec, mIvParameterSpec);   
        CipherInputStream cipherInputStream = new CipherInputStream(inputStream, cipher);
        return cipherInputStream;
    }    
    
    /**  
     * 文件采用DES算法解密文件  
     *  
     * @param file 已加密的文件  
     * @param dest 解密后文件
     */   
    public void decryptFile(String file, String dest) throws Exception {   
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");   
        cipher.init(Cipher.DECRYPT_MODE, mSecretKeySpec, mIvParameterSpec);
        InputStream inputStream = new FileInputStream(file);   
        OutputStream outputStream = new FileOutputStream(dest);   
        CipherOutputStream cipherOutputStream = new CipherOutputStream(outputStream, cipher);   
        byte[] buffer = new byte[1024];   
        int r;   
        while ((r = inputStream.read(buffer)) >= 0) {   
            System.out.println();  
            cipherOutputStream.write(buffer, 0, r);   
        }   
        cipherOutputStream.close();   
        outputStream.close();   
        inputStream.close();   
    }

    /**
     *
     * @param inputStream
     * @return
     * @throws Exception
     */
    public InputStream decryptInputStream(InputStream inputStream) throws Exception {   
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");  
        cipher.init(Cipher.DECRYPT_MODE, mSecretKeySpec, mIvParameterSpec);   
        CipherInputStream cipherInputStream = new CipherInputStream(inputStream, cipher);
        return cipherInputStream;
    }


    public static void main(String[] args) {
        EncryptUtil encryptUtil = new EncryptUtil("51vv.com51vv.com", "com.vv51com.vv51");
        try {
            //encryptUtil.encryptFile("D:\\jdbc.properties", "D:\\encrypt_jdbc.properties");
            encryptUtil.decryptFile("D:\\TEMP\\encrypt_jdbc.properties", "D:\\TEMP\\jdbc.properties");

            //encryptUtil.encryptFile("D:\\SecureCRT Download\\jdbc.properties", "D:\\SecureCRT Download\\encrypt_jdbc.properties");
            //encryptUtil.decryptFile("D:\\SecureCRT Download\\encrypt_jdbc.properties", "D:\\SecureCRT Download\\jdbc.properties");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
















