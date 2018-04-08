package com.vv51.vv_common_util.http_util;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import com.vv51.vv_common_util.other.ExceptionDump;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HttpClientUtils {
    private static final Logger logger = LogManager.getLogger(HttpClientUtils.class);

    private static HttpClient client = null;
    static {
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(128);
        cm.setDefaultMaxPerRoute(128);
        client = HttpClients.custom().setConnectionManager(cm).build();
    }

    /**
     * 发送一个 Post 请求, 使用指定的字符集编码.
     *
     * @param url
     * @param body
     *            RequestBody
     * @param mimeType
     *            例如 application/xml
     * @param charset
     *            编码
     * @param connTimeout
     *            建立链接超时时间,毫秒.
     * @param readTimeout
     *            响应超时时间,毫秒.
     * @return ResponseBody, 使用指定的字符集编码.
     *
     * @throws ConnectTimeoutException
     *             建立链接超时异常
     * @throws SocketTimeoutException
     *             响应超时
     * @throws Exception
     */
    public static String post(String url, String body, String mimeType, String charset, Integer connTimeout, Integer readTimeout)
            throws ConnectTimeoutException, SocketTimeoutException, Exception {
        HttpClient client = null;
        HttpPost post = new HttpPost(url);
        String result = "";
        try {
            if (StringUtils.isNotBlank(body)) {
                HttpEntity entity = new StringEntity(body, ContentType.create(mimeType, charset));
                post.setEntity(entity);
            }
            // 设置参数
            Builder customReqConf = RequestConfig.custom();
            if (connTimeout != null) {
                customReqConf.setConnectTimeout(connTimeout);
            }
            if (readTimeout != null) {
                customReqConf.setSocketTimeout(readTimeout);
            }
            post.setConfig(customReqConf.build());

            HttpResponse res;
            if (url.startsWith("https")) {
                // 执行 Https 请求.
                client = createSSLInsecureClient();
                res = client.execute(post);
            } else {
                // 执行 Http 请求.
                client = HttpClientUtils.client;
                res = client.execute(post);
            }
            result = IOUtils.toString(res.getEntity().getContent(), charset);
        } catch (Exception e) {
            logger.error("HttpClientUtils Post error[Exception]! url:" + url + ", body:" + body + ", " + ExceptionDump.getErrorInfoFromException(e));
            result = null;
            throw e;
        } finally {
            post.releaseConnection();
            if (url.startsWith("https") && client != null
                    && client instanceof CloseableHttpClient) {
                ((CloseableHttpClient) client).close();
            }
        }
        return result;
    }

    /**
     * 提交form表单
     *
     * @param url
     * @param params
     * @param connTimeout
     * @param readTimeout
     * @return
     * @throws ConnectTimeoutException
     * @throws SocketTimeoutException
     * @throws Exception
     */
    public static String postForm(String url, Map<String, String> params,
                                  Map<String, String> headers, Integer connTimeout,
                                  Integer readTimeout) throws ConnectTimeoutException, SocketTimeoutException, Exception {
        HttpClient client = null;
        HttpPost post = new HttpPost(url);
        try {
            if (params != null && !params.isEmpty()) {
                List<NameValuePair> formParams = new ArrayList<org.apache.http.NameValuePair>();
                Set<Entry<String, String>> entrySet = params.entrySet();
                for (Entry<String, String> entry : entrySet) {
                    formParams.add(new BasicNameValuePair(entry.getKey(), entry
                            .getValue()));
                }
                UrlEncodedFormEntity entity = new UrlEncodedFormEntity(
                        formParams, Consts.UTF_8);
                post.setEntity(entity);
            }
            if (headers != null && !headers.isEmpty()) {
                for (Entry<String, String> entry : headers.entrySet()) {
                    post.addHeader(entry.getKey(), entry.getValue());
                }
            }
            // 设置参数
            Builder customReqConf = RequestConfig.custom();
            if (connTimeout != null) {
                customReqConf.setConnectTimeout(connTimeout);
            }
            if (readTimeout != null) {
                customReqConf.setSocketTimeout(readTimeout);
            }
            post.setConfig(customReqConf.build());
            HttpResponse res = null;
            if (url.startsWith("https")) {
                // 执行 Https 请求.
                client = createSSLInsecureClient();
                res = client.execute(post);
            } else {
                // 执行 Http 请求.
                client = HttpClientUtils.client;
                res = client.execute(post);
            }
            return IOUtils.toString(res.getEntity().getContent(), "UTF-8");
        } finally {
            post.releaseConnection();
            if (url.startsWith("https") && client != null
                    && client instanceof CloseableHttpClient) {
                ((CloseableHttpClient) client).close();
            }
        }
    }

    /**
     * 发送一个 GET 请求
     *
     * @param url
     * @param charset
     * @return
     * @throws Exception
     */
    public static String get(String url, String charset) throws Exception {
        return get(url, charset, null, null);
    }

    /**
     * 发送一个 GET 请求
     *
     * @param url
     * @param charset
     * @param connTimeout
     *            建立链接超时时间,毫秒.
     * @param readTimeout
     *            响应超时时间,毫秒.
     * @return
     * @throws ConnectTimeoutException
     *             建立链接超时
     * @throws SocketTimeoutException
     *             响应超时
     * @throws Exception
     */
    public static String get(String url, String charset, Integer connTimeout,
                             Integer readTimeout) throws ConnectTimeoutException, SocketTimeoutException, Exception {
        HttpClient client = null;

        HttpGet get = new HttpGet(url);
        String result = "";
        try {
            // 设置参数
            Builder customReqConf = RequestConfig.custom();
            if (connTimeout != null) {
                customReqConf.setConnectTimeout(connTimeout);
            }
            if (readTimeout != null) {
                customReqConf.setSocketTimeout(readTimeout);
            }
            get.setConfig(customReqConf.build());

            HttpResponse res = null;

            if (url.startsWith("https")) {
                // 执行 Https 请求.
                client = createSSLInsecureClient();
                res = client.execute(get);
            } else {
                // 执行 Http 请求.
                client = HttpClientUtils.client;
                res = client.execute(get);
            }

            result = IOUtils.toString(res.getEntity().getContent(), charset);
        } catch (Exception e) {
            logger.error("HttpClientUtils Get error[Exception]! url:" + url + ", " + ExceptionDump.getErrorInfoFromException(e));
            result = null;
            throw e;
        } finally {
            get.releaseConnection();
            if (url.startsWith("https") && client != null
                    && client instanceof CloseableHttpClient) {
                ((CloseableHttpClient) client).close();
            }
        }
        return result;
    }

    /**
     * 从 response 里获取 charset
     *
     * @param ressponse
     * @return
     */
    @SuppressWarnings("unused")
    private static String getCharsetFromResponse(HttpResponse ressponse) {
        // Content-Type:text/html; charset=GBK
        if (ressponse.getEntity() != null
                && ressponse.getEntity().getContentType() != null
                && ressponse.getEntity().getContentType().getValue() != null) {
            String contentType = ressponse.getEntity().getContentType()
                    .getValue();
            if (contentType.contains("charset=")) {
                return contentType
                        .substring(contentType.indexOf("charset=") + 8);
            }
        }
        return null;
    }

    private static CloseableHttpClient createSSLInsecureClient()
            throws GeneralSecurityException {
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(
                    null, new TrustStrategy() {
                        public boolean isTrusted(X509Certificate[] chain,
                                                 String authType) throws CertificateException {
                            return true;
                        }
                    }).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                    sslContext, new X509HostnameVerifier() {

                @Override
                public boolean verify(String arg0, SSLSession arg1) {
                    return true;
                }

                @Override
                public void verify(String host, SSLSocket ssl)
                        throws IOException {
                }

                @Override
                public void verify(String host, X509Certificate cert)
                        throws SSLException {
                }

                @Override
                public void verify(String host, String[] cns,
                                   String[] subjectAlts) throws SSLException {
                }

            });
            return HttpClients.custom().setSSLSocketFactory(sslsf).build();
        } catch (GeneralSecurityException e) {
            throw e;
        }
    }

    public static void main(String[] args) {
//        String result = null;
//        try {
//            Gson gson = new Gson();
//            Test test = new Test(150, 3333L, "123123", "3333", "fffff", 8888, 9999);
////            String url = "http://114.112.43.83:8102/im_data_interface/location/recordLocation.htm?micUpQueryJson=" + URLEncoder.encode(gson.toJson(test), "utf8");
////            System.out.println("URL:" + url);
////            result = HttpClientUtils.get(url, "utf-8");
//            //result = HttpClientUtils.post("https://live.51vv.com/user/push/pushLiveMsg.htm", "{\"dstuid\":\"900001956272\"}", "text", "utf-8", 15, 10);
//            result = HttpClientUtils.post("http://114.112.43.83:8102/im_data_interface/location/recordLocation.htm", /*"&micUpQueryJson=" + */gson.toJson(test), "application/x-www-form-urlencoded", "utf-8", 15, 10);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        System.out.println(result);
//        logger.debug(result);

    }

}
