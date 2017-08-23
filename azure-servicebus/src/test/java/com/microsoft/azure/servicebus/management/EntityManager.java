package com.microsoft.azure.servicebus.management;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.UUID;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.microsoft.azure.servicebus.primitives.ClientConstants;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.SASUtil;

public class EntityManager {
    private static final int ONE_BOX_HTTPS_PORT = 4446;
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String USER_AGENT_HEADER_NAME = "User-Agent";
    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    private static final String CONTENT_TYPE = "application/atom+xml";
    private static final Duration CONNECTION_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration READ_TIMEOUT = Duration.ofMinutes(2);
    private static final int SAS_TOKEN_VALIDITY_IN_MINUTES = 5;
    private static final String USER_AGENT = String.format("%s/%s(%s)", ClientConstants.PRODUCT_NAME, ClientConstants.CURRENT_JAVACLIENT_VERSION, ClientConstants.PLATFORM_INFO);
    
    public static void createQueue(ConnectionStringBuilder namespaceConnectionStringBuilder, QueueDescription queueDescription) throws URISyntaxException, InvalidKeyException, IOException, ManagementException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException
    {
        URL queueURL = getManagementURL(namespaceConnectionStringBuilder, queueDescription.getPath());
        String sasToken = getSASToken(namespaceConnectionStringBuilder, queueURL);
        sendManagementHttpRequest(PUT_METHOD, queueURL, sasToken, queueDescription.getAtomXml(queueURL.toString()));
    }
    
    public static void deleteQueue(ConnectionStringBuilder namespaceConnectionStringBuilder, String queuePath) throws URISyntaxException, InvalidKeyException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException, IOException, ManagementException
    {
        URL queueURL = getManagementURL(namespaceConnectionStringBuilder, queuePath);
        String sasToken = getSASToken(namespaceConnectionStringBuilder, queueURL);
        sendManagementHttpRequest(DELETE_METHOD, queueURL, sasToken, null);
    }
    
    private static URL getManagementURL(ConnectionStringBuilder namespaceConnectionStringBuilder, String entityPath) throws URISyntaxException, MalformedURLException
    {
        URI endPointURI = namespaceConnectionStringBuilder.getEndpoint();
        URI httpURI = new URI("https", null, endPointURI.getHost(), getPortNumberFromHost(endPointURI.getHost()), "/"+entityPath, null, null);
        return httpURI.toURL();
    }
    
    private static void sendManagementHttpRequest(String httpMethod, URL url, String sasToken, String atomEntryString) throws IOException, ManagementException, NoSuchAlgorithmException, KeyManagementException, KeyStoreException
    {

        HttpsURLConnection httpConnection = (HttpsURLConnection)url.openConnection();

        httpConnection.setConnectTimeout((int)CONNECTION_TIMEOUT.toMillis());
        httpConnection.setReadTimeout((int)READ_TIMEOUT.toMillis());
        httpConnection.setDoOutput(true);
        httpConnection.setRequestMethod(httpMethod);
        httpConnection.setRequestProperty(USER_AGENT_HEADER_NAME, USER_AGENT);
        httpConnection.setRequestProperty(AUTHORIZATION_HEADER_NAME, sasToken);
        httpConnection.setRequestProperty(CONTENT_TYPE_HEADER_NAME, CONTENT_TYPE);
        if(atomEntryString != null && !atomEntryString.isEmpty())
        {
            try(BufferedOutputStream bos = new BufferedOutputStream(httpConnection.getOutputStream()))
            {
                bos.write(atomEntryString.getBytes(StandardCharsets.UTF_8));
                bos.flush();
            }
        }
        
        int responseCode = httpConnection.getResponseCode();
        System.out.println("Reponse code : " + responseCode);
        if(responseCode == HttpsURLConnection.HTTP_CREATED || responseCode == HttpsURLConnection.HTTP_ACCEPTED || responseCode == HttpsURLConnection.HTTP_OK)
        {
            try(BufferedInputStream bis = new BufferedInputStream(httpConnection.getInputStream()))
            {
                    byte[] readBytes = new byte[1024];
                    int numRead = bis.read(readBytes);
                    if(numRead != -1)
                    {
                        System.out.println("response is :" + new String(readBytes, 0, numRead));
                    }
            }
        }
        else
        {
            throw new ManagementException("Entity creation failed with response code:" + responseCode);
        }
    }
    
    public static void main(String a[]) throws Exception
    {
        System.out.println("Testing management part - Begin");
        ConnectionStringBuilder builder = new ConnectionStringBuilder("Endpoint=sb://samples.servicebus.onebox.windows-int.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=0r0uEdjf5joNo5zCfiDs0SuyvSdgSqNxSf4lLvQC76Q=");
        //ConnectionStringBuilder builder = new ConnectionStringBuilder("Endpoint=sb://vijayjavaclienttests.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DYnTjWvyHU4O5orFlYI8Sd/Frls/kdNsN5w6HjxFrtE=");
        
        
        QueueDescription qd = new QueueDescription("vijaymgmttestqueue7");
        qd.setMaxSizeInMegaBytes(1024);
        qd.setLockDuration(Duration.ofSeconds(30));
        //deleteQueue(builder, qd.getPath());
        createQueue(builder, qd);
        //deleteQueue(builder, "test2");
        System.out.println("Testing management part - End");
    }
    
    private static String getSASToken(ConnectionStringBuilder namespaceConnectionStringBuilder, URL url ) throws InvalidKeyException
    {
        String sasToken = namespaceConnectionStringBuilder.getSharedAccessSignatureToken();
        if(sasToken == null)
        {
            sasToken = SASUtil.generateSharedAccessSignatureToken(namespaceConnectionStringBuilder.getSasKeyName(), namespaceConnectionStringBuilder.getSasKey(), url.toString(), SAS_TOKEN_VALIDITY_IN_MINUTES * 60);
        }
        
        return sasToken;
    }
    
    private static int getPortNumberFromHost(String host)
    {
        if(host.endsWith("windows-int.net"))
        {
            return ONE_BOX_HTTPS_PORT;
        }
        else
        {
            return -1;
        }
    }
}
