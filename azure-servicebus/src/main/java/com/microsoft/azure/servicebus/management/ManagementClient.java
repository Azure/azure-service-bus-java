package com.microsoft.azure.servicebus.management;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.HttpsURLConnection;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import com.microsoft.azure.servicebus.ClientSettings;
import com.microsoft.azure.servicebus.primitives.*;
import com.microsoft.azure.servicebus.security.SecurityToken;
import com.microsoft.azure.servicebus.security.TokenProvider;
import org.asynchttpclient.*;
import org.asynchttpclient.util.HttpConstants;

import static org.asynchttpclient.Dsl.asyncHttpClient;

public class ManagementClient {
    private static final int ONE_BOX_HTTPS_PORT = 4446;
    private static final String API_VERSION_QUERY = "api-version=2017-04";
    private static final String PUT_METHOD = "PUT";
    private static final String DELETE_METHOD = "DELETE";
    private static final String USER_AGENT_HEADER_NAME = "User-Agent";
    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    private static final String CONTENT_TYPE = "application/atom+xml";
    private static final Duration CONNECTION_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration READ_TIMEOUT = Duration.ofMinutes(2);
    private static final String USER_AGENT = String.format("%s/%s(%s)", ClientConstants.PRODUCT_NAME, ClientConstants.CURRENT_JAVACLIENT_VERSION, ClientConstants.PLATFORM_INFO);

    private ClientSettings clientSettings;
    private URI namespaceEndpointURI;
    private AsyncHttpClient asyncHttpClient;

    public ManagementClient(URI namespaceEndpointURI, ClientSettings clientSettings)
    {
        this.namespaceEndpointURI = namespaceEndpointURI;
        this.clientSettings = clientSettings;
        DefaultAsyncHttpClientConfig.Builder clientBuilder = Dsl.config()
                .setConnectTimeout((int)this.clientSettings.getOperationTimeout().toMillis())
                .setRequestTimeout((int)this.clientSettings.getOperationTimeout().toMillis());
        this.asyncHttpClient = asyncHttpClient(clientBuilder);
    }

    public QueueDescription getQueue(String path) throws ExecutionException, InterruptedException {
        return this.getQueueAsync(path).get();
    }

    public CompletableFuture<QueueDescription> getQueueAsync(String path) {
        CompletableFuture<String> contentFuture = getEntityAsync(path, null, false);
        CompletableFuture<QueueDescription> qdFuture = new CompletableFuture<>();
        contentFuture.handleAsync((content, ex) -> {
            if (ex != null) {
                qdFuture.completeExceptionally(ex);
            } else {
                try {
                    qdFuture.complete(QueueDescriptionUtil.parseFromContent(content));
                } catch (MessagingEntityNotFoundException e) {
                    qdFuture.completeExceptionally(e);
                }
            }
            return null;
        });

        return qdFuture;
    }

    private CompletableFuture<String> getEntityAsync(String path, String query, boolean enrich) {
        String queryString = API_VERSION_QUERY + "&enrich=" + enrich;
        if (query != null) {
            queryString = queryString + "&" + query;
        }

        URL entityURL = null;
        try {
            entityURL = getManagementURL(this.namespaceEndpointURI, path, queryString);
        } catch (ServiceBusException e) {
            final CompletableFuture<String> exceptionFuture = new CompletableFuture<>();
            exceptionFuture.completeExceptionally(e);
            return exceptionFuture;
        }

        return sendManagementHttpRequestAsync(HttpConstants.Methods.GET, entityURL, null, null);
    }

    public CompletableFuture<QueueDescription> createQueueAsync(QueueDescription queueDescription) {
        if (queueDescription == null) {
            throw new IllegalArgumentException("queueDescription passed cannot be null");
        }

        QueueDescriptionUtil.normalizeDescription(queueDescription, this.namespaceEndpointURI);
        String atomRequest = null;
        try {
            atomRequest = QueueDescriptionUtil.serialize(queueDescription);
        } catch (ServiceBusException e) {
            final CompletableFuture<QueueDescription> exceptionFuture = new CompletableFuture<>();
            exceptionFuture.completeExceptionally(e);
            return exceptionFuture;
        }

        CompletableFuture<QueueDescription> responseFuture = new CompletableFuture<>();
        putEntityAsync(queueDescription.path, atomRequest, false, queueDescription.getForwardTo(), queueDescription.getForwardDeadLetteredMessagesTo())
            .handleAsync((content, ex) -> {
                if (ex != null) {
                    responseFuture.completeExceptionally(ex);
                } else {
                    try {
                        responseFuture.complete(QueueDescriptionUtil.parseFromContent(content));
                    } catch (MessagingEntityNotFoundException e) {
                        responseFuture.completeExceptionally(e);
                    }
                }

                return null;
        });

        return responseFuture;
    }

    private CompletableFuture<String> putEntityAsync(String path, String requestBody, boolean isUpdate, String forwardTo, String fwdDeadLetterTo) {
        URL entityURL = null;
        try {
            entityURL = getManagementURL(this.namespaceEndpointURI, path, API_VERSION_QUERY);
        } catch (ServiceBusException e) {
            final CompletableFuture<String> exceptionFuture = new CompletableFuture<>();
            exceptionFuture.completeExceptionally(e);
            return exceptionFuture;
        }

        HashMap<String, String> additionalHeaders = new HashMap<>();
        if (isUpdate) {
            additionalHeaders.put("If-Match", "*");
        }

        if (forwardTo != null && !forwardTo.isEmpty()) {
            try {
                String securityToken = getSecurityToken(this.clientSettings.getTokenProvider(), new URL(forwardTo));
                additionalHeaders.put(ManagementClientConstants.ServiceBusSupplementartyAuthorizationHeaderName, securityToken);
            } catch (InterruptedException | ExecutionException | MalformedURLException e) {
                final CompletableFuture<String> exceptionFuture = new CompletableFuture<>();
                exceptionFuture.completeExceptionally(e);
                return exceptionFuture;
            }
        }

        if (fwdDeadLetterTo != null && !fwdDeadLetterTo.isEmpty()) {
            try {
                String securityToken = getSecurityToken(this.clientSettings.getTokenProvider(), new URL(fwdDeadLetterTo));
                additionalHeaders.put(ManagementClientConstants.ServiceBusDlqSupplementaryAuthorizationHeaderName, securityToken);
            } catch (InterruptedException | ExecutionException | MalformedURLException e) {
                final CompletableFuture<String> exceptionFuture = new CompletableFuture<>();
                exceptionFuture.completeExceptionally(e);
                return exceptionFuture;
            }
        }

        return sendManagementHttpRequestAsync(HttpConstants.Methods.PUT, entityURL, requestBody, additionalHeaders);
    }

    public static void createEntity(URI namespaceEndpointURI, ClientSettings clientSettings, ResourceDescripton resourceDescription) throws ManagementException
    {
        try
        {
            URL entityURL = getManagementURL(namespaceEndpointURI, resourceDescription.getPath(), API_VERSION_QUERY);
            String securityToken = getSecurityToken(clientSettings.getTokenProvider(), entityURL);
            sendManagementHttpRequest(PUT_METHOD, entityURL, securityToken, resourceDescription.getAtomXml());
        }
        catch(ManagementException me)
        {
            throw me;
        }
        catch(Exception e)
        {
            throw new ManagementException("EntityCreation failed.", e);
        }
    }

    public static void deleteEntity(URI namespaceEndpointURI, ClientSettings clientSettings, String entityPath) throws ManagementException
    {
        try
        {
            URL entityURL = getManagementURL(namespaceEndpointURI, entityPath, API_VERSION_QUERY);
            String securityToken = getSecurityToken(clientSettings.getTokenProvider(), entityURL);
            sendManagementHttpRequest(DELETE_METHOD, entityURL, securityToken, null);
        }
        catch(ManagementException me)
        {
            throw me;
        }
        catch(Exception e)
        {
            throw new ManagementException("Entity deletion failed.", e);
        }
    }
    
    private static URL getManagementURL(URI namespaceEndpontURI, String entityPath, String query) throws ServiceBusException
    {
        try {
            URI httpURI = new URI("https", null, namespaceEndpontURI.getHost(), getPortNumberFromHost(namespaceEndpontURI.getHost()), "/"+entityPath, query, null);
            return httpURI.toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            throw new ServiceBusException(false, e);
        }
    }

    private CompletableFuture<String> sendManagementHttpRequestAsync(String httpMethod, URL url, String atomEntryString, HashMap<String, String> additionalHeaders) {
        String securityToken = null;
        try {
            securityToken = getSecurityToken(this.clientSettings.getTokenProvider(), url);
        } catch (InterruptedException | ExecutionException e) {
            final CompletableFuture<String> exceptionFuture = new CompletableFuture<>();
            exceptionFuture.completeExceptionally(e);
            return exceptionFuture;
        }

        RequestBuilder requestBuilder = new RequestBuilder(httpMethod)
                .setUrl(url.toString())
                .setBody(atomEntryString)
                .addHeader(USER_AGENT_HEADER_NAME, USER_AGENT)
                .addHeader(AUTHORIZATION_HEADER_NAME, securityToken)
                .addHeader(CONTENT_TYPE_HEADER_NAME, CONTENT_TYPE);

        if (additionalHeaders != null) {
            for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) {
                requestBuilder.addHeader(entry.getKey(), entry.getValue());
            }
        }

        Request unboundRequest = requestBuilder.build();

        ListenableFuture<Response> listenableFuture = this.asyncHttpClient
                .executeRequest(unboundRequest);

        CompletableFuture<String> outputFuture = new CompletableFuture<>();
        listenableFuture.toCompletableFuture()
                .handleAsync((response, ex) ->
                {
                    if (ex != null) {
                        outputFuture.completeExceptionally(ex);
                    } else {
                        try {
                            validateHttpResponse(unboundRequest, response);
                            outputFuture.complete(response.getResponseBody());
                        } catch (ServiceBusException e) {
                            outputFuture.completeExceptionally(e);
                        }
                    }
                    return null;
                });

        return outputFuture;
    }

    private static void validateHttpResponse(Request request, Response response) throws ServiceBusException, UnsupportedOperationException {
        if (response.hasResponseStatus() && response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
            return;
        }

        String exceptionMessage = response.getResponseBody();
        exceptionMessage = parseDetailIfAvailable(exceptionMessage);
        if (exceptionMessage == null) {
            exceptionMessage = response.getStatusText();
        }

        ServiceBusException exception = null;
        switch (response.getStatusCode())
        {
            case 401:   /*UnAuthorized*/
                exception = new AuthorizationFailedException(exceptionMessage);
                break;

            case 404: /*NotFound*/
            case 204: /*NoContent*/
                exception = new MessagingEntityNotFoundException(exceptionMessage);
                break;

            case 409: /*Conflict*/
                if (request.getMethod() == HttpConstants.Methods.DELETE) {
                    exception = new ServiceBusException(true, exceptionMessage);
                    break;
                }

                if (request.getMethod() == HttpConstants.Methods.PUT && request.getHeaders().contains("IfMatch")) {
                    /*Update request*/
                    exception = new ServiceBusException(true, exceptionMessage);
                    break;
                }

                if (exceptionMessage.contains(ManagementClientConstants.ConflictOperationInProgressSubCode)) {
                    exception = new ServiceBusException(true, exceptionMessage);
                    break;
                }

                exception = new MessagingEntityAlreadyExistsException(exceptionMessage);
                break;

            case 403: /*Forbidden*/
                if (exceptionMessage.contains(ManagementClientConstants.ForbiddenInvalidOperationSubCode)) {
                    //todo: log
                    throw new UnsupportedOperationException(exceptionMessage);
                }
                else {
                    exception = new QuotaExceededException(exceptionMessage);
                }
                break;

            case 400: /*BadRequest*/
                exception = new ServiceBusException(false, new IllegalArgumentException(exceptionMessage));
                break;

            case 503: /*ServiceUnavailable*/
                exception = new ServerBusyException(exceptionMessage);
                break;

            default:
                exception = new ServiceBusException(true, exceptionMessage = "; Status code: " + response.getStatusCode());
        }

        //todo: log
        throw exception;
    }

    private static String parseDetailIfAvailable(String content) {
        // todo
        return null;
    }

    private static String sendManagementHttpRequest(String httpMethod, URL url, String sasToken, String atomEntryString) throws IOException, ManagementException, NoSuchAlgorithmException, KeyManagementException, KeyStoreException
    {
        // No special handling of TLS here. Assuming the server cert is already trusted
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
        if(responseCode == HttpsURLConnection.HTTP_CREATED || responseCode == HttpsURLConnection.HTTP_ACCEPTED || responseCode == HttpsURLConnection.HTTP_OK)
        {
            try(BufferedInputStream bis = new BufferedInputStream(httpConnection.getInputStream()))
            {
                    StringBuffer response = new StringBuffer();
                    byte[] readBytes = new byte[1024];
                    int numRead = bis.read(readBytes);
                    while(numRead != -1)
                    {
                        response.append(new String(readBytes, 0, numRead));
                        numRead = bis.read(readBytes);
                    }

                    return response.toString();
            }
        }
        else
        {
            throw new ManagementException("Management operation failed with response code:" + responseCode);
        }
    }
    
    private static String getSecurityToken(TokenProvider tokenProvider, URL url ) throws InterruptedException, ExecutionException
    {
        SecurityToken token = tokenProvider.getSecurityTokenAsync(url.toString()).get();
        return token.getTokenValue();
    }
    
    private static int getPortNumberFromHost(String host)
    {
        if(host.endsWith("onebox.windows-int.net"))
        {
            return ONE_BOX_HTTPS_PORT;
        }
        else
        {
            return -1;
        }
    }
}
