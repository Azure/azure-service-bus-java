package com.microsoft.azure.servicebus.management;

import com.microsoft.azure.servicebus.ClientSettings;
import com.microsoft.azure.servicebus.primitives.*;
import com.microsoft.azure.servicebus.security.SecurityToken;
import com.microsoft.azure.servicebus.security.TokenProvider;
import org.asynchttpclient.*;
import org.asynchttpclient.util.HttpConstants;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.trace;

public class ManagementClient {
    private static final int ONE_BOX_HTTPS_PORT = 4446;
    private static final String API_VERSION_QUERY = "api-version=2017-04";
    private static final String USER_AGENT_HEADER_NAME = "User-Agent";
    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    private static final String CONTENT_TYPE = "application/atom+xml";
    private static final Duration CONNECTION_TIMEOUT = Duration.ofMinutes(1);
    private static final String USER_AGENT = String.format("%s/%s(%s)", ClientConstants.PRODUCT_NAME, ClientConstants.CURRENT_JAVACLIENT_VERSION, ClientConstants.PLATFORM_INFO);

    private ClientSettings clientSettings;
    private URI namespaceEndpointURI;
    private AsyncHttpClient asyncHttpClient;

    public ManagementClient(URI namespaceEndpointURI, ClientSettings clientSettings) {
        this.namespaceEndpointURI = namespaceEndpointURI;
        this.clientSettings = clientSettings;
        DefaultAsyncHttpClientConfig.Builder clientBuilder = Dsl.config()
                .setConnectTimeout((int)CONNECTION_TIMEOUT.toMillis())
                .setRequestTimeout((int)this.clientSettings.getOperationTimeout().toMillis());
        this.asyncHttpClient = asyncHttpClient(clientBuilder);
    }

    public QueueDescription getQueue(String path) throws ExecutionException, InterruptedException {
        return this.getQueueAsync(path).get();
    }

    public CompletableFuture<QueueDescription> getQueueAsync(String path) {
        EntityNameHelper.checkValidQueueName(path);

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

    public CompletableFuture<List<QueueDescription>> getQueuesAsync() {
        return getQueuesAsync(100, 0);
    }

    public CompletableFuture<List<QueueDescription>> getQueuesAsync(int count, int skip) {
        if (count > 100 || count < 1) {
            throw new IllegalArgumentException("Count should be between 1 and 100");
        }

        if (skip < 0) {
            throw new IllegalArgumentException("Skip cannot be negative");
        }

        CompletableFuture<String> contentFuture = getEntityAsync("$Resources/queues", String.format("$skip=%d&$top=%d", skip, count), false);
        CompletableFuture<List<QueueDescription>> qdFuture = new CompletableFuture<>();
        contentFuture.handleAsync((content, ex) -> {
            if (ex != null) {
                qdFuture.completeExceptionally(ex);
            } else {
                qdFuture.complete(QueueDescriptionUtil.parseCollectionFromContent(content));
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

    public CompletableFuture<QueueDescription> createQueueAsync(String queuePath) {
        return this.createQueueAsync(new QueueDescription(queuePath));
    }

    public CompletableFuture<QueueDescription> createQueueAsync(QueueDescription queueDescription) {
        return putQueueAsync(queueDescription, false);
    }

    public CompletableFuture<QueueDescription> updateQueueAsync(QueueDescription queueDescription) {
        return putQueueAsync(queueDescription, true);
    }

    private CompletableFuture<QueueDescription> putQueueAsync(QueueDescription queueDescription, boolean isUpdate) {
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
        putEntityAsync(queueDescription.path, atomRequest, isUpdate, queueDescription.getForwardTo(), queueDescription.getForwardDeadLetteredMessagesTo())
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

    public CompletableFuture<Boolean> queueExistsAsync(String path) {
        EntityNameHelper.checkValidQueueName(path);

        CompletableFuture<Boolean> existsFuture = new CompletableFuture<>();
        this.getQueueAsync(path).handleAsync((qd, ex) -> {
            if (ex != null) {
                if (ex instanceof MessagingEntityNotFoundException) {
                    existsFuture.complete(Boolean.FALSE);
                    return false;
                }

                existsFuture.completeExceptionally(ex);
                return false;
            }

            existsFuture.complete(Boolean.TRUE);
            return true;
        });

        return existsFuture;
    }

    public CompletableFuture<Void> deleteQueueAsync(String path) {
        EntityNameHelper.checkValidQueueName(path);
        return deleteEntityAsync(path);
    }

    private CompletableFuture<Void> deleteEntityAsync(String path) {
        URL entityURL = null;
        try {
            entityURL = getManagementURL(this.namespaceEndpointURI, path, API_VERSION_QUERY);
        } catch (ServiceBusException e) {
            final CompletableFuture<Void> exceptionFuture = new CompletableFuture<>();
            exceptionFuture.completeExceptionally(e);
            return exceptionFuture;
        }

        return sendManagementHttpRequestAsync(HttpConstants.Methods.DELETE, entityURL, null, null).thenAccept(c -> {});
    }

    private static URL getManagementURL(URI namespaceEndpontURI, String entityPath, String query) throws ServiceBusException {
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
                exception = new ServiceBusException(true, exceptionMessage + "; Status code: " + response.getStatusCode());
        }

        //todo: log
        throw exception;
    }

    private static String parseDetailIfAvailable(String content) {
        if (content == null || content.isEmpty()) {
            return null;
        }

        // todo: reuse
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(new ByteArrayInputStream(content.getBytes("utf-8")));
            Element doc = dom.getDocumentElement();
            doc.normalize();
            NodeList entries = doc.getChildNodes();
            for (int i = 0; i < entries.getLength(); i++) {
                Node node = entries.item(i);
                if (node.getNodeName().equals("Detail")) {
                    return node.getFirstChild().getTextContent();
                }
            }
        }
        catch (Exception ex) {
            // TODO: Log
        }

        return null;
    }

    private static String getSecurityToken(TokenProvider tokenProvider, URL url ) throws InterruptedException, ExecutionException {
        SecurityToken token = tokenProvider.getSecurityTokenAsync(url.toString()).get();
        return token.getTokenValue();
    }
    
    private static int getPortNumberFromHost(String host) {
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
