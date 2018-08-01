package com.microsoft.azure.servicebus;

import com.microsoft.azure.servicebus.management.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ManagementTests {

    private ManagementClientAsync managementClientAsync;

    @Before
    public void setup() {
        URI namespaceEndpointURI = TestUtils.getNamespaceEndpointURI();
        ClientSettings managementClientSettings = TestUtils.getManagementClientSettings();
        managementClientAsync = new ManagementClientAsync(namespaceEndpointURI, managementClientSettings);
    }

    @Test
    public void basicQueueCrudTest() throws InterruptedException, ExecutionException {
        String queueName = UUID.randomUUID().toString().substring(0, 8);
        QueueDescription q = new QueueDescription(queueName);
        q.setAutoDeleteOnIdle(Duration.ofHours(1));
        q.setDefaultMessageTimeToLive(Duration.ofDays(2));
        q.setDuplicationDetectionHistoryTimeWindow(Duration.ofMinutes(1));
        q.setEnableBatchedOperations(false);
        q.setEnableDeadLetteringOnMessageExpiration(true);
        q.setEnablePartitioning(false);
        q.setForwardTo(null);
        q.setForwardDeadLetteredMessagesTo(null);
        q.setLockDuration(Duration.ofSeconds(45));
        q.setMaxDeliveryCount(8);
        q.setMaxSizeInMB(2048);
        q.setRequiresDuplicateDetection(true);
        q.setRequiresSession(true);
        q.setUserMetadata("basicQueueCrudTest");

        ArrayList<AuthorizationRule> rules = new ArrayList<>();
        ArrayList<AccessRights> rights = new ArrayList<>();
        rights.add(AccessRights.Send);
        rights.add(AccessRights.Listen);
        rights.add(AccessRights.Manage);
        rules.add(new SharedAccessAuthorizationRule("allClaims", rights));
        q.setAuthorizationRules(rules);

        QueueDescription qCreated = this.managementClientAsync.createQueueAsync(q).get();
        Assert.assertEquals(q, qCreated);

        QueueDescription queue = this.managementClientAsync.getQueueAsync(queueName).get();
        Assert.assertEquals(qCreated, queue);

        queue.setEnableBatchedOperations(false);
        queue.setMaxDeliveryCount(9);
        queue.getAuthorizationRules().clear();
        rights = new ArrayList<>();
        rights.add(AccessRights.Send);
        rights.add(AccessRights.Listen);
        queue.getAuthorizationRules().add(new SharedAccessAuthorizationRule("noManage", rights));

        QueueDescription updatedQ = this.managementClientAsync.updateQueueAsync(queue).get();
        Assert.assertEquals(queue, updatedQ);

        Boolean exists = this.managementClientAsync.queueExistsAsync(queueName).get();
        Assert.assertTrue(exists);

        List<QueueDescription> queues = this.managementClientAsync.getQueuesAsync().get();
        Assert.assertTrue(queues.size() > 0);
        AtomicBoolean found = new AtomicBoolean(false);
        queues.forEach(queueDescription -> {
            if (queueDescription.getPath().equalsIgnoreCase(queueName)) {
                found.set(true);
            }
        });
        Assert.assertTrue(found.get());

        this.managementClientAsync.deleteQueueAsync(queueName).get();

        exists = this.managementClientAsync.queueExistsAsync(queueName).get();
        Assert.assertFalse(exists);
    }

    @Test
    public void basicTopicCrudTest() throws InterruptedException, ExecutionException {
        String topicName = UUID.randomUUID().toString().substring(0, 8);
        TopicDescription td = new TopicDescription(topicName);
        td.setAutoDeleteOnIdle(Duration.ofHours(1));
        td.setDefaultMessageTimeToLive(Duration.ofDays(2));
        td.setDuplicationDetectionHistoryTimeWindow(Duration.ofMinutes(1));
        td.setEnableBatchedOperations(false);
        td.setEnablePartitioning(false);
        td.setMaxSizeInMB(2048);
        td.setRequiresDuplicateDetection(true);
        td.setUserMetadata("basicTopicCrudTest");
        td.setSupportOrdering(true);

        ArrayList<AuthorizationRule> rules = new ArrayList<>();
        ArrayList<AccessRights> rights = new ArrayList<>();
        rights.add(AccessRights.Send);
        rights.add(AccessRights.Listen);
        rights.add(AccessRights.Manage);
        rules.add(new SharedAccessAuthorizationRule("allClaims", rights));
        td.setAuthorizationRules(rules);

        TopicDescription tCreated = this.managementClientAsync.createTopicAsync(td).get();
        Assert.assertEquals(td, tCreated);

        TopicDescription topic = this.managementClientAsync.getTopicAsync(topicName).get();
        Assert.assertEquals(tCreated, topic);

        topic.setEnableBatchedOperations(false);
        topic.setDefaultMessageTimeToLive(Duration.ofDays(3));
        topic.getAuthorizationRules().clear();
        rights = new ArrayList<>();
        rights.add(AccessRights.Send);
        rights.add(AccessRights.Listen);
        topic.getAuthorizationRules().add(new SharedAccessAuthorizationRule("noManage", rights));

        TopicDescription updatedT = this.managementClientAsync.updateTopicAsync(topic).get();
        Assert.assertEquals(topic, updatedT);

        Boolean exists = this.managementClientAsync.topicExistsAsync(topicName).get();
        Assert.assertTrue(exists);

        List<TopicDescription> topics = this.managementClientAsync.getTopicsAsync().get();
        Assert.assertTrue(topics.size() > 0);
        AtomicBoolean found = new AtomicBoolean(false);
        topics.forEach(topicDescription -> {
            if (topicDescription.getPath().equalsIgnoreCase(topicName)) {
                found.set(true);
            }
        });
        Assert.assertTrue(found.get());

        this.managementClientAsync.deleteTopicAsync(topicName).get();

        exists = this.managementClientAsync.topicExistsAsync(topicName).get();
        Assert.assertFalse(exists);
    }

    @Test
    public void basicSubscriptionCrudTest() throws InterruptedException, ExecutionException {
        String topicName = UUID.randomUUID().toString().substring(0, 8);
        this.managementClientAsync.createTopicAsync(topicName).get();

        String subscriptionName = UUID.randomUUID().toString().substring(0, 8);
        SubscriptionDescription subscriptionDescription = new SubscriptionDescription(topicName, subscriptionName);
        subscriptionDescription.setAutoDeleteOnIdle(Duration.ofHours(1));
        subscriptionDescription.setDefaultMessageTimeToLive(Duration.ofDays(2));
        subscriptionDescription.setEnableBatchedOperations(false);
        subscriptionDescription.setEnableDeadLetteringOnMessageExpiration(true);
        subscriptionDescription.setEnableDeadLetteringOnFilterEvaluationException(false);
        subscriptionDescription.setForwardTo(null);
        subscriptionDescription.setForwardDeadLetteredMessagesTo(null);
        subscriptionDescription.setLockDuration(Duration.ofSeconds(45));
        subscriptionDescription.setMaxDeliveryCount(8);
        subscriptionDescription.setRequiresSession(true);
        subscriptionDescription.setUserMetadata("basicSubscriptionCrudTest");

        SubscriptionDescription createdS = this.managementClientAsync.createSubscriptionAsync(subscriptionDescription).get();
        Assert.assertEquals(subscriptionDescription, createdS);

        SubscriptionDescription getS = this.managementClientAsync.getSubscriptionAsync(topicName, subscriptionName).get();
        Assert.assertEquals(createdS, getS);

        getS.setEnableBatchedOperations(false);
        getS.setMaxDeliveryCount(9);

        SubscriptionDescription updatedQ = this.managementClientAsync.updateSubscriptionAsync(getS).get();
        Assert.assertEquals(getS, updatedQ);

        Boolean exists = this.managementClientAsync.subscriptionExistsAsync(topicName, subscriptionName).get();
        Assert.assertTrue(exists);

        List<SubscriptionDescription> subscriptions = this.managementClientAsync.getSubscriptionsAsync(topicName).get();
        Assert.assertEquals(1, subscriptions.size());
        Assert.assertEquals(subscriptionName, subscriptions.get(0).getSubscriptionName());

        this.managementClientAsync.deleteSubscriptionAsync(topicName, subscriptionName).get();

        exists = this.managementClientAsync.subscriptionExistsAsync(topicName, subscriptionName).get();
        Assert.assertFalse(exists);

        this.managementClientAsync.deleteTopicAsync(topicName);

        exists = this.managementClientAsync.subscriptionExistsAsync(topicName, subscriptionName).get();
        Assert.assertFalse(exists);
    }
}
