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

    private ManagementClient managementClient;

    @Before
    public void setup() {
        URI namespaceEndpointURI = TestUtils.getNamespaceEndpointURI();
        ClientSettings managementClientSettings = TestUtils.getManagementClientSettings();
        managementClient = new ManagementClient(namespaceEndpointURI, managementClientSettings);
    }

    @Test
    public void basicQueueCrudTest() throws InterruptedException, ExecutionException {
        String queueName = UUID.randomUUID().toString().substring(0, 8);
        QueueDescription q = new QueueDescription(queueName);
        q.setAutoDeleteOnIdle(Duration.ofHours(1));
        q.setDefaultMessageTimeToLive(Duration.ofDays(2));
        q.setDuplicationDetectionHistoryTimeWindow(Duration.ofMinutes(1));
        q.setEnableBatchedOperations(true);
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

        QueueDescription qCreated = this.managementClient.createQueueAsync(q).get();
        Assert.assertEquals(q, qCreated);

        QueueDescription queue = this.managementClient.getQueue(queueName);
        Assert.assertEquals(qCreated, queue);

        queue.setEnableBatchedOperations(false);
        queue.setMaxDeliveryCount(9);
        queue.getAuthorizationRules().clear();
        rights = new ArrayList<>();
        rights.add(AccessRights.Send);
        rights.add(AccessRights.Listen);
        queue.getAuthorizationRules().add(new SharedAccessAuthorizationRule("noManage", rights));

        QueueDescription updatedQ = this.managementClient.updateQueueAsync(queue).get();
        Assert.assertEquals(queue, updatedQ);

        Boolean exists = this.managementClient.queueExistsAsync(queueName).get();
        Assert.assertTrue(exists);

        List<QueueDescription> queues = this.managementClient.getQueuesAsync().get();
        Assert.assertTrue(queues.size() > 0);
        AtomicBoolean found = new AtomicBoolean(false);
        queues.forEach(queueDescription -> {
            if (queueDescription.getPath().equalsIgnoreCase(queueName)) {
                found.set(true);
            }
        });
        Assert.assertTrue(found.get());

        this.managementClient.deleteQueueAsync(queueName).get();

        exists = this.managementClient.queueExistsAsync(queueName).get();
        Assert.assertFalse(exists);
    }
}
