package com.microsoft.azure.servicebus;

import com.microsoft.azure.servicebus.management.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ManagementTests {

    ManagementClient managementClient;

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
        rights.add(AccessRights.SEND);
        rights.add(AccessRights.LISTEN);
        rights.add(AccessRights.MANAGE);
        rules.add(new SharedAccessAuthorizationRule("allClaims", rights));

        QueueDescription qCreated = this.managementClient.createQueueAsync(q).get();
        Assert.assertEquals(qCreated, q);

        QueueDescription queue = this.managementClient.getQueue(queueName);
        Assert.assertEquals(qCreated, queue);

        queue.setEnableBatchedOperations(false);
        queue.setMaxDeliveryCount(9);
        queue.getAuthorizationRules().clear();
        rights = new ArrayList<>();
        rights.add(AccessRights.SEND);
        rights.add(AccessRights.LISTEN);
        queue.getAuthorizationRules().add(new SharedAccessAuthorizationRule("noManage", rights));

        QueueDescription updatedQ = this.managementClient.updateQueue(queue);
    }
}
