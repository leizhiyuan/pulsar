/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ConsumeMode;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ConsumerPopTest extends ProducerConsumerBase {

    private static final TransactionImpl transaction = mock(TransactionImpl.class);

    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        doReturn(1L).when(transaction).getTxnIdLeastBits();
        doReturn(1L).when(transaction).getTxnIdMostBits();
        doReturn(TransactionImpl.State.OPEN).when(transaction).getState();
        CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);
        doNothing().when(transaction).registerAckOp(any());
        doReturn(true).when(transaction).checkIfOpen(any());
        doReturn(completableFuture).when(transaction).registerAckedTopic(any(), any());

        Thread.sleep(1000 * 3);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPop() throws PulsarClientException, InterruptedException {
        String topic = "testAckResponse";
        try {
            admin.topics().createPartitionedTopic(topic, 3);
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(false)
                .create();
        @Cleanup
        Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .consumeMode(ConsumeMode.Pop)
                .receiverQueueSize(0)
                .subscribe();

        for (int i =0;i<10;i++) {
            MessageId messageId = producer.send(1);
            System.out.println("send message" + messageId);
        }

        for (int i =0;i<10;i++) {
            Message<Integer> message = consumer.pop(5, TimeUnit.SECONDS);
            System.out.println("result" + message.getMessageId());
            String par = message.getMessageId().toString().split(":")[2];
            TopicMessageIdImpl topicMessageId =
                    new TopicMessageIdImpl("persistent://public/default/testAckResponse-partition-"+ par,
                            "persistent://public/default/testAckResponse", message.getMessageId());
            consumer.acknowledge(topicMessageId);
        }
        TimeUnit.SECONDS.sleep(10000);
    }
}
