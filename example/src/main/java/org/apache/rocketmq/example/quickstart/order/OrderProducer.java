/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart.order;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import javax.xml.transform.Result;
import java.util.List;

/**
 * 有序生产
 */
public class OrderProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.  rcoketmq有组的概念
         */
        DefaultMQProducer producer = new DefaultMQProducer("order_producer");

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        producer.setNamesrvAddr("127.0.0.1:9876");
        /*
         * Launch the instance.
         */
        producer.start();

            try {


                /*
                 * Call send message to deliver message to specilate  brokers.
                 */

                for (int j = 0; j < 5; j++) {
                    /*
                     * Create a message instance, specifying topic, tag and message body.
                     */
                    Message msg = new Message("OrderTopicTest" /* Topic */,
                            "TagA" /* Tag */,
                            ("Hello RocketMQ " +j).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                    );

                    // 指定队列 生产消息
                    SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                                @Override
                                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                                    Integer index = (Integer)arg;
                                  return  mqs.get(index);
                                }
                            },
                            1);
                    System.out.println(sendResult);
                    System.out.printf("%s%n", sendResult);
                }
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
