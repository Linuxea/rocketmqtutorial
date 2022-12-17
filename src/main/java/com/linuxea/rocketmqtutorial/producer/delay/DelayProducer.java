package com.linuxea.rocketmqtutorial.producer.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class DelayProducer {

  public static void main(String[] args) throws Exception {

    // Instantiate a producer to send scheduled messages
    DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
    // 设置NameServer地址
    String mqNameSvr = System.getenv("mqnamesvr");
    producer.setNamesrvAddr(mqNameSvr + ":9876");  //（2）
    producer.setSendMsgTimeout(Integer.MAX_VALUE);
    // Launch producer
    producer.start();
    int totalMessagesToSend = 100;
    for (int i = 0; i < totalMessagesToSend; i++) {
      Message message = new Message("TestTopic", ("Hello scheduled message " + i).getBytes());
      // This message will be delivered to consumer 10 seconds later.
      message.setDelayTimeLevel(3);
      // Send the message
      producer.send(message);
    }

    // Shutdown producer after use.
    producer.shutdown();
  }

}
