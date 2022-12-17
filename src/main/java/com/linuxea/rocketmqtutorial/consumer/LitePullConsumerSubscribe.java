package com.linuxea.rocketmqtutorial.consumer;

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;

public class LitePullConsumerSubscribe {

  public static volatile boolean running = true;

  public static void main(String[] args) throws Exception {

    DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(
        "lite_pull_consumer_test");
    litePullConsumer.subscribe("TopicTest", "*");
    litePullConsumer.setPullBatchSize(20);

    // 设置NameServer地址
    String mqNameSvr = System.getenv("mqnamesvr");
    litePullConsumer.setNamesrvAddr(mqNameSvr + ":9876");

    litePullConsumer.start();
    try {
      while (running) {
        List<MessageExt> messageExts = litePullConsumer.poll();
        System.out.printf("%s%n", messageExts);
      }
    } finally {
      litePullConsumer.shutdown();
    }
  }
}