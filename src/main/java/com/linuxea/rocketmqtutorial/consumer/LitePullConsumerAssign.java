package com.linuxea.rocketmqtutorial.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class LitePullConsumerAssign {
  public static volatile boolean running = true;
  public static void main(String[] args) throws Exception {

    DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer("please_rename_unique_group_name");

    litePullConsumer.setAutoCommit(false);

    // 设置NameServer地址
    String mqNameSvr = System.getenv("mqnamesvr");
    litePullConsumer.setNamesrvAddr(mqNameSvr + ":9876");

    litePullConsumer.start();

    Collection<MessageQueue> mqSet = litePullConsumer.fetchMessageQueues("TopicTest");
    List<MessageQueue> list = new ArrayList<>(mqSet);
    List<MessageQueue> assignList = new ArrayList<>();
    for (int i = 0; i < list.size() / 2; i++) {
      assignList.add(list.get(i));
    }
    litePullConsumer.assign(assignList);
    litePullConsumer.seek(assignList.get(0), 10);
    try {
      while (running) {
        List<MessageExt> messageExts = litePullConsumer.poll();
        System.out.printf("%s %n", messageExts);
        litePullConsumer.commitSync();
      }
    } finally {
      litePullConsumer.shutdown();
    }
  }
}
