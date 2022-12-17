package com.linuxea.rocketmqtutorial.consumer;


import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;

public class PullConsumer {

  public static void main(String[] args) throws MQClientException {

    DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");

    // 设置NameServer地址
    String mqNameSvr = System.getenv("mqnamesvr");
    consumer.setNamesrvAddr(mqNameSvr + ":9876");
    consumer.start();

    try {
      MessageQueue mq = new MessageQueue();
      mq.setQueueId(0);
      mq.setTopic("TopicTest");
      mq.setBrokerName("broker-b");
      long offset = 26;
      PullResult pullResult = consumer.pull(mq, "*", offset, 32);
      if (pullResult.getPullStatus().equals(PullStatus.FOUND)) {
        System.out.printf("%s%n", pullResult.getMsgFoundList());
        consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    consumer.shutdown();
  }
}
