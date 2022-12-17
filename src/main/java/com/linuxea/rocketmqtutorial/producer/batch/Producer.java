package com.linuxea.rocketmqtutorial.producer.batch;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class Producer {


    public static void main(String[] args) throws Exception {
      DefaultMQProducer producer = new DefaultMQProducer("BatchProducerGroupName");
      // 设置NameServer地址
      String mqNameSvr = System.getenv("mqnamesvr");
      producer.setNamesrvAddr(mqNameSvr + ":9876");  //（2）
      producer.start();

      //If you just send messages of no more than 1MiB at a time, it is easy to use batch
      //Messages of the same batch should have: same topic, same waitStoreMsgOK and no schedule support
      String topic = "BatchTest";
      List<Message> messages = new LinkedList<>();
      messages.add(new Message(topic, "Tag", "OrderID001", "Hello world 0".getBytes()));
      messages.add(new Message(topic, "Tag", "OrderID002", "Hello world 1".getBytes()));
      messages.add(new Message(topic, "Tag", "OrderID003", "Hello world 2".getBytes()));

      producer.send(messages);

      producer.shutdown();
    }
  }
