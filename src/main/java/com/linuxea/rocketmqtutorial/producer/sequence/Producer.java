package com.linuxea.rocketmqtutorial.producer.sequence;


import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class Producer {

    public static void main(String[] args) throws UnsupportedEncodingException {
        try {

            DefaultMQProducer producer = new DefaultMQProducer("localhostSyncProducer");
            // 设置NameServer地址
            String mqNameSvr = System.getenv("mqnamesvr");
            producer.setNamesrvAddr(mqNameSvr + ":9876");  //（2）
            producer.setSendMsgTimeout(Integer.MAX_VALUE);

            producer.start();

            String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
            for (int i = 0; i < 100; i++) {
                int orderId = i % 10;
                Message msg =
                    new Message("TopicTest", tags[i % tags.length], "KEY" + i,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

                SendResult sendResult = producer.send(msg, (mqs, msg1, arg) -> {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }, orderId);

                System.out.printf("%s%n", sendResult);
            }

            producer.shutdown();
        } catch (MQClientException | RemotingException | MQBrokerException |
                 InterruptedException e) {
            e.printStackTrace();
        }
    }
}
