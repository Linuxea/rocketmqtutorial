package com.linuxea.rocketmqtutorial.producer.common;

import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class AsyncProducer {

    public static void main(String[] args) throws Exception {

        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer("localhostSyncProducer");
        // 设置NameServer地址
        String mqNameSvr = System.getenv("mqnamesvr");
        producer.setNamesrvAddr(mqNameSvr + ":9876");  //（2）
        // 启动producer
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for (int i = 0; i < 100; i++) {
            final int index = i;
            // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
            Message msg = new Message("TopicTest",
                    "TagA",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 异步发送消息, 发送结果通过callback返回给客户端
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }


        // 一旦producer不再使用，关闭producer
//        producer.shutdown();

        TimeUnit.SECONDS.sleep(1000L);
    }
}