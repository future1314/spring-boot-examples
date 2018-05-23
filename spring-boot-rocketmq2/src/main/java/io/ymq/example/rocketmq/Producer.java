package io.ymq.example.rocketmq;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * 描述: 消息生产者
 *
 * @author yanpenglei
 * @create 2018-02-01 18:09
 **/
@Component
public class Producer {

    /**
     * 生产者的组名
     */
    @Value("${apache.rocketmq.producer.producerGroup}")
    private String producerGroup;

    /**
     * NameServer 地址
     */
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    @PostConstruct
    public void defaultMQProducer() {

        //生产者的组名
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

        //指定NameServer地址，多个地址以 ; 隔开
        producer.setNamesrvAddr(namesrvAddr);

        try {

            /**
             * Producer对象在使用之前必须要调用start初始化，初始化一次即可
             * 注意：切记不可以在每次发送消息时，都调用start方法
             */
            producer.start();

            // String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD",
            // "TagE" };

            for (int i = 1; i <= 5; i++) {

                Message msg = new Message("TopicOrderTest", "order_1", "KEY" + i, ("order_1 " + i).getBytes());

                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, 0);//

                System.err.println(sendResult);
            }
            for (int i = 1; i <= 5; i++) {

                Message msg = new Message("TopicOrderTest", "order_2", "KEY" + i, ("order_2 " + i).getBytes());

                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, 1);//

                System.err.println(sendResult);
            }
            for (int i = 1; i <= 5; i++) {

                Message msg = new Message("TopicOrderTest", "order_3", "KEY" + i, ("order_3 " + i).getBytes());

                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, 2);//

                System.err.println(sendResult);
            }

            producer.shutdown();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutdown();
        }

    }
}