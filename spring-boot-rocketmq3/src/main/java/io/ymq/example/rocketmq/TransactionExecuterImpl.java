package io.ymq.example.rocketmq;

import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.common.message.Message;

import java.util.Random;

public class TransactionExecuterImpl implements LocalTransactionExecuter{
    public LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg){
        try {

            if(new Random().nextInt(5)==2){
                int a=1/0;
            }
        }catch (Exception e){
            System.out.println("执行本地任务失败！！！！！！" );//输出消息内容
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        System.out.println("执行本地任务成功---------------发送确认消息。" );//输出消息内容
        return LocalTransactionState.COMMIT_MESSAGE;

    }

}
