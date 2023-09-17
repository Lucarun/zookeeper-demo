package com.example.zookeeperdemo.runner;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

/**
 * User: luca
 * Date: 2023/9/16
 * Description:
 */
//@Component
public class ZookeeperRunner implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
        try {
            final CountDownLatch countDownLatch=new CountDownLatch(1);
            ZooKeeper zooKeeper=
                    new ZooKeeper("127.0.0.1:2181," +
                            "127.0.0.1:2182,127.0.0.1:2183",
                            4000, new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            if(Event.KeeperState.SyncConnected==event.getState()){
                                //如果收到了服务端的响应事件，连接成功
                                countDownLatch.countDown();
                            }
                        }
                    });
            countDownLatch.await();
            //CONNECTED
            System.out.println(zooKeeper.getState());
        } catch (Exception e){

        }
    }
}
