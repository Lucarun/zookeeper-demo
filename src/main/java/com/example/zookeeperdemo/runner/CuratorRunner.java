package com.example.zookeeperdemo.runner;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * User: luca
 * Date: 2023/9/17
 * Description:
 */

@Component
public class CuratorRunner implements CommandLineRunner {


    static String USERNAME_PASSWORD = "luca:123";

    @Override
    public void run(String... args) throws Exception {
        CuratorFramework curatorFramework= CuratorFrameworkFactory
                .builder().connectString("127.0.0.1:2181," +
                        "127.0.0.1:2182,127.0.0.1:2183")
                .sessionTimeoutMs(4000).retryPolicy(new
                        ExponentialBackoffRetry(1000,3))
//                .authorization("digest", USERNAME_PASSWORD.getBytes())
                .namespace("")
                .build();
        curatorFramework.start();

        String path = "/test";
        curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path,"Luca".getBytes());
        curatorFramework.setData().forPath(path, "LucaTest".getBytes());

        Stat stat1=new Stat();
        byte[] bytes = curatorFramework.getData().storingStatIn(stat1).forPath(path);
        System.out.println("path : " + path + " content: " + new String(bytes));


        Stat stat = curatorFramework.checkExists().forPath(path);
        if (stat != null) {

            System.out.println("stat : " + stat);

            System.out.println("Node exists with version: " + stat.getVersion());
            System.out.println("Data length: " + stat.getDataLength());
            System.out.println("Children count: " + stat.getNumChildren());
            System.out.println("Node ctime: " + stat.getCtime());
            System.out.println("Node mtime: " + stat.getMtime());
            System.out.println("Mzxid: " + stat.getMzxid());
            System.out.println("Pzxid: " + stat.getPzxid());
        } else {
            System.out.println("Node does not exist.");
        }


        List<ACL> newAcl = Arrays.asList(
                new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest(USERNAME_PASSWORD)))
                ,new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.8.16"))
        );
        curatorFramework.setACL().withACL(newAcl).forPath(path);
        System.out.println("Node ACL updated successfully.");


        List<ACL> aclList = curatorFramework.getACL().forPath(path);
        for (ACL acl : aclList) {
            System.out.println("ACL: " + acl.toString());
        }

        curatorFramework.close();
    }

}
