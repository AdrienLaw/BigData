package com.adrien.zkCase;

import org.apache.zookeeper.*;

import java.io.IOException;

public class ZKServer {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";
    //1. 获取连接
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    //2. 注册
    public void regist(String hostname) throws KeeperException, InterruptedException {
        String create = zk.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online "+ create);
    }


    //3. 业务逻辑
    public void business() throws InterruptedException {
        System.out.println("来接客");
        Thread.sleep(Long.MAX_VALUE);
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //1. 获取连接
        ZKServer zkServer = new ZKServer();
        zkServer.getConnect();
        //2. 注册（创建节点）
        zkServer.regist(args[0]);
        //3. 业务逻辑
        zkServer.business();
    }
}
