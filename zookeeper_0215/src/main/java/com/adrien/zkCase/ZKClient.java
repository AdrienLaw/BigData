package com.adrien.zkCase;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZKClient {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk = null;
    private String parentNode = "/servers";
    //1. 获取连接
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());
                try {
                    //确保可以循环执行
                    getServers();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //2. 监听节点变化
    public void getServers() throws KeeperException, InterruptedException {
        //监听parentNode 路径子节点
        List<String> children = zk.getChildren(parentNode, true);
        ArrayList<String> servers = new ArrayList<>();
        //获取子节点信息
        for (String child : children) {
            byte[] data = zk.getData(parentNode + "/" + child, false, null);
            //保存信息
            servers.add(new String(data));
        }
        //打印数据
        System.out.println(servers);
    }


    //3. 业务逻辑
    public void business() throws InterruptedException {
        System.out.println("来送客");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //1. 连接
        ZKClient zkClient = new ZKClient();
        zkClient.getConnect();
        //2. 监听节点变化
        zkClient.getServers();
        //3. 实现业务逻辑
        zkClient.business();
    }
}
