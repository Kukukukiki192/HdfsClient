package com.k.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 客户端代码常用套路
 * 1. 获取一个客户端对象
 * 2. 执行相关操作命令
 * 3. 关闭资源
 * HDFS Zookeeper
 */
public class HDFSClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");

        //创建配置文件
        Configuration configuration = new Configuration();
        // 用户
        String user = "K";

        // 1.获取客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws IOException {
        // 3.关闭资源
        fs.close();
    }

    //创建目录
    @Test
    public void mkdir() throws Exception {
        // 2.创建文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }


}
