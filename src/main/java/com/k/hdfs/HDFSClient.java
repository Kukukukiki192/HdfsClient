package com.k.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

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
        //设置副本数
        configuration.set("dfs.replication", "2");

        // 用户
        String user = "K";

        // 1.获取客户端对象(文件系统)
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
        //创建文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));
    }

    /**上传文件
     * 参数优先级    hdfs-default.xml < hdfs-site.xml < resources/hdfs-site.xml < 代码中配置
     */
    @Test
    public void put() throws Exception {
        //参数说明(boolean delSrc是否删除原数据, boolean overwrite是否允许覆盖, Path src原数据路径win, Path dst目标路径HDFS)
        fs.copyFromLocalFile(false, true, new Path("src/main/resources/sunwukong.txt"), new Path("/xiyou/huaguoshan"));
        //                                                                     相对以项目为根路径                    或：hdfs://hadoop102/xiyou/huaguoshan
    }

    //下载文件
    @Test
    public void get() throws Exception {
        //参数说明(boolean delSrc是否删除原文件, Path src原文件路径HDFS, Path dst目标路径Win, boolean useRawLocalFileSystem是否开启crc文件校验-false为开启)
        fs.copyToLocalFile(true, new Path("/xiyou/huaguoshan"), new Path("D:/"), false);
    }

    //文件更名和移动
    @Test
    public void mv() throws IOException {
        //参数说明(Path var1原文件路径, Path var2目标文件路径)
        //重命名
//        fs.rename(new Path("/wcinput/word.txt"), new Path("/wcinput/ss.txt"));

        //移动并更名
//        fs.rename(new Path("/wcinput/ss.txt"), new Path("/cls.txt"));

        //目录更名
        fs.rename(new Path("/wcinput"), new Path("/input"));
    }

    //删除文件和目录
    @Test
    public void rm() throws IOException {
        //参数说明(Path var1要删除的路径, boolean var2是否递归删除)
        //删除文件
//        fs.delete(new Path("/hadoop-3.1.3.tar.gz"), false);

        //删除空目录
//        fs.delete(new Path("/input"), false);

        //删除非空目录
        fs.delete(new Path("/jingguo"), true);
    }

    //查看文件详情
    @Test
    public void fileDetail() throws IOException {
        //递归遍历根目录下所有文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("-------------" + fileStatus.getPath() + "-------------");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());    //Size
            System.out.println(fileStatus.getModificationTime());   //Last Modified
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());
            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //文件和文件夹判断
    @Test
    public void isFile() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()) {
                System.out.println("文件：" + fileStatus.getPath().getName());
            } else {
                System.out.println("目录：" + fileStatus.getPath().getName());
            }
        }
    }
}