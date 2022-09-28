package com.adrien.hdfs.operation;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSOperation {

    @Test
    public void mkDir () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path("/adrien/dir001"));
        fileSystem.close();
    }

    @Test
    public void mkDirWithPermission () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ); //777
        fileSystem.mkdirs(new Path("/adrien/dir002Per"),fsPermission);
        fileSystem.close();
    }

    @Test
    public void uploadFile () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyFromLocalFile(
                new Path(""),
                new Path(""));
        fileSystem.close();

        new DistributedFileSystem();

    }

    @Test
    public void downloadFile () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.copyToLocalFile(
                new Path(""),
                new Path("")
        );
        fileSystem.close();
    }

    @Test
    public void renameFile () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.rename(
                new Path(""),
                new Path("")
        );
        fileSystem.close();
    }

    @Test
    public void deleteFile () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        //boolean 是否递归删除
        fileSystem.delete(
                new Path("/adrien/output/flow"),true);
        fileSystem.close();
    }

    /**
     * 查看文件信息
     */
    @Test
    public void listFiles () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        //boolean 是否递归查询
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(
                new Path(""), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();
            //输出文件名称
            System.out.println(status.getPath().getName());
            //长度
            System.out.println(status.getLen());
            //权限
            System.out.println(status.getPermission());
            //分组
            System.out.println(status.getGroup());
            //获取存储块信息
            System.out.println("======= hosts ========");
            for (BlockLocation blockLocation : status.getBlockLocations()) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println(status.isFile() == true ? "问价" : "文件夹");
        }
        fileSystem.close();
    }


    @Test
    public void catFiles () throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop101:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        //boolean 是否递归删除
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(""));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }
        fileSystem.close();
    }


    /**
     * IO 流
     */
    @Test
    public void putFileToHDFS () throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop101:9000"), configuration);
        //创建输入流
        FileInputStream fis = new FileInputStream(new File(""));
        FSDataOutputStream fos = fileSystem.create(new Path(""));
        IOUtils.copy(fis,fos);
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(fis);
    }


    @Test
    public void mergeFile() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI(""), new Configuration(), "hadoop");
        FSDataOutputStream fos = fileSystem.create(new Path("hdfs://hadoop101:9000/xx.xml"));
        //获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //读取本地文件
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("file://本地"));
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            FSDataInputStream fis = localFileSystem.open(path);
            IOUtils.copy(fis,fos);
            IOUtils.closeQuietly(fis);
        }
        IOUtils.closeQuietly(fos);
        localFileSystem.close();
        fileSystem.close();
    }


}
