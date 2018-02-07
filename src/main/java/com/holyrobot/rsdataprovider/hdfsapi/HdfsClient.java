package com.holyrobot.rsdataprovider.hdfsapi;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@RestController
public class HdfsClient {

    private static final String HDFS_URI = "hdfs://node2:8020";

    /**
     * 获取文件系统
     * @return
     */
    public static FileSystem getFileSystem(){
        //读取配置文件
        Configuration con = new Configuration();
        FileSystem fs = null;
        if(StringUtils.isBlank(HDFS_URI)){
            // 返回默认文件系统  如果在 Hadoop集群下运行，使用此种方法可直接获取默认文件系统
            try {
                fs = FileSystem.get(con);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            try {
                fs = FileSystem.get(new URI(HDFS_URI),con);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return fs;
    }

    /**
     * 创建目录
     * @param paths     目录路径
     * @return
     */
    @PostMapping(value = "/mkdir")
    public String mkdir(@RequestParam(value = "paths") String paths){
        FileSystem fs = null;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                paths = HDFS_URI + paths;
            }
            fs = getFileSystem();
            Path path = new Path(paths);
            fs.mkdirs(path);
            return "目录: " + paths + " 创建成功!";
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "目录: " + paths + " 创建失败!";
    }

    /**
     * 删除目录
     * @param paths     目录路径
     * @return
     */
    @PostMapping(value = "rmdir")
    public String rmdir(@RequestParam(value = "paths") String paths){
        FileSystem fs = null;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                paths = HDFS_URI + paths;
            }
            fs = getFileSystem();
            Path path = new Path(paths);
            fs.delete(path,true);
            return "目录: " + paths + " 删除成功!";
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "目录: " + paths + " 删除失败!";
    }

    /**
     * 文件重命名
     * @param oldname   原文件名
     * @param newname   新文件名
     * @return
     */
    @PostMapping(value = "rename")
    public boolean rename(@RequestParam(value = "oldname") String oldname,
                          @RequestParam(value = "newname") String newname){
        FileSystem fs = null;
        boolean flag = false;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                oldname = HDFS_URI + oldname;
                newname = HDFS_URI + newname;
            }
            fs = getFileSystem();
            Path oldpath = new Path(oldname);
            Path newpath = new Path(newname);
            flag = fs.rename(oldpath, newpath);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return flag;
    }

    /**
     * 判断目录是否存在
     * @param paths  目录路径
     * @param create    是否创建目录
     * @return
     */
    @PostMapping(value = "isexist")
    public boolean existDir(@RequestParam(value = "paths") String paths,
                            @RequestParam(value = "create",required = false,defaultValue = "false") boolean create){
        boolean flag = false;
        FileSystem fs = null;
        if (StringUtils.isEmpty(paths)){
            return flag;
        }
        try{
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                paths = HDFS_URI + paths;
            }
            Path path = new Path(paths);
            fs = getFileSystem();
            if (create){
                if (!fs.exists(path)){
                    fs.mkdirs(path);
                }
            }
            if (fs.isDirectory(path)){
                flag = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return flag;
    }

    /**
     * 创建新文件
     * @param paths     文件路径名字
     * @param contents      文件中的内容
     * @return
     */
    @PostMapping(value = "/createFile")
    public boolean createFile(@RequestParam(value = "paths") String paths,
                           @RequestParam(value = "contents",required = false,defaultValue = "") String contents){
        FileSystem fs = null;
        FSDataOutputStream out = null;
        boolean flag = false;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                paths = HDFS_URI + paths;
            }
            fs = getFileSystem();
            Path path = new Path(paths);
            out = fs.create(path);
            out.write(contents.getBytes());
            out.flush();
            flag = true;
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                out.close();
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return flag;
    }

    /**
     * 文件上传到HDFS（小文件）
     * @param delSrc        是否删除源文件（默认false）
     * @param overwrite     是否覆盖已有文件（默认false）
     * @param localPath       源文件路径（本地）
     * @param hdfsPath      HDFS上传路径(不包含文件名)
     * @return
     */
    @PostMapping(value = "/up")
    public String copyFileToHDFS(@RequestParam(value = "delSrc",required = false,defaultValue = "false") boolean delSrc,
                                @RequestParam(value = "overwrite",required = false,defaultValue = "false") boolean overwrite,
                                @RequestParam(value = "localPath") String localPath,
                                @RequestParam(value = "hdfsPath") String hdfsPath){
        FileSystem fs = null;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                hdfsPath = HDFS_URI + hdfsPath;
            }
            fs = getFileSystem();
            Path localPaths = new Path(localPath);
            //获取上传的文件名字
            String fileName = localPath.substring(localPath.lastIndexOf('/')+1,localPath.length());
            //创建目标路径，包括文件名。
            if(hdfsPath.charAt(hdfsPath.length()-1) != '/'){
                hdfsPath = hdfsPath + "/" + fileName;
            }else{
                hdfsPath = hdfsPath + fileName;
            }
            Path hdfsPaths = new Path(hdfsPath);
            //如果选择不覆盖已存在文件，就判断该文件在HDFS中是否存在
            if (!overwrite){
                boolean isexist = fs.exists(hdfsPaths);
                if(isexist){
                    return "false,该文件已存在!";
                }
            }
            //上传源文件
            fs.copyFromLocalFile(delSrc,overwrite,localPaths, hdfsPaths);
            return "true,文件上传成功!";
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "false,文件上传失败!";
    }

    /**
     * 大文件上传
     * @param localPath     源文件路径（本地）
     * @param hdfsPath      HDFS上传路径(不包含文件名)
     * @return
     */
    @PostMapping(value = "/bigUp")
    public String copyBigFileToHDFS(@RequestParam(value = "localPath") String localPath,
                                 @RequestParam(value = "hdfsPath") String hdfsPath){
        FileSystem fs = null;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                hdfsPath = HDFS_URI + hdfsPath;
            }
            fs = getFileSystem();
            //获取上传的文件名字
            String fileName = localPath.substring(localPath.lastIndexOf('/')+1,localPath.length());
            //创建目标路径，包括文件名。
            if(hdfsPath.charAt(hdfsPath.length()-1) != '/'){
                hdfsPath = hdfsPath + "/" + fileName;
            }else{
                hdfsPath = hdfsPath + fileName;
            }
            Path hdfsPaths = new Path(hdfsPath);
            //如果选择不覆盖已存在文件，就判断该文件在HDFS中是否存在
            boolean isexist = fs.exists(hdfsPaths);
            if(isexist){
                return "false,该文件已存在!";
            }
            InputStream in = new BufferedInputStream(new FileInputStream(new File(localPath)));
            FSDataOutputStream out = fs.create(hdfsPaths);
            IOUtils.copyBytes(in, out,2048, true);
            return "true,文件上传成功!";
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "false,文件上传失败!";
    }

    /**
     * 从HDFS中下载文件
     * @param delSrc        是否删除源文件（默认false）
     * @param windows       是否使用原生本地文件系统
     * @param localPath       源文件路径（HDFS路径）
     * @param hdfsPath      目的文件路径（不包含文件名）
     * @return
     */
    @PostMapping(value = "/down")
    public String copyFileToLoca(@RequestParam(value = "delSrc",required = false,defaultValue = "false") boolean delSrc,
                               @RequestParam(value = "windows",required = false,defaultValue = "false") boolean windows,
                               @RequestParam(value = "localPath") String localPath,
                               @RequestParam(value = "hdfsPath") String hdfsPath){
        FileSystem fs = null;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                hdfsPath = HDFS_URI + hdfsPath;
            }
            fs = getFileSystem();
            Path hdfsPaths = new Path(hdfsPath);
            //判断HDFS中是否存在该文件
            if(!fs.exists(hdfsPaths)){
                return "false,该文件不存在!";
            }
            //获取上传的文件名字
            String fileName = hdfsPath.substring(hdfsPath.lastIndexOf('/')+1,hdfsPath.length());
            //创建目标路径，包括文件名。
            if(localPath.charAt(localPath.length()-1) != '/'){
                localPath = localPath + "/" + fileName;
            }else{
                localPath = localPath + fileName;
            }
            Path localPaths = new Path(localPath);
            //下载
            fs.copyToLocalFile(delSrc, hdfsPaths, localPaths, windows);
            return "true,文件下载成功!";
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "false,文件下载失败!";
    }

    /**
     * 大文件下载
     * @param localPath     目的文件路径（不包含文件名）
     * @param hdfsPath      HDFS源文件路径
     * @return
     */
    @PostMapping(value = "/bigDown")
    public String copyFileToLoca(@RequestParam(value = "localPath") String localPath,
                                 @RequestParam(value = "hdfsPath") String hdfsPath){
        FileSystem fs = null;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(hdfsUri)){
                hdfsPath = HDFS_URI + hdfsPath;
            }
            fs = getFileSystem();
            Path hdfsPaths = new Path(hdfsPath);
            //判断HDFS中是否存在该文件
            if(!fs.exists(hdfsPaths)){
                return "false,该文件不存在!";
            }
            //获取上传的文件名字
            String fileName = hdfsPath.substring(hdfsPath.lastIndexOf('/')+1,hdfsPath.length());
            //创建目标路径，包括文件名。
            if(localPath.charAt(localPath.length()-1) != '/'){
                localPath = localPath + "/" + fileName;
            }else{
                localPath = localPath + fileName;
            }
            //下载
            FSDataInputStream in = fs.open(hdfsPaths);
            OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(localPath)));
            IOUtils.copyBytes(in, out, 2048, true);
            return "true,文件下载成功!";
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "false,文件下载失败!";
    }

    /**
     * 获取路径下的文件列表
     * @param paths
     * @return
     */
    @PostMapping(value = "/pathFiles")
    public Object pathFiles(@RequestParam(value = "paths") String paths){
        FileSystem fs = null;
        String[] files = new String[0];
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(paths)){
                paths = HDFS_URI + paths;
            }
            fs = getFileSystem();
            Path path = new Path(paths);
            FileStatus[] fileStatuses = fs.listStatus(path);
            // 获取目录下的所有文件路径
            Path[] pathArr = FileUtil.stat2Paths(fileStatuses);
            //将目录转换为目录数组
            if (pathArr != null && pathArr.length > 0){
                files = new String[pathArr.length];
                for (int i = 0; i < files.length; i++){
                    files[i] = pathArr[i].toString();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return JSON.toJSON(files);
    }

    /**
     * 递归列出给定路径下的文件路径信息
     * @param paths
     * @return
     */
    @PostMapping(value = "/RPathFiles")
    public Object rPathFiles(@RequestParam(value = "paths") String paths){
        FileSystem fs = null;
        List<String> files = new ArrayList<String>();
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(paths)){
                paths = HDFS_URI + paths;
            }
            fs = getFileSystem();
            Path path = new Path(paths);
            //列出给定路径中文件的状态和块位置
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path,true);
            while (iterator.hasNext()){
                LocatedFileStatus next = iterator.next();
                String filePath = next.getPath().toString();
                files.add(filePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return JSON.toJSON(files);
    }

    /**
     * 查找某个文件在 HDFS集群的位置信息
     * @param paths
     * @return
     */
    @PostMapping(value = "/fileBlock")
    public Object fileBlock(@RequestParam(value = "paths") String paths){
        FileSystem fs = null;
        try {
            String hdfsUri = HDFS_URI;
            if(StringUtils.isNotBlank(paths)){
                paths = HDFS_URI + paths;
            }
            fs = getFileSystem();
            Path path = new Path(paths);
            //获取文件路径
            FileStatus fileStatus = fs.getFileStatus(path);
            //获取文件块位置列表
            BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            return JSON.toJSON(blockLocations);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 获取 HDFS 集群节点信息
     * @return
     */
    @GetMapping(value = "/dataNodeInfo")
    public Object dataNodeInfo(){
        FileSystem fs = null;
        DatanodeInfo[] dataNodeStats = new DatanodeInfo[0];
        try {
            fs = getFileSystem();
            // 获取分布式文件系统
            DistributedFileSystem hdfs = (DistributedFileSystem)fs;
            dataNodeStats = hdfs.getDataNodeStats();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return JSON.toJSON(dataNodeStats);
    }
}
