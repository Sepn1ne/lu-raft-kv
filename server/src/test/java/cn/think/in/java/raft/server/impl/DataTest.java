package cn.think.in.java.raft.server.impl;

import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.*;

/**
 * @author Sepn1ne
 * @version 1.0
 * @description: 测试数据库中的数据
 * @date 2024/10/22 23:32
 */
public class DataTest {
    private DefaultLogModule logModule= DefaultLogModule.getInstance();
    private RocksDB rocksDB;
    static {
        RocksDB.loadLibrary();
    }

    public void printAllLog(String node){
        //String filePath = "D:/aaaJavaStudy/source-project/lu-raft-kv/rocksDB-raft/" + node + "/logModule";
        // 构建相对路径
        String baseDir = System.getProperty("user.dir");
        String projectRootDir = baseDir.replace("\\server", "");
        String filePath = projectRootDir + "/rocksDB-raft/" + node + "/logModule";
        System.out.println(filePath);
        Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            rocksDB = RocksDB.open(options, filePath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        RocksIterator rocksIterator = rocksDB.newIterator();
        int count = 1;
        System.out.println("==================端口号为: " + 8775 +"的结点的日志如下:===================");
        for(rocksIterator.seekToFirst();rocksIterator.isValid();rocksIterator.next()){
            String key = new String(rocksIterator.key());
            String value = new String(rocksIterator.value());
            System.out.println("日志:" +key +":"+value);
        }
    }
    public void printAllData(String node){
        //String filePath = "D:/aaaJavaStudy/source-project/lu-raft-kv/rocksDB-raft/" + node + "/logModule";
        // 构建相对路径
        String baseDir = System.getProperty("user.dir");
        String projectRootDir = baseDir.replace("\\server", "");
        String filePath = projectRootDir + "/rocksDB-raft/" + node ;
        System.out.println(filePath);
        Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            rocksDB = RocksDB.open(options, filePath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        RocksIterator rocksIterator = rocksDB.newIterator();
        int count = 1;
        System.out.println("==================端口号为: " + node +"的结点的数据如下:===================");
        for(rocksIterator.seekToFirst();rocksIterator.isValid();rocksIterator.next()){
            String key = new String(rocksIterator.key());
            String value = new String(rocksIterator.value());
            System.out.println("数据:" +key +":"+value);
        }
    }

    //输出所有结点的日志
    @Test
    public void printLog(){
        String[] nodes = {"8775","8776","8777","8778","8779"};
        for(String node:nodes){
            printAllLog(node);
        }
    }

    //输出所有结点的状态机中的数据
    @Test
    public void printData(){
        String[] nodes = {"8775","8776","8777","8778","8779"};
        for(String node:nodes){
            printAllData(node);
        }
    }

    //测试是否不同节点中的日志是否全部一致
    @Test
    public void testIfLogIsSameInAllServer(){
        String baseDir = System.getProperty("user.dir");
        String projectRootDir = baseDir.replace("\\server", "");

        String[] nodes = {"8775","8776","8777","8778","8779"};
        String[] paths = new String[5];
        for(int i=0;i<5;i++){
            paths[i] = projectRootDir + "/rocksDB-raft/" + nodes[i] + "/logModule";
        }
        for(int i=0;i<5;i++){
            System.out.println(paths[i]);
        }

        Options options = new Options();
        options.setCreateIfMissing(true);

        List<Map<String, String>> allData = new ArrayList<>();

        // 读取每个数据库的数据
        for (String path : paths) {
            try (RocksDB db = RocksDB.open(options, path)) {
                Map<String, String> data = new HashMap<>();
                try (RocksIterator iterator = db.newIterator()) {
                    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                        String key = new String(iterator.key());
                        String value = new String(iterator.value());
                        data.put(key, value);
                    }
                }
                allData.add(data);
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }

        // 比较所有数据库的数据
        boolean isConsistent = true;
        for (int i = 1; i < allData.size(); i++) {
            if (!allData.get(0).equals(allData.get(i))) {
                isConsistent = false;
                System.out.println("数据不一致!");
                Map<String,String> m1 = allData.get(0);
                Map<String,String> m2 = allData.get(i);
                Set<Map.Entry<String, String>> entries = m1.entrySet();
                for(Map.Entry<String, String> entry : entries){
                    String key = entry.getKey();
                    String value = entry.getValue();
                    if(!value.equals(m2.get(key))){
                        System.out.println("m1中的数据:" + key + ":" + value);
                        System.out.println("m2中的数据:" + key + ":" + m2.get(key));
                    }
                }
                break;
            }
        }

        if (isConsistent) {
            System.out.println("所有数据库中的数据一致。");
        } else {
            System.out.println("数据库中的数据不一致。");
        }


    }


}
