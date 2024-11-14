/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package cn.think.in.java.raft.server.impl;

import cn.think.in.java.raft.common.entity.Command;
import cn.think.in.java.raft.common.entity.LogEntry;
import cn.think.in.java.raft.server.StateMachine;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * 默认的状态机实现.
 *
 * @author 莫那·鲁道
 */
@Slf4j
public class DefaultStateMachine implements StateMachine {

    /** public just for test */
    public String dbDir;
    public String stateMachineDir;

    //public RocksDB machineDb;
    public TransactionDB machineDb;

    // key
    private static final byte[] LAST_APPLIED_KEY = "LAST_APPLIED_KEY".getBytes();
    private static final byte[] VOTED_FOR_KEY = "VOTED_FOR_KEY".getBytes();


    private DefaultStateMachine() {
        dbDir = "./rocksDB-raft/" + System.getProperty("serverPort");

        stateMachineDir = dbDir + "/stateMachine";
        RocksDB.loadLibrary();

        File file = new File(stateMachineDir);
        boolean success = false;

        if (!file.exists()) {
            success = file.mkdirs();
        }
        if (success) {
            log.warn("make a new dir : " + stateMachineDir);
        }

        Options options = new Options();
        options.setCreateIfMissing(true);

        TransactionDBOptions transactionDBOptions = new TransactionDBOptions();

        try {
            //machineDb = RocksDB.open(options, stateMachineDir);
            machineDb = TransactionDB.open(options, transactionDBOptions, stateMachineDir);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public static DefaultStateMachine getInstance() {
        return DefaultStateMachineLazyHolder.INSTANCE;
    }

    @Override
    public void init() throws Throwable {

    }

    @Override
    public void destroy() throws Throwable {
        machineDb.close();
        log.info("destroy success");
    }

    private static class DefaultStateMachineLazyHolder {

        private static final DefaultStateMachine INSTANCE = new DefaultStateMachine();
    }

    @Override
    public LogEntry get(String key) {
        try {
            byte[] result = machineDb.get(key.getBytes());
            if (result == null) {
                return null;
            }
            return JSON.parseObject(result, LogEntry.class);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getString(String key) {
        try {
            byte[] bytes = machineDb.get(key.getBytes());
            if (bytes != null) {
                return new String(bytes);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return "";
    }

    @Override
    public void setString(String key, String value) {
        try {
            machineDb.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delString(String... key) {
        try {
            for (String s : key) {
                machineDb.delete(s.getBytes());
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    //当使用TransactionDB的时候，所有正在修改的RocksDB里的key都会被上锁，因此可以不适用synchronized关键字
    //但是在高并发的情况下，可能两个线程的apply顺序会发生变化，因此应该在调用apply的方法中，对可能存在的并发操作进行同步。
    @Override
    public void apply(LogEntry logEntry) {

        Command command = logEntry.getCommand();
        if (command == null) {
            // 忽略空日志
            return;
        }
        String key = command.getKey();
        Long index = logEntry.getIndex();

        //对于实现了AutoCloseable接口的对象，在调用close()方法时，会自动释放资源，也可以使用try-catch-resource关键字来释放资源。
        Transaction transaction = null;
        try (
                WriteOptions writeOptions = new WriteOptions();
                TransactionOptions transactionOptions = new TransactionOptions();
        ) {
            transaction = machineDb.beginTransaction(writeOptions, transactionOptions);
            writeOptions.setSync(true);
            transaction.put(key.getBytes(), JSON.toJSONBytes(logEntry));
            //测试事务
            //System.exit(1);
            transaction.put(LAST_APPLIED_KEY, index.toString().getBytes());
            transaction.commit();
        } catch (RocksDBException e) {
            log.error("Error during transaction commit, index: {}", index, e);
            e.printStackTrace();
            try {
                transaction.rollback();
            } catch (RocksDBException ex) {
                ex.printStackTrace();
                log.error("rollback fail...");
            }
        } finally {
            if(transaction != null){
                transaction.close();
            }
        }
    }

    public long getLastApplied() {
        long res = 0;
        try {
            byte[] bytes = machineDb.get(LAST_APPLIED_KEY);
            if(bytes == null || bytes.length == 0){
                return res;
            }
            res = Long.parseLong(new String(bytes));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
            // e.printStackTrace();
            // log.error("get lastApplied fail...");
        }
        return res;
    }

    public void setVotedFor(String votedFor){
        if(votedFor == null) {
            log.error("votedFor is null...");
            return ;
        }
        // if("".equals(votedFor)) {
        //     log.info("votedFor is \"\"...");
        // }
        try {
            machineDb.put(VOTED_FOR_KEY,votedFor.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
            //log.error("set votedFor fail, the votedFor is {}", votedFor,e);
            //e.printStackTrace();
        }
    }

    public String getVotedFor(){
        String res = "";
        try {
            byte[] bytes = machineDb.get(VOTED_FOR_KEY);
            // 当DB中不存在votedFor时，应该返回空字符串""表示当前结点未投票
            if(bytes == null || bytes.length == 0){
                return res;
            }
            res = new String(machineDb.get(VOTED_FOR_KEY));
        } catch (RocksDBException e) {
            log.error("get votedFor fail...");
            //e.printStackTrace();
        }
        return res;
    }

}
