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
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 *
 * @author 莫那·鲁道
 */
public class DefaultStateMachineTest {
    static DefaultStateMachine machine = DefaultStateMachine.getInstance();

    static {
        System.setProperty("serverPort", "8777");
        machine.dbDir = "./rocksDB-raft/" + System.getProperty("serverPort");

        //machine.dbDir = "/Users/cxs/code/lu-raft-revert/rocksDB-raft/" + System.getProperty("serverPort");
        machine.stateMachineDir = machine.dbDir + "/stateMachine";
    }

    @Before
    public void before() {
        machine = DefaultStateMachine.getInstance();
    }

    @Test
    public void apply() {
        LogEntry logEntry = LogEntry.builder().index(1L).term(1).command(Command.builder().key("hello").value("value1").build()).build();
        machine.apply(logEntry);


        System.out.println(machine.get("hello").toString());
        System.out.println(machine.getLastApplied()); //1
    }

    //测试状态机的getLastApplied()
    @Test
    public void testGetLastApplied(){
        LogEntry logEntry1 = LogEntry.builder().index(1L).term(1).command(Command.builder().key("hello1").value("value1").build()).build();
        LogEntry logEntry2 = LogEntry.builder().index(2L).term(1).command(Command.builder().key("hello2").value("value2").build()).build();
        machine.apply(logEntry1);
        machine.apply(logEntry2);
        machine.get("hello1");
        machine.get("hello12");
        System.out.println(machine.getLastApplied());//2
    }

    //测试apply()方法的synchronized去掉后是否会导致数据丢失
    @Test
    public void testParallelApply(){
        long[] nums = new long[100];
        for(int i=0;i<100;i++){
            nums[i] = i;
        }
        for(long n : nums){
            new Thread(()->{
                LogEntry log = LogEntry.builder().index(n).term(1).command(Command.builder().key("hello" + n).value("value" + n).build()).build();
                machine.apply(log);
            }).start();
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        printAllData();
    }

    //测试rocksDB的事务是否能够保证原子性：见DefaultStateMachine中的System.exit(1)
    @Test
    public void testExistWhenTransaction(){
        machine.apply(LogEntry.builder().index(1L).term(1).command(Command.builder().key("hello").value("value1").build()).build());
    }

    //输出状态机的数据
    public void printAllData(){
        RocksIterator rocksIterator = machine.machineDb.newIterator();
        int cnt = 0;
        for(rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()){
            System.out.println(new String(rocksIterator.key()) + ":" + new String(rocksIterator.value()));
            cnt++;
        }
        System.out.println("total:" + cnt);
    }


    @Test
    public void applyRead() throws RocksDBException {

        System.out.println(machine.get("hello:7"));
    }
}
