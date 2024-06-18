package com.jinlong.jindb.backend.tbm;

import com.jinlong.jindb.backend.tm.TransactionManagerImpl;
import com.jinlong.jindb.backend.utils.Panic;

import java.util.ArrayList;
import java.util.List;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 *
 * @Author zjl
 * @Date 2024/6/18
 */
public class Table {

    TableManager tableManager;
    long uid;
    String name;
    byte status;
    long nextUid;
    List<Field> fields = new ArrayList<>();

    public Table(TableManager tableManager, long uid) {
        this.tableManager = tableManager;
        this.uid = uid;
    }

    public Table(TableManager tableManager, String tableName, long nextUid) {
        this.tableManager = tableManager;
        this.name = tableName;
        this.nextUid = nextUid;
    }

}
