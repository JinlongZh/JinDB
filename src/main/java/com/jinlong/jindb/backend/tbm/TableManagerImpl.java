package com.jinlong.jindb.backend.tbm;

import com.jinlong.jindb.backend.dm.DataManager;
import com.jinlong.jindb.backend.parser.statement.*;
import com.jinlong.jindb.backend.utils.Parser;
import com.jinlong.jindb.backend.vm.VersionManager;
import com.jinlong.jindb.common.ErrorConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TableManagerImpl
 *
 * @Author zjl
 * @Date 2024/6/18
 */
public class TableManagerImpl implements TableManager {

    VersionManager versionManager;

    DataManager dataManager;

    private Booter booter;
    private Map<String, Table> tableCache;
    private Map<Long, List<Table>> xidTableCache;
    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.versionManager = vm;
        this.dataManager = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }

    private void loadTables() {
        long uid = firstTableUid();
        while (uid != 0) {
            Table table = Table.loadTable(this, uid);
            uid = table.nextUid;
            tableCache.put(table.name, table);
        }
    }

    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead ? 1 : 0;
        res.xid = versionManager.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    @Override
    public byte[] commit(long xid) throws Exception {
        versionManager.commit(xid);
        return "commit".getBytes();
    }

    @Override
    public byte[] abort(long xid) {
        versionManager.abort(xid);
        return "abort".getBytes();
    }

    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            List<Table> t = xidTableCache.get(xid);
            if (t == null) {
                return "\n".getBytes();
            }
            for (Table tb : t) {
                sb.append(tb.toString()).append("\n");
            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if (tableCache.containsKey(create.tableName)) {
                throw ErrorConstants.DuplicatedTableException;
            }
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            updateFirstTableUid(table.uid);
            tableCache.put(create.tableName, table);
            if (!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if (table == null) {
            throw ErrorConstants.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    @Override
    public byte[] read(long xid, Select select) throws Exception {
        lock.lock();
        Table table = tableCache.get(select.tableName);
        lock.unlock();
        if (table == null) {
            throw ErrorConstants.TableNotFoundException;
        }
        return table.read(xid, select).getBytes();
    }

    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if (table == null) {
            throw ErrorConstants.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if (table == null) {
            throw ErrorConstants.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }

    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }
}
