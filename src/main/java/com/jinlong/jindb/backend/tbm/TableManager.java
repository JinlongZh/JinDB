package com.jinlong.jindb.backend.tbm;

import com.jinlong.jindb.backend.dm.DataManager;
import com.jinlong.jindb.backend.parser.statement.*;
import com.jinlong.jindb.backend.utils.Parser;
import com.jinlong.jindb.backend.vm.VersionManager;

/**
 * TableManager
 *
 * @Author zjl
 * @Date 2024/6/18
 */
public interface TableManager {

    BeginRes begin(Begin begin);

    byte[] commit(long xid) throws Exception;

    byte[] abort(long xid);

    byte[] show(long xid);

    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;

    byte[] read(long xid, Select select) throws Exception;

    byte[] update(long xid, Update update) throws Exception;

    byte[] delete(long xid, Delete delete) throws Exception;

    static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }

}
