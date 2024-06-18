package com.jinlong.jindb.common;

/**
 * 统一异常管理
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class ErrorConstants {
    // common
    public static final Exception CacheFullException = new RuntimeException("Cache is full!");

    // tm
    public static final Exception BadXIDFileException = new RuntimeException("Bad XID file!");

    // vm
    public static final Exception DeadlockException = new RuntimeException("Deadlock!");
    public static final Exception ConcurrentUpdateException = new RuntimeException("Concurrent update issue!");
    public static final Exception NullEntryException = new RuntimeException("Null entry!");

    // tbm
    public static final Exception InvalidFieldException = new RuntimeException("Invalid field type!");
}
