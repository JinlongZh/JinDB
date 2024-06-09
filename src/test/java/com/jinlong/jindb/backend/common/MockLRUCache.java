package com.jinlong.jindb.backend.common;

/**
 * LRU驱逐策略的抽象缓存Mock
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class MockLRUCache extends AbstractLRUCache<Long> {

    public MockLRUCache() {
        super(50);
    }

    @Override
    protected Long getForCache(long key) throws Exception {
        return key;
    }

    @Override
    protected void releaseForCache(Long obj) {
    }

}
