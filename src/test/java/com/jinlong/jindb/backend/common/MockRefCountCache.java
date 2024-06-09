package com.jinlong.jindb.backend.common;

/**
 * MockRefCountCache
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class MockRefCountCache extends AbstractRefCountCache<Long> {

    public MockRefCountCache() {
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
