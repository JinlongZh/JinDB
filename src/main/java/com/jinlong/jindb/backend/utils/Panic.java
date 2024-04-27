package com.jinlong.jindb.backend.utils;

/**
 * Panic
 *
 * @Author zjl
 * @Date 2024/4/27 17:40
 */
public class Panic {

    public static void panic(Exception e) {
        e.printStackTrace();
        System.exit(1);
    }

}
