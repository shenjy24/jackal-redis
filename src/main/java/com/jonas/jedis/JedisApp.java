package com.jonas.jedis;

import java.sql.Timestamp;

/**
 * @author shenjy
 * @date 2020/6/17
 * @description
 */
public class JedisApp {

    public static void main(String[] args) {
        JedisManager manager = new JedisManager();

        for (int i = 0; i < 20; i++) {
            new JedisThread("thread-" + i, manager).start();
        }
    }

    static class JedisThread extends Thread {
        private JedisManager manager;

        public JedisThread(String name, JedisManager manager) {
            super(name);
            this.manager = manager;
        }

        @Override
        public void run() {
            String lockId = null;
            try {
                lockId = manager.lock(JedisManager.LOCK_KEY, 5);
                if (null == lockId) {
                    System.out.println(Thread.currentThread().getName() + " does not get lock!");
                    return;
                }

                for (int i = 0; i < 10; i++) {
                    Thread.sleep(10);
                    System.out.println(i + ":" + Thread.currentThread().getName() + ":" + new Timestamp(System.currentTimeMillis()));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                if (null != lockId) {
                    manager.unlock(JedisManager.LOCK_KEY, lockId);
                }
            }
        }
    }
}
