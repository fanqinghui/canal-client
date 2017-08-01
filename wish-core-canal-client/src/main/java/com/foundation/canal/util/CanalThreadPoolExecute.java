package com.foundation.canal.util;

import org.slf4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 针对线程池的封装，解决线程池无法线上异常问题
 * @author fqh
 * @email fanqinghui100@126.com
 * @date 2017/8/1 21:55
 */
public class CanalThreadPoolExecute extends ThreadPoolExecutor {
    Logger logger = org.slf4j.LoggerFactory.getLogger(CanalThreadPoolExecute.class);

    /**
     * Creates a new {@code ThreadPoolExecutor} with the given initial
     * parameters and default thread factory and rejected execution handler.
     * It may be more convenient to use one of the {@link Executors} factory
     * methods instead of this general purpose constructor.
     *
     * @param corePoolSize    the number of threads to keep in the pool, even
     *                        if they are idle, unless {@code allowCoreThreadTimeOut} is set
     * @param maximumPoolSize the maximum number of threads to allow in the
     *                        pool
     * @param keepAliveTime   when the number of threads is greater than
     *                        the core, this is the maximum time that excess idle threads
     *                        will wait for new tasks before terminating.
     * @param unit            the time unit for the {@code keepAliveTime} argument
     * @param workQueue       the queue to use for holding tasks before they are
     *                        executed.  This queue will hold only the {@code Runnable}
     *                        tasks submitted by the {@code execute} method.
     * @throws IllegalArgumentException if one of the following holds:<br>
     *                                  {@code corePoolSize < 0}<br>
     *                                  {@code keepAliveTime < 0}<br>
     *                                  {@code maximumPoolSize <= 0}<br>
     *                                  {@code maximumPoolSize < corePoolSize}
     * @throws NullPointerException     if {@code workQueue} is null
     */
    public CanalThreadPoolExecute(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }


    /**
     * Executes the given task sometime in the future.  The task
     * may execute in a new thread or in an existing pooled thread.
     * <p>
     * If the task cannot be submitted for execution, either because this
     * executor has been shutdown or because its capacity has been reached,
     * the task is handled by the current {@code RejectedExecutionHandler}.
     *
     * @param command the task to execute
     * @throws NullPointerException       if {@code command} is null
     */
    @Override
    public void execute(Runnable command) {
        super.execute(wrap(command,canalTrace(),Thread.currentThread().getName()));
    }

    /**
     * @param task
     * @throws NullPointerException       {@inheritDoc}
     */
    @Override
    public Future<?> submit(Runnable task) {
        return super.submit(wrap(task,canalTrace(),Thread.currentThread().getName()));
    }

    private Exception canalTrace(){
        return new Exception("canal stack trace");
    }

    /**
     * 核心处理任务，拦截异常信息的
     * @param task
     * @param canalStack
     * @param threadName
     * @return
     */
    private Runnable wrap(final Runnable task, final Exception canalStack, String threadName) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    task.run();
                } catch (Exception e) {
                    logger.error("canal-client run error:",e);
                    canalStack.printStackTrace();
                    throw e;
                }
            }
        };
    }
}
