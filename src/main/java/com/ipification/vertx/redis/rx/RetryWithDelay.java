package com.ipification.vertx.redis.rx;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RetryWithDelay implements Function<Flowable<? extends Throwable>, Publisher<Object>> {

    private static final Logger logger = LoggerFactory.getLogger(RetryWithDelay.class);

    private int retryCount;
    private long lastRetry;
    private final long startTime;
    private final long minTimeout;
    private final long retryDuration;

    public RetryWithDelay(long retryDuration, long minTimeout) {
        this.startTime = System.currentTimeMillis();
        this.retryDuration = retryDuration;
        this.minTimeout = minTimeout;
    }

    @Override
    public Publisher<Object> apply(Flowable<? extends Throwable> attempts) throws Exception {
        return attempts
                .concatMap((Function<Throwable, Flowable<?>>) throwable -> {
                    if (System.currentTimeMillis() - startTime < retryDuration) {
                        //logger.warn("Retry = {} time for error = {}", retryCount++, throwable.getMessage());
                        // When this Observable calls onNext, the original
                        // Observable will be retried (i.e. re-subscribed).
                        lastRetry = System.currentTimeMillis();
                        return Flowable.timer(minTimeout,
                                TimeUnit.MILLISECONDS);
                    }
                    /* Max retries hit. Just pass the error along. */
                    //logger.error("Could not recover from exception = {} error = {} after retries = {}", throwable.getClass().getName(), throwable.getMessage(), retryCount);
                    return Flowable.error(throwable);
                });
    }
}
