/*
 * Copyright (c) 2024. Lazar Bulic lazarbulic@gmail.com
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ipification.vertx.redis.rx;

import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.redis.client.*;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.vertx.reactivex.redis.client.Request.cmd;

public class RedisHolder {

    private static final Logger logger = LoggerFactory.getLogger(RedisHolder.class);

    private final Vertx vertx;
    private final Redis redis;
    private final RedisOptions redisOptions;
    private RedisConnection redisConnection;
    private final String name;
    private final AtomicBoolean CONNECTING = new AtomicBoolean();

    public RedisHolder(Vertx vertx, RedisOptions redisOptions, String name) {
        this.vertx = vertx;
        this.redisOptions = redisOptions;
        this.name = name;
        this.redis = Redis.createClient(vertx, redisOptions);
    }

    public Completable rxConnect() {
        if (redisConnection == null || CONNECTING.get()) {
            return redis.rxConnect()
                    .doOnSuccess(connection -> {
                        if (redisConnection != null) {
                            redisConnection.close();
                        }
                        redisConnection = connection;
                        /*connection.exceptionHandler(ex -> {
                            if (CONNECTING.compareAndSet(false, true)) {
                                attemptReconnect(0);
                            }
                        });*/
                        CONNECTING.set(false);
                    })
                    .flatMapCompletable(ignore -> {
                        if (RedisClientType.CLUSTER.equals(redisOptions.getType())) {
                            return redisConnection.rxSend(cmd(Command.CLUSTER).arg("NODES"))
                                    .doOnSuccess(result -> {
                                        String topology = result.toString();
                                        String[] nodes = topology.split("\n");
                                        redisOptions.setEndpoints(null);
                                        for (String node : nodes) {
                                            String connectionUrl = node.split(" ")[1].split("@")[0];
                                            //logger.info("[{}] Adding node to connection options = {}", name, connectionUrl);
                                            redisOptions.addConnectionString("redis://" + connectionUrl);
                                        }
                                    }).ignoreElement();
                        } else {
                            return Completable.complete();
                        }
                    });
        } else {
            return Completable.error(new RuntimeException("RedisConnectionWrapper already connected to Redis, will not create new connection"));
        }
    }

    public Maybe<Response> rxSend(Request request) {
        return Maybe.defer(() -> redisConnection.rxSend(request))
                .timeout(1000, TimeUnit.MILLISECONDS)
                .doOnError(ex -> {
                    if (CONNECTING.compareAndSet(false, true)) {
                        attemptReconnect(0);
                    }
                }).retryWhen(new RetryWithDelay(2000, 100));
    }

    private void attemptReconnect(int retry) {
        long backoff = (long) (Math.pow(2, Math.min(retry, 10)) * 10);
        logger.info("[{}] - Attempting to reconnect {}", name, retry);
        vertx.setTimer(backoff, timer -> {
            rxConnect().doOnError(t -> {
                        logger.warn("[{}] - Failed to reconnect, attempt {}", name, retry);
                        attemptReconnect(retry + 1);
                    })
                    .doOnComplete(() -> logger.info("[{}] - Successfully Reconnected after {} times", name, retry))
                    .subscribe(() -> {}, error -> {});
        });
    }
}
