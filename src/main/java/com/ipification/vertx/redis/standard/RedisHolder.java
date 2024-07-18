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

package com.ipification.vertx.redis.standard;

import com.ipification.vertx.redis.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.redis.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.vertx.redis.client.Request.cmd;

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

    public Future<Void> connect() {
        if (redisConnection == null || CONNECTING.get()) {
            return redis.connect()
                    .onSuccess(connection -> {
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
                    .compose(ignore -> {
                        if (RedisClientType.CLUSTER.equals(redisOptions.getType())) {
                            return redisConnection.send(cmd(Command.CLUSTER).arg("NODES"))
                                    .onSuccess(result -> {
                                        String topology = result.toString();
                                        String[] nodes = topology.split("\n");
                                        redisOptions.setEndpoints(null);
                                        for (String node : nodes) {
                                            String connectionUrl = node.split(" ")[1].split("@")[0];
                                            //logger.info("[{}] Adding node to connection options = {}", name, connectionUrl);
                                            redisOptions.addConnectionString("redis://" + connectionUrl);
                                        }
                                    }).mapEmpty();
                        } else {
                            return Future.succeededFuture();
                        }
                    });
        } else {
            return Future.failedFuture(new RuntimeException("RedisConnectionWrapper already connected to Redis, will not create new connection"));
        }
    }

    public Future<Response> send(Request request) {
        Promise<Response> result = Promise.promise();

        sendWithRetry(request, 0)
                .onComplete(result);

        //timeout handler
        long timerId = vertx.setTimer(2000, l -> {
            if (!result.future().isComplete()) {
                result.fail(TimeoutException.INSTANCE);
            }
        });

        result.future().onComplete(v -> vertx.cancelTimer(timerId));

        return result.future();
    }

    private Future<Response> sendWithRetry(Request request, int retry) {
        Promise<Response> result = Promise.promise();

        redisConnection.send(request)
                .timeout(1000, TimeUnit.MILLISECONDS)
                .onFailure(exception -> {
                    if (CONNECTING.compareAndSet(false, true)) {
                        attemptReconnect(0);
                    }
                })
                .recover(t -> {
                    Promise<Response> recoverPromis = Promise.promise();
                    vertx.setTimer(100, timer -> {
                        sendWithRetry(request, retry + 1)
                                .onComplete(recoverPromis);
                    });
                    return recoverPromis.future();
                })
                .onComplete(result);

        return result.future();
    }

    private void attemptReconnect(int retry) {
        long backoff = (long) (Math.pow(2, Math.min(retry, 10)) * 10);
        logger.info("[{}] - Attempting to reconnect {}", name, retry);
        vertx.setTimer(backoff, timer -> {
            connect().onFailure(t -> {
                        logger.warn("[{}] - Failed to reconnect, attempt {}", name, retry);
                        attemptReconnect(retry + 1);
                    })
                    .onSuccess(v -> logger.info("[{}] - Successfully Reconnected after {} times", name, retry));
        });
    }
}
