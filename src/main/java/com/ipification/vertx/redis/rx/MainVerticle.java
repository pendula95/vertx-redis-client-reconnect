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
import io.vertx.core.Promise;
import io.vertx.core.net.NetClientOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.redis.client.Command;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.vertx.reactivex.redis.client.Request.cmd;

public class MainVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        NetClientOptions netClientOptions = new NetClientOptions()
                .setTcpKeepAlive(true)
                .setTcpNoDelay(true)
                .setConnectTimeout(1000);

        RedisOptions redisOptions = new RedisOptions()
                .setMaxPoolSize(32)
                .setMaxPoolWaiting(32 * 2)
                .setMaxWaitingHandlers(128 * 1024)
                .setNetClientOptions(netClientOptions)
                .setPoolCleanerInterval(1000)
                //.setPoolRecycleTimeout(10000)
                .setType(RedisClientType.CLUSTER);

        RedisHolder redisHolderTraffic = new RedisHolder(vertx, new RedisOptions(redisOptions)
                .addConnectionString("redis://127.0.0.1:7001")
                .setPassword("uh5ohNoo"),
                "TRAFFIC");

        RedisHolder redisHolderOperation = new RedisHolder(vertx, new RedisOptions(redisOptions)
                .addConnectionString("redis://127.0.0.1:8001")
                .setPassword("YaTeiW1y"),
                "OPERATION");

        redisHolderTraffic.rxConnect()
                .andThen(redisHolderOperation.rxConnect())
                .andThen(Completable.defer(() -> {
                    return vertx.createHttpServer()
                            .requestHandler(request -> {
                                String ip = request.getParam("ip");
                                String port = request.getParam("port");
                                redisHolderTraffic.rxSend(cmd(Command.GET).arg(ip + ":" + port))
                                        .flatMap(response -> {
                                            return redisHolderTraffic.rxSend(cmd(Command.GET).arg(response.toString()));
                                        })
                                        .subscribe(response -> {
                                            request.response().setStatusCode(200).end(response.toString());
                                        }, error -> {
                                            request.response().setStatusCode(500).end(error.getClass().getName());
                                        }, () -> request.response().setStatusCode(404).end());
                            })
                            .rxListen(10111, "0.0.0.0")
                            .ignoreElement();
                }))
                .doOnComplete(() -> {
                    vertx.setPeriodic(500, id -> {
                        long currentTimeMillis = System.currentTimeMillis();
                        redisHolderOperation.rxSend(cmd(Command.SETEX).arg(currentTimeMillis).arg(5).arg(currentTimeMillis))
                                .flatMap(v -> redisHolderOperation.rxSend(cmd(Command.GET).arg(currentTimeMillis)))
                                .ignoreElement()
                                .subscribe(() -> {}, error -> {});
                    });
                    vertx.setPeriodic(1000, id -> {
                        redisHolderOperation.rxSend(cmd(Command.GET).arg(System.currentTimeMillis()))
                                .ignoreElement()
                                .subscribe(() -> {}, error -> {});
                    });
                }).subscribe(() -> {
                    logger.info("Deployed verticle completed");
                    startPromise.complete();
                }, startPromise::fail);

    }

    public static void main(String[] args) {
        // Deploying the MainVerticle
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new MainVerticle());
    }
}
