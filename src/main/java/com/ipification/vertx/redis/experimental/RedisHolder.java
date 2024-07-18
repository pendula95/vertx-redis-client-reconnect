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

package com.ipification.vertx.redis.experimental;

import com.ipification.vertx.redis.rx.RetryWithDelay;
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
    private final String name;
    private final AtomicBoolean CONNECTING = new AtomicBoolean();

    public RedisHolder(Vertx vertx, RedisOptions redisOptions, String name) {
        this.vertx = vertx;
        this.name = name;
        this.redisOptions = redisOptions;
        this.redis = Redis.createClient(vertx, redisOptions);
    }

    public Completable rxConnect() {
        return redis.rxConnect().ignoreElement();
    }

    public Maybe<Response> rxSend(Request request) {
        return Maybe.defer(() -> redis.rxSend(request))
                .timeout(1000, TimeUnit.MILLISECONDS)
                .retryWhen(new RetryWithDelay(2000, 100));
    }
}
