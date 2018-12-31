/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pgasync.impl;

import com.github.pgasync.*;
import com.github.pgasync.ConnectionPoolBuilder.PoolProperties;
import com.github.pgasync.impl.conversion.DataConverter;

import javax.annotation.concurrent.GuardedBy;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Pool for backend connections. Callbacks are queued and executed when pool has an available
 * connection.
 *
 * @author Antti Laisi
 */
public abstract class PgConnectionPool implements ConnectionPool {

    final int maxSize;
    final ReentrantLock lock = new ReentrantLock();
    @GuardedBy("lock")
    int size;
    @GuardedBy("lock")
    boolean closed;
    @GuardedBy("lock")
    final Queue<CompletableFuture<? super Connection>> subscribers = new LinkedList<>();
    @GuardedBy("lock")
    final Queue<Connection> connections = new LinkedList<>();

    final InetSocketAddress address;
    final String username;
    final String password;
    final String database;
    final DataConverter dataConverter;
    final boolean pipeline;

    public PgConnectionPool(PoolProperties properties) {
        this.address = InetSocketAddress.createUnresolved(properties.getHostname(), properties.getPort());
        this.username = properties.getUsername();
        this.password = properties.getPassword();
        this.database = properties.getDatabase();
        this.maxSize = properties.getPoolSize();
        this.dataConverter = properties.getDataConverter();
        this.pipeline = properties.getUsePipelining();
    }

    @Override
    public void close() throws Exception {
        lock.lock();
        try {
            closed = true;
            while (!subscribers.isEmpty()) {
                CompletableFuture<? super Connection> queued = subscribers.poll();
                queued.completeExceptionally(new SqlException("Connection pool is closing"));
            }
            while (!connections.isEmpty()) {
                Connection connection = connections.poll();
                connection.close();
                size--;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletableFuture<Connection> getConnection() {
        CompletableFuture<Connection> uponAvailable = new CompletableFuture<>();

        lock.lock();
        try {
            if (closed) {
                uponAvailable.completeExceptionally(new SqlException("Connection pool is closed"));
            } else {
                Connection connection = connections.poll();
                if (connection != null) {
                    uponAvailable.complete(connection);
                } else {
                    if (tryIncreaseSize()) {
                        new PgConnection(openStream(address), dataConverter)
                                .connect(username, password, database)
                                .thenAccept(uponAvailable::complete)
                                .exceptionally(th -> {
                                    uponAvailable.completeExceptionally(th);
                                    return null;
                                });
                    } else {
                        // Pool is full now and all connections are busy
                        subscribers.offer(uponAvailable);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        return uponAvailable;
    }

    private boolean tryIncreaseSize() {
        if (size < maxSize) {
            size++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void release(Connection connection) {
        boolean failed = connection == null || !((PgConnection) connection).isConnected();
        lock.lock();
        try {
            if (closed) {
                if (!failed) {
                    connection.close();
                }
            } else {
                if (failed) {
                    size--;
                } else {
                    if (!subscribers.isEmpty()) {
                        subscribers.poll()
                                .complete(connection);
                    } else {
                        connections.add(connection);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletableFuture<Transaction> begin() {
        return getConnection()
                .thenApply(Connection::begin)
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<Row> queryRows(String sql, Object... params) {
        return getConnection()
                .thenApply(connection -> connection.queryRows(sql, params))
                .thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<ResultSet> querySet(String sql, Object... params) {
        return getConnection()
                .thenApply(connection -> connection.querySet(sql, params))
                .thenCompose(Function.identity());
    }

    /**
     * Creates a new socket stream to the backend.
     *
     * @param address Server address
     * @return Stream with no pending messages
     */
    protected abstract PgProtocolStream openStream(InetSocketAddress address);

    /**
     * Transaction that chains releasing the connection after COMMIT/ROLLBACK.
     */
    class ReleasingTransaction implements Transaction {

        final AtomicBoolean released = new AtomicBoolean();
        final Connection connection;
        final Transaction transaction;

        ReleasingTransaction(Connection connection, Transaction transaction) {
            this.connection = connection;
            this.transaction = transaction;
        }

        @Override
        public CompletableFuture<Transaction> begin() {
            // Nested transactions should not release things automatically.
            return transaction.begin();
        }

        @Override
        public CompletableFuture<Void> rollback() {
            return transaction.rollback()
                    .thenAccept(v -> ReleasingTransaction.this.releaseConnection());
        }

        @Override
        public CompletableFuture<Void> commit() {
            return transaction.commit()
                    .thenAccept(v -> ReleasingTransaction.this.releaseConnection());
        }

        @Override
        public CompletableFuture<Row> queryRows(String sql, Object... params) {
            if (released.get()) {
                return CompletableFuture.failedFuture(new SqlException("Transaction is already completed"));
            } else {
                return transaction.queryRows(sql, params)
                        .exceptionally(th -> {
                            releaseConnection();
                            throw new IllegalStateException(th);
                        });
            }
        }

        @Override
        public CompletableFuture<ResultSet> querySet(String sql, Object... params) {
            if (released.get()) {
                return CompletableFuture.failedFuture(new SqlException("Transaction is already completed"));
            } else {
                return transaction.querySet(sql, params)
                        .exceptionally(th -> {
                            releaseConnection();
                            throw new IllegalStateException(th);
                        });
            }
        }

        void releaseConnection() {
            release(connection);
            released.set(true);
        }
    }
}
