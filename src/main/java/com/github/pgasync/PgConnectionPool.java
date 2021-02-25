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

package com.github.pgasync;

import com.pgasync.ConnectibleBuilder;
import com.pgasync.Connection;
import com.pgasync.Listening;
import com.pgasync.PreparedStatement;
import com.pgasync.Row;
import com.pgasync.SqlException;
import com.pgasync.ResultSet;
import com.pgasync.Transaction;

import javax.annotation.concurrent.GuardedBy;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Resource pool for backend connections.
 *
 * @author Antti Laisi
 */
public class PgConnectionPool extends PgConnectible {

    private class PooledPgConnection implements Connection {

        private class PooledPgTransaction implements Transaction {

            private final Transaction delegate;

            PooledPgTransaction(Transaction delegate) {
                this.delegate = delegate;
            }

            public CompletableFuture<Void> commit() {
                return delegate.commit();
            }

            public CompletableFuture<Void> rollback() {
                return delegate.rollback();
            }

            public CompletableFuture<Void> close() {
                return delegate.close();
            }

            public CompletableFuture<Transaction> begin() {
                return delegate.begin().thenApply(PooledPgTransaction::new);
            }

            @Override
            public CompletableFuture<Integer> query(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, String sql, Object... params) {
                return delegate.query(onColumns, onRow, sql, params);
            }

            @Override
            public CompletableFuture<Void> script(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
                return delegate.script(onColumns, onRow, onAffected, sql);
            }

            public Connection getConnection() {
                return PooledPgConnection.this;
            }
        }

        private final PgConnection delegate;
        private PooledPgPreparedStatement evicted;
        private final LinkedHashMap<String, PooledPgPreparedStatement> statements = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, PooledPgPreparedStatement> eldest) {
                if (size() > maxStatements) {
                    evicted = eldest.getValue();
                    return true;
                } else {
                    return false;
                }
            }
        };

        PooledPgConnection(PgConnection delegate) {
            this.delegate = delegate;
        }

        CompletableFuture<Connection> connect(String username, String password, String database) {
            return delegate.connect(username, password, database).thenApply(conn -> PooledPgConnection.this);
        }

        public boolean isConnected() {
            return delegate.isConnected();
        }

        private void closeNextStatement(Iterator<PooledPgPreparedStatement> statementsSource, CompletableFuture<Void> onComplete) {
            if (statementsSource.hasNext()) {
                statementsSource.next().delegate.close()
                        .thenAccept(v -> {
                            statementsSource.remove();
                            closeNextStatement(statementsSource, onComplete);
                        })
                        .exceptionally(th -> {
                            futuresExecutor.execute(() -> onComplete.completeExceptionally(th));
                            return null;
                        });
            } else {
                futuresExecutor.execute(() -> onComplete.complete(null));
            }
        }

        CompletableFuture<Void> shutdown() {
            CompletableFuture<Void> onComplete = new CompletableFuture<>();
            closeNextStatement(statements.values().iterator(), onComplete);
            return onComplete
                    .thenApply(v -> {
                        if (!statements.isEmpty()) {
                            throw new IllegalStateException("Stale prepared statements detected (" + statements.size() + ")");
                        }
                        return delegate.close();
                    })
                    .thenCompose(Function.identity());
        }

        @Override
        public CompletableFuture<Void> close() {
            return release(this);
        }

        @Override
        public CompletableFuture<Listening> subscribe(String channel, Consumer<String> onNotification) {
            return delegate.subscribe(channel, onNotification);
        }

        @Override
        public CompletableFuture<Transaction> begin() {
            return delegate.begin().thenApply(PooledPgTransaction::new);
        }

        @Override
        public CompletableFuture<Void> script(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, Consumer<Integer> onAffected, String sql) {
            return delegate.script(onColumns, onRow, onAffected, sql);
        }

        @Override
        public CompletableFuture<Integer> query(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> onRow, String sql, Object... params) {
            return prepareStatement(sql, dataConverter.assumeTypes(params))
                    .thenApply(stmt ->
                            stmt.fetch(onColumns, onRow, params)
                                    .handle((affected, th) ->
                                            stmt.close()
                                                    .thenApply(v -> {
                                                        if (th == null) {
                                                            return affected;
                                                        } else {
                                                            throw new RuntimeException(th);
                                                        }
                                                    })
                                    ).thenCompose(Function.identity())
                    ).thenCompose(Function.identity());
        }

        @Override
        public CompletableFuture<PreparedStatement> prepareStatement(String sql, Oid... parametersTypes) {
            PooledPgPreparedStatement statement = statements.remove(sql);
            if (statement != null) {
                return CompletableFuture.completedFuture(statement);
            } else {
                return delegate.preparedStatementOf(sql, parametersTypes)
                        .thenApply(stmt -> new PooledPgPreparedStatement(sql, stmt));
            }
        }

        private class PooledPgPreparedStatement implements PreparedStatement {

            private static final String DUPLICATED_PREPARED_STATEMENT_DETECTED = "Duplicated prepared statement detected. Closing extra instance. \n{0}";
            private final String sql;
            private final PgConnection.PgPreparedStatement delegate;

            private PooledPgPreparedStatement(String sql, PgConnection.PgPreparedStatement delegate) {
                this.sql = sql;
                this.delegate = delegate;
            }

            @Override
            public CompletableFuture<Void> close() {
                PooledPgPreparedStatement already = statements.put(sql, this);
                if (evicted != null) {
                    try {
                        if (already != null && already != evicted) {
                            Logger.getLogger(PgConnectionPool.class.getName()).log(Level.WARNING, DUPLICATED_PREPARED_STATEMENT_DETECTED, already.sql);
                            return evicted.delegate.close()
                                    .thenApply(v -> already.delegate.close())
                                    .thenCompose(Function.identity());
                        } else {
                            return evicted.delegate.close();
                        }
                    } finally {
                        evicted = null;
                    }
                } else {
                    if (already != null) {
                        Logger.getLogger(PgConnectionPool.class.getName()).log(Level.WARNING, DUPLICATED_PREPARED_STATEMENT_DETECTED, already.sql);
                        return already.delegate.close();
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }
            }

            @Override
            public CompletableFuture<ResultSet> query(Object... params) {
                return delegate.query(params);
            }

            @Override
            public CompletableFuture<Integer> fetch(BiConsumer<Map<String, PgColumn>, PgColumn[]> onColumns, Consumer<Row> processor, Object... params) {
                return delegate.fetch(onColumns, processor, params);
            }
        }
    }

    private final int maxConnections;
    private final int maxStatements;

    private final Lock guard = new ReentrantLock();
    @GuardedBy("guard")
    private int size;
    @GuardedBy("guard")
    private boolean closed;
    @GuardedBy("guard")
    private final Queue<CompletableFuture<? super Connection>> uponAvailableSubscribers = new LinkedList<>();
    @GuardedBy("guard")
    private final Queue<PooledPgConnection> availableConnections = new LinkedList<>();
    @GuardedBy("guard")
    private CompletableFuture<Void> uponFullyAvailable;
    @GuardedBy("guard")
    private final List<Runnable> executed = new ArrayList<>(32);

    public PgConnectionPool(ConnectibleBuilder.ConnectibleProperties properties, Supplier<CompletableFuture<ProtocolStream>> obtainStream, Executor futuresExecutor) {
        super(properties, obtainStream, futuresExecutor);
        this.maxConnections = properties.getMaxConnections();
        this.maxStatements = properties.getMaxStatements();
    }

    private <T> T locked(Supplier<T> action) {
        Runnable[] copy = new Runnable[]{};
        guard.lock();
        try {
            return action.get();
        } finally {
            try {
                copy = executed.toArray(copy);
                executed.clear();
            } finally {
                guard.unlock();
            }
            execute(copy);
        }
    }

    private void locked(Runnable action) {
        Runnable[] copy = new Runnable[]{};
        guard.lock();
        try {
            action.run();
        } finally {
            try {
                copy = executed.toArray(copy);
                executed.clear();
            } finally {
                guard.unlock();
            }
            execute(copy);
        }
    }

    private void execute(Runnable[] targets) {
        for (Runnable r : targets) {
            try {
                futuresExecutor.execute(r);
            } catch (Throwable th) { // Catch here because of simple executors like (r -> { r.run(); })
                Logger.getLogger(PgConnectionPool.class.getName()).log(Level.SEVERE, th.getMessage(), th);
            }
        }
    }

    private void toExecute(Runnable r) {
        executed.add(r);
    }

    private CompletableFuture<Void> fullyAvailable() {
        if (uponFullyAvailable == null) {
            if (size <= availableConnections.size()) {
                return CompletableFuture.completedFuture(null);
            } else {
                uponFullyAvailable = new CompletableFuture<>();
                return uponFullyAvailable;
            }
        } else {
            return CompletableFuture.failedFuture(new IllegalStateException("Only a single 'fullyAvailable' request at a time is supported"));
        }
    }

    private void discardAvailableSubscribers(String reason) {
        while (!uponAvailableSubscribers.isEmpty()) {
            CompletableFuture<? super Connection> queued = uponAvailableSubscribers.poll();
            toExecute(() -> queued.completeExceptionally(new SqlException(reason)));
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        return locked(() -> {
            closed = true;
            discardAvailableSubscribers("Connection pool is closing");
            return fullyAvailable()
                    .thenApply(v -> locked(() -> {
                        uponFullyAvailable = null;
                        Collection<CompletableFuture<Void>> shutdownTasks = new ArrayList<>();
                        while (!availableConnections.isEmpty()) {
                            PooledPgConnection connection = availableConnections.poll();
                            shutdownTasks.add(connection.shutdown());
                            size--;
                        }
                        return CompletableFuture.allOf(shutdownTasks.toArray(CompletableFuture<?>[]::new));
                    }))
                    .thenCompose(Function.identity());
        });
    }

    @Override
    public CompletableFuture<Connection> getConnection() {
        return locked(() -> {
            CompletableFuture<Connection> uponAvailable = new CompletableFuture<>();
            if (closed) {
                toExecute(() -> uponAvailable.completeExceptionally(new SqlException("Connection pool is closed")));
            } else {
                Connection connection = firstAliveConnection();
                if (connection != null) {
                    toExecute(() -> uponAvailable.complete(connection));
                } else {
                    if (tryIncreaseSize()) {
                        obtainStream.get()
                                .thenApply(stream -> new PooledPgConnection(new PgConnection(stream, dataConverter))
                                        .connect(username, password, database))
                                .thenCompose(Function.identity())
                                .thenApply(pooledConnection -> {
                                    if (validationQuery != null && !validationQuery.isBlank()) {
                                        return pooledConnection.completeScript(validationQuery)
                                                .handle((rss, th) -> {
                                                    if (th != null) {
                                                        return ((PooledPgConnection) pooledConnection).delegate.close()
                                                                .thenApply(v -> CompletableFuture.<Connection>failedFuture(th))
                                                                .thenCompose(Function.identity());
                                                    } else {
                                                        return CompletableFuture.completedFuture(pooledConnection);
                                                    }
                                                })
                                                .thenCompose(Function.identity());
                                    } else {
                                        return CompletableFuture.completedFuture(pooledConnection);
                                    }
                                })
                                .thenCompose(Function.identity())
                                .thenAccept(pooledConnection -> locked(() -> toExecute(() -> uponAvailable.complete(pooledConnection))))
                                .exceptionally(th -> locked(() -> {
                                    size--;
                                    toExecute(() -> uponAvailable.completeExceptionally(th));
                                    discardAvailableSubscribers("Unable to connect");
                                    checkFullyAvailable();
                                    return null;
                                }));
                    } else {
                        // Pool is full now and all connections are busy
                        uponAvailableSubscribers.offer(uponAvailable);
                    }
                }
            }
            return uponAvailable;
        });
    }

    private void checkFullyAvailable() {
        if (uponFullyAvailable != null && size <= availableConnections.size()) {
            toExecute(() -> uponFullyAvailable.complete(null));
        }
    }

    private Connection firstAliveConnection() {
        Connection connection = availableConnections.poll();
        while (connection != null && !connection.isConnected()) {
            size--;
            connection = availableConnections.poll();
        }
        return connection;
    }

    private boolean tryIncreaseSize() {
        if (size < maxConnections) {
            size++;
            return true;
        } else {
            return false;
        }
    }

    private CompletableFuture<Void> release(PooledPgConnection connection) {
        if (connection == null) {
            throw new IllegalArgumentException("'connection' should be not null");
        }
        return locked(() -> {
            if (connection.isConnected()) {
                if (!uponAvailableSubscribers.isEmpty()) {
                    CompletableFuture<? super Connection> subscriber = uponAvailableSubscribers.poll();
                    toExecute(() -> subscriber.complete(connection));
                } else {
                    availableConnections.offer(connection);
                    checkFullyAvailable();
                }
            } else {
                size--;
                discardAvailableSubscribers("Connection lost");
                checkFullyAvailable();
            }
            return CompletableFuture.completedFuture(null);
        });
    }
}
