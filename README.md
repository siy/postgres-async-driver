# pgAsync - Asynchronous PostgreSQL Java driver

pgAsync is a non-blocking Java driver for PostgreSQL. The driver supports connection pooling, prepared statements, transactions, all standard SQL types and custom column types. 

## Install
The source dependency is available for this library. Add this in your `settings.gradle.kts` file:
```java
sourceControl {
    gitRepository(uri("https://github.com/marat-gainullin/postgres-async-driver.git")) {
        producesModule("com.github.pgasync:postgres-async-driver")
    }
}
```
Add the following dependency to your `build.gradle.kts` file:
```java
dependencies{
    implementation("com.github.pgasync:postgres-async-driver:v1.0.3")
}
```
## Usage
The library is compatible with any network framework like Grizzly, MINA, Netty, or any other, which works with `java.nio.ByteBuffer`. 
The following examples use `Netty` as a network infrastructure.

### SQL Sripts

```java
Connectible pool = new NettyConnectibleBuilder()
        .database(envOrDefault("PG_DATABASE", "postgres"))
        .username(envOrDefault("PG_USERNAME", "postgres"))
        .password(envOrDefault("PG_PASSWORD", "postgres"))
        .ssl(true)
        .maxConnections(size);
        .pool();
// ...        
        pool.completeScript(
                 "DROP TABLE IF EXISTS CP_TEST;" +
                 "CREATE TABLE CP_TEST (ID VARCHAR(255) PRIMARY KEY)"
        ).thenAccept(resultSets -> {
            resultSets.forEach(resultSet -> {
                // resultSet.
            });
        });
```

### SQL Queries
```java
Connectible pool = new NettyConnectibleBuilder()
        .database(envOrDefault("PG_DATABASE", "postgres"))
        .username(envOrDefault("PG_USERNAME", "postgres"))
        .password(envOrDefault("PG_PASSWORD", "postgres"))
        .ssl(true)
        .maxConnections(size);
        .pool();

// ...
pool.completeQuery("SELECT COUNT(*) cnt FROM CP_TEST").thenAccept(resultSet -> {
    // resultSet.size() // Here it is always 1
    Row resultItem = resultSet.at(0);
    int count = resultItem.getInt("cnt");
    // count ...
});

// ... 
pool.completeQuery("INSERT INTO CP_TEST VALUES($1)", singletonList(10)).thenAccept(resultSet -> {
    int inserted = resultSet.affectedRows();
    // inserted ...
});
```

### Transactions

A transactional unit of work is started with `begin()`.

```java
        pool.getConnection().thenAccept(connection ->
                connection.begin().thenAccept(transaction ->
                        transaction.completeQuery("SELECT COUNT(*) cnt FROM CP_TEST")
                                .thenApply(resultSet -> {
                                    Row resultItem = resultSet.at(0);
                                    return resultItem.getInt("cnt");
                                })
                                .thenApply(count -> transaction.completeQuery("Insert into cp_log (cnt) Values($1)", count))
                                .thenCompose(Function.identity())
                                .thenAccept(insertResult -> transaction.commit())
                )
        );
```

### Pretty Kotlin
To avoid using long chains of `CompleteableFuture.then*()` methods, use Kotlin programming language and its brilliant feature `suspend functions`. If you apply such trick like the following:
```java
suspend fun <T> CompletableFuture<T>.upon(): T {
    return suspendCoroutine { continuation ->
        whenComplete { r, th ->
            if (th == null) {
                continuation.resume(r)
            } else {
                continuation.resumeWithException(th)
            }
        }
    }
}
```
then you will be able to write your business logic in a sync-like style like this:
```java

    suspend fun fetchSomeData(
        userId: Long,
        since: Instant,
        till: Instant,
        open: Boolean
    ): List<SomeDatum> = postgresClient.query(
        databaseAt, "Select * from some_table",
        userId, since, till
    ).upon()
        .map(SomeDatum::of)
        .filter { datum -> datum.closed || open }
```
This code is written in `Kotlin` and it operates on the results directly because `upon()` function turns a `CompleteableFuture` in a suspension point.
With this trick you write your code like it would be synchronous, but it remains fully asynchronous and non-blocking! 