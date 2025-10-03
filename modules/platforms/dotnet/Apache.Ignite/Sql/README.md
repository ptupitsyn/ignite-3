# ADO.NET Integration

Ignite implements [ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview) classes, such as `DbConnection`, `DbCommand`, `DbDataReader`, etc.,
allowing you to use standard ADO.NET components to interact with Ignite SQL.

## Connection String

Connection string parameters correspond to `IgniteClientConfiguration` properties. Full example:

```
Endpoints=localhost:10800,localhost:10801;SocketTimeout=00:00:10;OperationTimeout=00:03:30;
HeartbeatInterval=00:00:05.5;ReconnectInterval=00:01:00;SslEnabled=True;Username=user1;Password=hunter2
```

## Connection

To establish a connection to an Ignite cluster, use `IgniteDbConnection` class

```csharp
var connStr = "Endpoints=localhost:10800";
await using var conn = new IgniteDbConnection(connStr);
await conn.OpenAsync();
```

## Commands

Use `IgniteDbConnection.CreateCommand` method to create a command.

```csharp
DbCommand cmd = conn.CreateCommand();
cmd.CommandText = "DROP TABLE IF EXISTS Person";
await cmd.ExecuteNonQueryAsync();
```

## Data Reader

IgniteDbDataReader works similarly to other ADO.NET data readers.

```csharp
DbCommand cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM Person";
await using var reader = await cmd.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    Console.WriteLine($"Person [ID={reader.GetInt32(0)}, Name={reader.GetString(1)}]");
}
```

## Transactions

You can use `DbConnection.BeginTransaction` method to start a transaction.

```csharp
await using DbTransaction tx = await conn.BeginTransactionAsync();
cmd.Transaction = tx;
// ...
await tx.CommitAsync();
```

NOTE: Ignite does not support custom isolation levels. All transactions are effectively `Serializable`.

## Full Example

```csharp
var connStr = $"Endpoints=localhost:10800";
await using var conn = new IgniteDbConnection(connStr);
await conn.OpenAsync();

DbCommand createTableCmd = conn.CreateCommand();
createTableCmd.CommandText = "CREATE TABLE IF NOT EXISTS Person (ID INT PRIMARY KEY, Name VARCHAR)";
await createTableCmd.ExecuteNonQueryAsync();

DbCommand insertCmd = conn.CreateCommand();
insertCmd.CommandText = "INSERT INTO Person (ID, Name) VALUES (?, ?)";

await using DbTransaction tx = await conn.BeginTransactionAsync();
insertCmd.Transaction = tx;

DbParameter idParam = insertCmd.CreateParameter();
insertCmd.Parameters.Add(idParam);

DbParameter nameParam = insertCmd.CreateParameter();
insertCmd.Parameters.Add(nameParam);

for (var i = 1; i <= 3; i++)
{
    idParam.Value = i;
    nameParam.Value = "Person " + i;
    await insertCmd.ExecuteNonQueryAsync();
}

await tx.CommitAsync();

DbCommand selectCmd = conn.CreateCommand();
selectCmd.CommandText = "SELECT * FROM Person WHERE ID > ?";

DbParameter selectParam = selectCmd.CreateParameter();
selectParam.Value = 1;
selectCmd.Parameters.Add(selectParam);

await using var reader = await selectCmd.ExecuteReaderAsync();

for (var i = 0; i < reader.FieldCount; i++)
{
    Console.WriteLine($"{reader.GetName(i)}: {reader.GetFieldType(i)}");
}

while (await reader.ReadAsync())
{
    int id = reader.GetInt32(0);
    string name = reader.GetString(1);

    Console.WriteLine($"Person [ID={id}, Name={name}]");
}
```
