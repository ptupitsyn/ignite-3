#pragma warning disable SA1405

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Apache.Ignite;
using Apache.Ignite.Network;
using Apache.Ignite.Sql;
using Apache.Ignite.Table;
using Apache.Ignite.Transactions;

var client = await IgniteClient.StartAsync(new IgniteClientConfiguration("127.0.0.1:10942"));
var table = (await client.Tables.GetTableAsync("TBL1"))!;

// 1. Record binary view.
{
    IRecordView<IIgniteTuple> view = table.RecordBinaryView;

    IIgniteTuple fullRecord = new IgniteTuple
    {
        ["id"] = 42,
        ["name"] = "John Doe"
    };

    await view.UpsertAsync(transaction: null, fullRecord);

    IIgniteTuple keyRecord = new IgniteTuple { ["id"] = 42 };
    (IIgniteTuple value, bool hasValue) = await view.GetAsync(transaction: null, keyRecord);

    Debug.Assert(hasValue);
    Debug.Assert(value.FieldCount == 2);
    Debug.Assert(value["id"] as int? == 42);
    Debug.Assert(value["name"] as string == "John Doe");
}

// 2. Record view.
{
    var pocoView = table.GetRecordView<Poco>();

    await pocoView.UpsertAsync(transaction: null, new Poco(42, "John Doe"));
    (Poco? value, bool hasValue) = await pocoView.GetAsync(transaction: null, new Poco(42));

    Debug.Assert(hasValue);
    Debug.Assert(value.Name == "John Doe");
}

// 3. KV Binary View.
{
    IKeyValueView<IIgniteTuple, IIgniteTuple> kvView = table.KeyValueBinaryView;

    IIgniteTuple key = new IgniteTuple { ["id"] = 42 };
    IIgniteTuple val = new IgniteTuple { ["name"] = "John Doe" };

    await kvView.PutAsync(transaction: null, key, val);
    (IIgniteTuple? value, bool hasValue) = await kvView.GetAsync(transaction: null, key);

    Debug.Assert(hasValue);
    Debug.Assert(value.FieldCount == 1);
    Debug.Assert(value["name"] as string == "John Doe");
}

// 4. KV View
{
    IKeyValueView<long, Poco> kvView = table.GetKeyValueView<long, Poco>();

    await kvView.PutAsync(transaction: null, 42, new Poco(Id: 0, Name: "John Doe"));
    (Poco? value, bool hasValue) = await kvView.GetAsync(transaction: null, 42);

    Debug.Assert(hasValue);
    Debug.Assert(value.Name == "John Doe");
}

// 5. SQL
{
    IResultSet<IIgniteTuple> resultSet = await client.Sql.ExecuteAsync(transaction: null, "select name from tbl where id = ?", 42);
    List<IIgniteTuple> rows = await resultSet.ToListAsync();
    IIgniteTuple row = rows.Single();
    Debug.Assert(row["name"] as string == "John Doe");
}

// 6. Transactions
{
    var accounts = table.GetKeyValueView<long, Account>();
    await accounts.PutAsync(transaction: null, 42, new Account(16_000));

    await using ITransaction tx = await client.Transactions.BeginAsync();

    (Account account, bool hasValue) = await accounts.GetAsync(tx, 42);
    account = account with { Balance = account.Balance + 500 };

    await accounts.PutAsync(tx, 42, account);

    Debug.Assert((await accounts.GetAsync(tx, 42)).Value.Balance == 16_500);

    await tx.RollbackAsync();

    Debug.Assert((await accounts.GetAsync(null, 42)).Value.Balance == 16_000);
}

// 7. Compute
{
    IList<IClusterNode> nodes = await client.GetClusterNodesAsync();
    string res = await client.Compute.ExecuteAsync<string>(nodes, "org.foo.bar.Job", 42)
}

public record Poco(long Id, string? Name = null);

public record Account(decimal Balance);
