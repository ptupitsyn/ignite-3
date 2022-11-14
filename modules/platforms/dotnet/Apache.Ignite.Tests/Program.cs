#pragma warning disable SA1405

using System.Diagnostics;
using Apache.Ignite;
using Apache.Ignite.Table;

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

public record Poco(long Id, string? Name = null);
