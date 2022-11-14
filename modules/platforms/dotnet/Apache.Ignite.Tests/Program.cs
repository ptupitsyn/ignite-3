#pragma warning disable SA1405

using System.Diagnostics;
using Apache.Ignite;
using Apache.Ignite.Table;

var client = await IgniteClient.StartAsync(new IgniteClientConfiguration("127.0.0.1:10942"));
var table = await client.Tables.GetTableAsync("TBL1");

IRecordView<IIgniteTuple> view = table!.RecordBinaryView;

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
