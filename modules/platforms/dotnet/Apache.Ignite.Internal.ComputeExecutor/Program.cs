/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Net.Security;
using Apache.Ignite;
using Apache.Ignite.Marshalling;
using Apache.Ignite.Table;
using Microsoft.Extensions.Logging;

const string envServerAddr = "IGNITE_COMPUTE_EXECUTOR_SERVER_ADDRESS";
const string envServerSslEnabled = "IGNITE_COMPUTE_EXECUTOR_SERVER_SSL_ENABLED";
const string envServerSslSkipValidation = "IGNITE_COMPUTE_EXECUTOR_SERVER_SSL_SKIP_CERTIFICATE_VALIDATION";

string? serverAddr = Environment.GetEnvironmentVariable(envServerAddr);
bool serverSslEnabled = string.Equals("true", Environment.GetEnvironmentVariable(envServerSslEnabled), StringComparison.OrdinalIgnoreCase);
bool serverSslSkipCertValidation = string.Equals("true", Environment.GetEnvironmentVariable(envServerSslSkipValidation), StringComparison.OrdinalIgnoreCase);

if (string.IsNullOrWhiteSpace(serverAddr))
{
    throw new InvalidOperationException($"Environment variable {envServerAddr} is not set.");
}

ISslStreamFactory? sslStreamFactory = (serverSslEnabled, serverSslSkipCertValidation) switch
{
    (true, true) => new SslStreamFactory
    {
        SslClientAuthenticationOptions = new SslClientAuthenticationOptions
        {
#pragma warning disable CA5359 // Configured to disable cert validation.
            RemoteCertificateValidationCallback = (_, _, _, _) => true
#pragma warning restore CA5359
        }
    },
    (true, false) => new SslStreamFactory(),
    _ => null
};

var clientCfg = new IgniteClientConfiguration(serverAddr)
{
    RetryPolicy = RetryNonePolicy.Instance, // No reconnect on error.
    ReconnectInterval = TimeSpan.Zero, // No background reconnect.
    SslStreamFactory = sslStreamFactory,
    LoggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning)),
    HeartbeatInterval = TimeSpan.FromSeconds(1)
};

using var client = await IgniteClient.StartAsync(clientCfg).ConfigureAwait(false);

// Sleep forever. The host process will terminate us when the executor is stopped.
await Task.Delay(Timeout.Infinite).ConfigureAwait(false);

#pragma warning disable CA2007, SA1600, SA1649, SA1402, CA1050

ITable? table = await client.Tables.GetTableAsync("my-table");

ReceiverDescriptor<MyData, MyArg, MyResult> receiverDesc = ReceiverDescriptor.Of(new MyReceiver());

IAsyncEnumerable<MyData> data = Enumerable
    .Range(1, 100)
    .Select(x => new MyData(x, $"Name {x}"))
    .ToAsyncEnumerable();

IAsyncEnumerable<MyResult> results = table!.RecordBinaryView.StreamDataAsync(
    data: data,
    receiver: receiverDesc,
    keySelector: dataItem => new IgniteTuple { ["id"] = dataItem.Id },
    payloadSelector: dataItem => dataItem,
    receiverArg: new MyArg("Some info"));

await foreach (MyResult result in results)
{
    Console.WriteLine(result);
}

public record MyData(int Id, string Name);

public record MyArg(string Info);

public record MyResult(MyData Data, MyArg Arg);

public class MyReceiver : IDataStreamerReceiver<MyData, MyArg, MyResult>
{
    public IMarshaller<MyData> PayloadMarshaller =>
        new JsonMarshaller<MyData>();

    public IMarshaller<MyArg> ArgumentMarshaller =>
        new JsonMarshaller<MyArg>();

    public IMarshaller<MyResult> ResultMarshaller =>
        new JsonMarshaller<MyResult>();

    public ValueTask<IList<MyResult>?> ReceiveAsync(IList<MyData> page, MyArg arg, IDataStreamerReceiverContext context, CancellationToken cancellationToken)
    {
        IList<MyResult> results = page
            .Select(data => new MyResult(data, arg))
            .ToList();

        return ValueTask.FromResult(results)!;
    }
}
