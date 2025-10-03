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

namespace Apache.Ignite.Tests.Sql;

using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Ignite.Sql;
using NUnit.Framework;

public class IgniteDbConnectionTests : IgniteTestsBase
{
    [Test]
    public async Task Demo()
    {
        var connStr = $"Endpoints=localhost:10942,localhost:10801;SocketTimeout=00:00:07";
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
    }

    [Test]
    public async Task TestOpenClose()
    {
        var connectionString = $"Endpoints={GetConfig().Endpoints.First()}";
        await using var conn = new IgniteDbConnection(connectionString);
        Assert.AreEqual(ConnectionState.Closed, conn.State);

        await conn.OpenAsync();
        Assert.AreEqual(ConnectionState.Open, conn.State);
        Assert.AreEqual("3.x", conn.ServerVersion);
        Assert.AreEqual(string.Empty, conn.DataSource);
        Assert.AreEqual(string.Empty, conn.Database);
        Assert.AreEqual(connectionString, conn.ConnectionString);
        Assert.IsNotNull(conn.Client);

        await conn.CloseAsync();
        Assert.AreEqual(ConnectionState.Closed, conn.State);
    }

    [Test]
    public async Task TestExistingClient([Values(true, false)] bool ownsClient)
    {
        using var client = await IgniteClient.StartAsync(GetConfig());

        await using var conn = new IgniteDbConnection(null);
        Assert.AreEqual(ConnectionState.Closed, conn.State);

        conn.Open(client, ownsClient);
        Assert.AreEqual(ConnectionState.Open, conn.State);

        await conn.CloseAsync();
        Assert.AreEqual(ConnectionState.Closed, conn.State);

        if (ownsClient)
        {
            Assert.That(client.GetConnections(), Is.Empty, "Client should be closed after connection is closed with ownsClient: true");
        }
        else
        {
            Assert.That(client.GetConnections(), Is.Not.Empty, "Client should be open after connection is closed with ownsClient: false");
        }
    }

    [Test]
    public void TestConnectionStringProperty()
    {
        var connectionString = $"Endpoints={GetConfig().Endpoints.First()}";
        using var conn = new IgniteDbConnection(null);
        Assert.AreEqual(string.Empty, conn.ConnectionString);
        conn.ConnectionString = connectionString;
        Assert.AreEqual(ConnectionState.Closed, conn.State);

        conn.Open();
        Assert.AreEqual(connectionString, conn.ConnectionString);
        Assert.IsNotNull(conn.Client);

        conn.Close();
        Assert.AreEqual(ConnectionState.Closed, conn.State);
    }

    [Test]
    public async Task TestCommand([Values(true, false)] bool withTx)
    {
        await using var conn = new IgniteDbConnection(null);
        Assert.AreEqual(ConnectionState.Closed, conn.State);

        conn.Open(Client, ownsClient: false);
        Assert.AreEqual(ConnectionState.Open, conn.State);

        await using var cmd = conn.CreateCommand();
        Assert.IsNotNull(cmd);
        Assert.AreEqual(conn, cmd.Connection);
        Assert.AreEqual(CommandType.Text, cmd.CommandType);
        Assert.AreEqual(0, cmd.CommandTimeout);
        Assert.IsNull(cmd.Transaction);

        await using var tx = withTx ? await conn.BeginTransactionAsync(IsolationLevel.Serializable) : null;
        cmd.Transaction = tx;
        Assert.AreEqual(tx, cmd.Transaction);

        cmd.CommandText = "SELECT 1";
        var scalar = await cmd.ExecuteScalarAsync();
        Assert.AreEqual(1, scalar);
    }

    [Test]
    public void TestToString()
    {
        using var conn = new IgniteDbConnection(null);
        conn.ConnectionString = "foo_bar";

        Assert.AreEqual(
            "IgniteDbConnection { ConnectionString = foo_bar, State = Closed, ServerVersion = 3.x, Client = }",
            conn.ToString());
    }
}
