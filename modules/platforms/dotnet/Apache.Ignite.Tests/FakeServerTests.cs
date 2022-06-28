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

namespace Apache.Ignite.Tests
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Tests that <see cref="FakeServer"/> works as expected.
    /// </summary>
    public class FakeServerTests
    {
        [Test]
        public async Task TestConnectToFakeServerAndGetTablesReturnsEmptyList()
        {
            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var sw = Stopwatch.StartNew();
            var count = 100000;
            for (int i = 0; i < count; i++)
            {
                var tables = await client.Tables.GetTablesAsync();
                if (tables.Count != 0)
                {
                    throw new Exception("e");
                }
            }

            Console.WriteLine(count / sw.Elapsed.TotalSeconds + " RPS");
        }

        [Test]
        public async Task TestConnectToFakeServerAndGetTableThrowsError()
        {
            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var ex = Assert.ThrowsAsync<IgniteClientException>(async () => await client.Tables.GetTableAsync("t"));
            Assert.AreEqual(FakeServer.Err, ex!.Message);
        }

        [Test]
        public async Task TestConnectToFakeServerAndGetExistingTableReturnsTable()
        {
            using var server = new FakeServer();
            using var client = await server.ConnectClientAsync();

            var table = await client.Tables.GetTableAsync(FakeServer.ExistingTableName);
            Assert.IsNotNull(table);
            Assert.AreEqual(FakeServer.ExistingTableName, table!.Name);
        }

        [Test]
        public async Task TestFakeServerDropsConnectionOnSpecifiedRequestCount()
        {
            var cfg = new IgniteClientConfiguration
            {
                RetryPolicy = RetryNonePolicy.Instance
            };

            using var server = new FakeServer(reqId => reqId % 3 == 0);
            using var client = await server.ConnectClientAsync(cfg);

            // 2 requests succeed, 3rd fails.
            await client.Tables.GetTablesAsync();
            await client.Tables.GetTablesAsync();

            Assert.CatchAsync(async () => await client.Tables.GetTablesAsync());

            // Reconnect by FailoverSocket logic.
            await client.Tables.GetTablesAsync();
        }
    }
}
