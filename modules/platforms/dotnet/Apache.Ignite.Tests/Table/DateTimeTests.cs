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

namespace Apache.Ignite.Tests.Table;

using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using NodaTime;
using NUnit.Framework;

public class DateTimeTests : IgniteTestsBase
{
    [Test]
    public async Task TestDateTime()
    {
        await Client.Sql.ExecuteAsync(null, "DROP TABLE IF EXISTS test_date_time");
        await Client.Sql.ExecuteAsync(
            null,
            "CREATE TABLE test_date_time (id INT PRIMARY KEY, birthday TIMESTAMP WITH LOCAL TIME ZONE NOT NULL)");

        var table = await Client.Tables.GetTableAsync("test_date_time");
        var view = table!.GetRecordView<DtPoco>();

        var poco = new DtPoco
        {
            Id = 1,
            Birthday = DateTime.UtcNow
        };

        await view.UpsertAsync(null, poco);

        using var resultSet = await Client.Sql.ExecuteAsync<DtPoco>(null, "select * from test_date_time");
        await foreach (var row in resultSet)
        {
            Console.WriteLine(row);
        }
    }

    [SuppressMessage("Naming", "CA1708:Identifiers should differ by more than case", Justification = "Reviewed")]
    private record DtPoco
    {
        private Instant birthday;

        public DateTime Birthday
        {
            get => birthday.ToDateTimeUtc();
            set => birthday = Instant.FromDateTimeUtc(value);
        }

        public int Id { get; set; }
    }

    private record DtPoco2(int Id, DateTime Dt);
}
