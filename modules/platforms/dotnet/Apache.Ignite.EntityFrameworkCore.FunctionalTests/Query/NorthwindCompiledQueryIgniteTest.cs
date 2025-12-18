// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Apache.Ignite.EntityFrameworkCore.FunctionalTests.Query;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

public class NorthwindCompiledQueryIgniteTest(NorthwindQueryIgniteFixture<NoopModelCustomizer> fixture)
    : NorthwindCompiledQueryTestBase<NorthwindQueryIgniteFixture<NoopModelCustomizer>>(fixture)
{
    [ConditionalFact(Skip = "Not supported.")]
    public override void Keyless_query()
    {
        base.Keyless_query();
    }

    [ConditionalFact(Skip = "Not supported.")]
    public override void Keyless_query_first()
    {
        base.Keyless_query_first();
    }

    [ConditionalFact(Skip = "Not supported.")]
    public override Task Keyless_query_async()
    {
        return base.Keyless_query_async();
    }

    [ConditionalFact(Skip = "Not supported.")]
    public override Task Keyless_query_first_async()
    {
        return base.Keyless_query_first_async();
    }

    [ConditionalFact(Skip = "Not supported.")]
    public override void Compiled_query_when_using_member_on_context()
    {
        base.Compiled_query_when_using_member_on_context();
    }

    [ConditionalFact(Skip = "Not supported.")]
    public override void Query_with_array_parameter()
    {
        base.Query_with_array_parameter();
    }

    [ConditionalFact(Skip = "Not supported.")]
    public override Task Query_with_array_parameter_async()
    {
        return base.Query_with_array_parameter_async();
    }
}
