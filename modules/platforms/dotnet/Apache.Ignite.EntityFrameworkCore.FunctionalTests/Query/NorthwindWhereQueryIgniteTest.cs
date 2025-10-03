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

public class NorthwindWhereQueryIgniteTest : NorthwindWhereQueryRelationalTestBase<
    NorthwindQueryIgniteFixture<NoopModelCustomizer>>
{
    public NorthwindWhereQueryIgniteTest(NorthwindQueryIgniteFixture<NoopModelCustomizer> fixture)
        : base(fixture)
    {
    }

    public override async Task Where_compare_constructed_multi_value_equal(bool async)
    {
        // Anonymous type to constant comparison - not supported in Ignite.
        await AssertTranslationFailed(() => base.Where_compare_constructed_multi_value_equal(async));
    }

    public override Task ElementAt_over_custom_projection_compared_to_not_null(bool async)
    {
        // Unordered query with ElementAt is unpredictable.
        return Task.CompletedTask;
    }

    public override Task ElementAtOrDefault_over_custom_projection_compared_to_null(bool async)
    {
        // Unordered query with ElementAt is unpredictable.
        return Task.CompletedTask;
    }
}
