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

    [ConditionalTheory(Skip = "Unordered query with ElementAt is unpredictable.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task ElementAt_over_custom_projection_compared_to_not_null(bool async)
    {
        return base.ElementAt_over_custom_projection_compared_to_not_null(async);
    }

    [ConditionalTheory(Skip = "Unordered query with ElementAt is unpredictable.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task ElementAtOrDefault_over_custom_projection_compared_to_null(bool async)
    {
        return base.ElementAtOrDefault_over_custom_projection_compared_to_null(async);
    }

    [ConditionalTheory(Skip = "Not supported.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_string_indexof(bool async)
    {
        return base.Where_string_indexof(async);
    }

    [ConditionalTheory(Skip = "Unordered query with ElementAt is unpredictable.")]
    [MemberData(nameof(IsAsyncData))]
    public override Task Where_datetime_date_component(bool async)
    {
        return base.Where_datetime_date_component(async);
    }

    public override Task Where_date_add_year_constant_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_year_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_month_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_dayOfYear_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_day_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_hour_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_minute_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_second_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_millisecond_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetimeoffset_now_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetimeoffset_utcnow_component(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_bitwise_or(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_bitwise_and(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_bitwise_xor(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_compare_tuple_constructed_equal(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_compare_tuple_constructed_multi_value_equal(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_compare_tuple_constructed_multi_value_not_equal(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_compare_tuple_create_constructed_equal(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_compare_tuple_create_constructed_multi_value_equal(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_compare_tuple_create_constructed_multi_value_not_equal(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_compare_constructed_equal(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_compare_constructed_multi_value_not_equal(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_now(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_utcnow(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetimeoffset_utcnow(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }

    public override Task Where_datetime_today(bool async)
    {
        // Not supported.
        return Task.CompletedTask;
    }
}
