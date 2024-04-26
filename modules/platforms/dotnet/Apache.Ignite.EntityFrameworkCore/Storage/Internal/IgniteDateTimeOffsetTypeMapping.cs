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

namespace Apache.Ignite.EntityFrameworkCore.Storage.Internal;

using System;
using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteDateTimeOffsetTypeMapping : DateTimeOffsetTypeMapping
{
    private const string DateTimeOffsetFormatConst = @"'{0:yyyy\-MM\-dd HH\:mm\:ss.FFFFFFFzzz}'";

    public static new IgniteDateTimeOffsetTypeMapping Default { get; } = new(IgniteTypeMappingSource.TextTypeName);

    public IgniteDateTimeOffsetTypeMapping(
        string storeType,
        DbType? dbType = System.Data.DbType.DateTimeOffset)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(typeof(DateTimeOffset)),
                storeType,
                dbType: dbType))
    {
    }

    protected IgniteDateTimeOffsetTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IgniteDateTimeOffsetTypeMapping(parameters);

    protected override string SqlLiteralFormatString
        => DateTimeOffsetFormatConst;
}
