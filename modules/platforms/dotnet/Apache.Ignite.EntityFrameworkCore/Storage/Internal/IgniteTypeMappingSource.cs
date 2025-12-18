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
using System.Collections.Generic;
using System.Linq;
using Common;
using Microsoft.EntityFrameworkCore.Storage;

public class IgniteTypeMappingSource(
    TypeMappingSourceDependencies dependencies,
    RelationalTypeMappingSourceDependencies relationalDependencies)
    : RelationalTypeMappingSource(dependencies, relationalDependencies)
{
    // TODO: Get rid of custom type mapping classes where possible (e.g., use built-in StringTypeMapping).
    internal const string IntegerTypeName = "INTEGER";
    internal const string RealTypeName = "REAL";
    internal const string BlobTypeName = "BLOB";
    internal const string TextTypeName = "VARCHAR";
    internal const string GuidTypeName = "UUID";
    internal const string BoolTypeName = "BOOL";

    private static readonly LongTypeMapping BigInt = new(IntegerTypeName);
    private static readonly DoubleTypeMapping Real = new(RealTypeName);
    private static readonly IgniteByteArrayTypeMapping Blob = IgniteByteArrayTypeMapping.Default;
    private static readonly IgniteStringTypeMapping Text = IgniteStringTypeMapping.Default;
    private static readonly IgniteBoolTypeMapping Bool = new(BoolTypeName, null);

    private readonly Dictionary<Type, RelationalTypeMapping> _clrTypeMappings = new()
    {
        { typeof(string), Text },
        { typeof(byte[]), Blob },
        { typeof(bool), Bool },
        { typeof(byte), new ByteTypeMapping(IntegerTypeName) },
        { typeof(char), new CharTypeMapping(TextTypeName) },
        { typeof(int), new IntTypeMapping(IntegerTypeName) },
        { typeof(long), BigInt },
        { typeof(sbyte), new SByteTypeMapping(IntegerTypeName) },
        { typeof(short), new ShortTypeMapping(IntegerTypeName) },
        { typeof(uint), new UIntTypeMapping(IntegerTypeName) },
        { typeof(ulong), IgniteULongTypeMapping.Default },
        { typeof(ushort), new UShortTypeMapping(IntegerTypeName) },
        { typeof(DateTime), IgniteDateTimeTypeMapping.Default },
        { typeof(DateTimeOffset), IgniteDateTimeOffsetTypeMapping.Default },
        { typeof(TimeSpan), new TimeSpanTypeMapping(TextTypeName) },
        { typeof(DateOnly), IgniteDateOnlyTypeMapping.Default },
        { typeof(TimeOnly), IgniteTimeOnlyTypeMapping.Default },
        { typeof(decimal), IgniteDecimalTypeMapping.Default },
        { typeof(double), Real },
        { typeof(float), new FloatTypeMapping(RealTypeName) },
        { typeof(Guid), new IgniteGuidTypeMapping(GuidTypeName) },
    };

    private readonly Dictionary<string, RelationalTypeMapping> _storeTypeMappings = new(StringComparer.OrdinalIgnoreCase)
    {
        { IntegerTypeName, BigInt },
        { RealTypeName, Real },
        { BlobTypeName, Blob },
        { TextTypeName, Text }
    };

    private readonly Func<string, RelationalTypeMapping?>[] _typeRules =
    {
        name => Contains(name, "INT")
            ? BigInt
            : null,
        name => Contains(name, "CHAR")
                || Contains(name, "CLOB")
                || Contains(name, "TEXT")
            ? Text
            : null,
        name => Contains(name, "BLOB")
            ? Blob
            : null,
        name => Contains(name, "REAL")
                || Contains(name, "FLOA")
                || Contains(name, "DOUB")
            ? Real
            : null
    };

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var mapping = base.FindMapping(mappingInfo)
            ?? FindRawMapping(mappingInfo);

        return mapping != null
            && mappingInfo.StoreTypeName != null
                ? mapping.WithStoreTypeAndSize(mappingInfo.StoreTypeName, null)
                : mapping;
    }

    private static bool Contains(string haystack, string needle)
        => haystack.Contains(needle, StringComparison.OrdinalIgnoreCase);

    private RelationalTypeMapping? FindRawMapping(RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        if (clrType == typeof(byte[]) && mappingInfo.ElementTypeMapping != null)
        {
            return null;
        }

        if (clrType != null
            && _clrTypeMappings.TryGetValue(clrType, out var mapping))
        {
            return mapping;
        }

        var storeTypeName = mappingInfo.StoreTypeName;
        if (storeTypeName != null
            && _storeTypeMappings.TryGetValue(storeTypeName, out mapping)
            && (clrType == null || mapping.ClrType.UnwrapNullableType() == clrType))
        {
            return mapping;
        }

        if (storeTypeName != null)
        {
            var affinityTypeMapping = _typeRules.Select(r => r(storeTypeName)).FirstOrDefault(r => r != null);

            if (affinityTypeMapping != null)
            {
                return clrType == null || affinityTypeMapping.ClrType.UnwrapNullableType() == clrType
                    ? affinityTypeMapping
                    : null;
            }

            if (clrType == null || clrType == typeof(byte[]))
            {
                return Blob;
            }
        }

        return null;
    }
}
