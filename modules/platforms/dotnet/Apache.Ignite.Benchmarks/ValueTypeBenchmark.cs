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

namespace Apache.Ignite.Benchmarks;

using System.Linq;
using BenchmarkDotNet.Attributes;

/// <summary>
/// 100M, MB Pro M3
/// | Method      | Mean     | Error    | StdDev   |
/// |------------ |---------:|---------:|---------:|
/// | TestValType | 53.73 ms | 0.028 ms | 0.023 ms |
/// | TestRefType | 56.27 ms | 0.047 ms | 0.044 ms |.
///
/// 100M, Intel Core i9-12900H
/// | Method      | Mean      | Error    | StdDev   | Ratio |
/// |------------ |----------:|---------:|---------:|------:|
/// | TestValType |  65.59 ms | 0.766 ms | 0.679 ms |  0.43 |
/// | TestRefType | 154.09 ms | 1.819 ms | 1.519 ms |  1.00 |.
///
/// 1M, Intel Core i9-12900H
/// | Method      | Mean       | Error    | StdDev   | Median     | Ratio | RatioSD |
/// |------------ |-----------:|---------:|---------:|-----------:|------:|--------:|
/// | TestValType |   417.8 us |  5.77 us | 14.69 us |   412.3 us |  0.39 |    0.01 |
/// | TestRefType | 1,074.9 us | 14.41 us | 12.04 us | 1,074.4 us |  1.00 |    0.02 |.
/// </summary>
public class ValueTypeBenchmark
{
    private const int Count = 1_000_000;

    private static readonly MyRefType[] RefItems = Enumerable
        .Range(0, Count)
        .Select(x => new MyRefType { Field1 = x, Field2 = x + 1 })
        .ToArray();

    private static readonly MyValType[] ValItems = Enumerable
        .Range(0, Count)
        .Select(x => new MyValType { Field1 = x, Field2 = x + 1 })
        .ToArray();

    [Benchmark]
    public int TestValType()
    {
        int sum = 0;

        foreach (var item in ValItems)
        {
            sum += item.Field1 + item.Field2;
        }

        return sum;
    }

    [Benchmark(Baseline = true)]
    public int TestRefType()
    {
        int sum = 0;

        foreach (var item in RefItems)
        {
            sum += item.Field1 + item.Field2;
        }

        return sum;
    }

#pragma warning disable
    public class MyRefType
    {
        public int Field1;
        public int Field2;
    }

    public struct MyValType
    {
        public int Field1;
        public int Field2;
    }
}
