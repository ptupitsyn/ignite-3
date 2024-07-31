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

namespace Apache.Ignite.Benchmarks;

using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Internal;

/// <summary>
/// Results on i9-12900H, .NET SDK 6.0.3224, Ubuntu 22.04:
/// | Method    | Mean     | Error    | StdDev   |
/// |---------- |---------:|---------:|---------:|
/// | Heartbeat | 15.62 us | 0.386 us | 1.121 us |.
/// </summary>
[SimpleJob]
public class HeartbeatBenchmark
{
    private IgniteClientInternal? _client;
    private ClientSocket? _socket;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        // Requires PlatformBenchmarkNodeRunner class to be started.
        var cfg = new IgniteClientConfiguration("127.0.0.1:10420");

        _client = (IgniteClientInternal)await IgniteClient.StartAsync(cfg);
        _socket = _client.Socket.GetSockets().Single();
    }

    [GlobalCleanup]
    public void GlobalCleanup() => _client?.Dispose();

    [Benchmark]
    public async Task Heartbeat() => await _socket!.HeartbeatAsync();
}
