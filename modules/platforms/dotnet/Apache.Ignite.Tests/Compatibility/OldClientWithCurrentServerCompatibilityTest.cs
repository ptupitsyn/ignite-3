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

namespace Apache.Ignite.Tests.Compatibility;

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading.Tasks;
using NUnit.Framework;
using TestHelpers;

/// <summary>
/// Tests that an old client can connect to the current server.
/// </summary>
[TestFixture("3.0.0")]
public class OldClientWithCurrentServerCompatibilityTest : IgniteTestsBase
{
    private readonly string _clientVersion;

    private TempDir _packageDir;

    private AssemblyLoadContext _loadContext;

    private Assembly _oldClientAssembly;

    public OldClientWithCurrentServerCompatibilityTest(string clientVersion) =>
        _clientVersion = clientVersion;

    [OneTimeSetUp]
    public async Task InitOldClient()
    {
        // TODO: Use a different approach:
        // - Separate solution
        // - Symlinks to reuse testing infra
        // - Many project files for every version
        _packageDir = new TempDir();
        await NuGetUtils.DownloadNuGetPackageAsync("Apache.Ignite", _clientVersion, _packageDir.Path);

        _loadContext = new AssemblyLoadContext(
            name: $"{nameof(OldClientWithCurrentServerCompatibilityTest)}-{_clientVersion}",
            isCollectible: true);

        var dllPath = Path.Combine(_packageDir.Path, "lib/net8.0/Apache.Ignite.dll");
        _oldClientAssembly = _loadContext.LoadFromAssemblyPath(dllPath);
        Assert.IsNotNull(_oldClientAssembly);
    }

    [OneTimeTearDown]
    public void CleanupOldClient()
    {
        _loadContext.Unload();
        _packageDir.Dispose();
    }

    [Test]
    public async Task TestClientStart()
    {
        var oldClientType = _oldClientAssembly.GetType(typeof(IgniteClient).FullName!);
        Assert.IsNotNull(oldClientType);

        Task task = (Task)oldClientType.GetMethod("StartAsync", BindingFlags.Public | BindingFlags.Static)
            ?.Invoke(null, [GetConfig()])!;

        await task;
    }
}
