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
using System.Threading;
using System.Threading.Tasks;
using NuGet.Common;
using NuGet.Packaging;
using NuGet.Protocol;
using NuGet.Protocol.Core.Types;
using NuGet.Versioning;
using NUnit.Framework;
using TestHelpers;

/// <summary>
/// Tests that an old client can connect to the current server.
/// </summary>
[TestFixture("3.0.0")]
public class OldClientWithCurrentServerCompatibilityTest : IgniteTestsBase
{
    private readonly string _clientVersion;

    public OldClientWithCurrentServerCompatibilityTest(string clientVersion) =>
        _clientVersion = clientVersion;

    [Test]
    public async Task TestDownloadNuGetPackage()
    {
        SourceCacheContext cache = new SourceCacheContext();
        SourceRepository repository = Repository.Factory.GetCoreV3("https://api.nuget.org/v3/index.json");
        FindPackageByIdResource resource = await repository.GetResourceAsync<FindPackageByIdResource>();

        string packageId = "Apache.Ignite";
        NuGetVersion packageVersion = new NuGetVersion(_clientVersion);
        using MemoryStream packageStream = new MemoryStream();

        await resource.CopyNupkgToStreamAsync(
            packageId,
            packageVersion,
            packageStream,
            cache,
            NullLogger.Instance,
            CancellationToken.None);

        Console.WriteLine($"Downloaded package {packageId} {packageVersion}");

        using PackageArchiveReader packageReader = new PackageArchiveReader(packageStream);

        FrameworkSpecificGroup frameworkSpecificGroup = packageReader.GetLibItems().Single();
        Assert.AreEqual("net8.0", frameworkSpecificGroup.TargetFramework.GetShortFolderName());

        using var tempDir = new TempDir();

        foreach (var item in frameworkSpecificGroup.Items)
        {
            packageReader.ExtractFile(item, Path.Combine(tempDir.Path, item), NullLogger.Instance);
        }
    }
}
