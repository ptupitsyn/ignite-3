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
using System.Threading;
using System.Threading.Tasks;
using NuGet.Common;
using NuGet.Packaging;
using NuGet.Protocol;
using NuGet.Protocol.Core.Types;
using NuGet.Versioning;

/// <summary>
/// NuGet utility class for compatibility tests.
/// </summary>
public static class NuGetUtils
{
    public static async Task DownloadNuGetPackageAsync(
        string packageId,
        string version,
        string outputPath,
        CancellationToken cancellationToken = default)
    {
        var repository = Repository.Factory.GetCoreV3("https://api.nuget.org/v3/index.json");
        var resource = await repository.GetResourceAsync<FindPackageByIdResource>(cancellationToken);

        using var packageStream = new MemoryStream();

        await resource.CopyNupkgToStreamAsync(
            id: packageId,
            version: new NuGetVersion(version),
            destination: packageStream,
            cacheContext: new SourceCacheContext(),
            logger: NullLogger.Instance,
            cancellationToken: CancellationToken.None);

        Console.WriteLine($"Downloaded package {packageId} {new NuGetVersion(version)}");

        using PackageArchiveReader packageReader = new PackageArchiveReader(packageStream);

        foreach (var item in packageReader.GetFiles())
        {
            packageReader.ExtractFile(item, Path.Combine(outputPath, item), NullLogger.Instance);
        }
    }
}
