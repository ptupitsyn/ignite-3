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

namespace TalkSamples;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Static code analysis demo.
/// </summary>
public class Analyzers
{
    [Test]
    [SuppressMessage(
        "StyleCop.CSharp.DocumentationRules",
        "SA1600:Elements should be documented",
        Justification = "Demo")]
    [Parallelizable(ParallelScope.Children)]
    [SuppressMessage(
        "Structure",
        "NUnit1009:One may not specify ParallelScope.Children on a non-parameterized test method",
        Justification = "Demo")]
    public void Test1()
    {
        Assert.Pass();
    }
}
