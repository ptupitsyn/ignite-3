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

package org.apache.ignite.internal.runner.app.client.benchmarks;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParameters;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Client vs embedded benchmark.
 *
 * <p>Results on i9-12900H, openjdk 11.0.18, Ubuntu 22.04:
 * Benchmark                                 Mode  Cnt       Score       Error  Units
 * ItThinClientPutGetBenchmark.clientGet    thrpt    3   21521.397 ± 15122.121  ops/s
 * ItThinClientPutGetBenchmark.embeddedGet  thrpt    3  108435.632 ± 94284.946  ops/s
 */
@State(Scope.Benchmark)
public class ItThinClientPutGetBenchmark {
    private Ignite server;

    private IgniteClient client;

    private RecordView<Tuple> embeddedTable;

    private RecordView<Tuple> clientTable;

    private final Tuple key = Tuple.create().set("id", 1);

    private static final Path WORK_DIR = WorkDirectoryExtension.BASE_PATH.resolve("ItThinClientPutGetBenchmark");

    @Setup
    public void init() {
        String node0Name = "ItThinClientPutGetBenchmark";

        String cfg = "{\n"
                + "  network.port: 3344,\n"
                + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\" ]\n"
                + "  clientConnector.port: 10800,\n"
                + "  rest.port: 10300\n"
                + "}";

        CompletableFuture<Ignite> fut = TestIgnitionManager.start(node0Name, cfg, WORK_DIR.resolve(node0Name));

        String metaStorageNode = node0Name;

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNode)
                .metaStorageNodeNames(List.of(metaStorageNode))
                .clusterName("cluster")
                .build();
        TestIgnitionManager.init(initParameters);

        server = fut.join();

        server.sql().createSession().execute(null, "CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY, name VARCHAR)");
        embeddedTable = server.tables().table("test").recordView();

        client = IgniteClient.builder()
                .addresses("localhost:10800")
                .build();
        clientTable = client.tables().table("test").recordView();

        clientTable.upsert(null, Tuple.create().set("id", 1).set("name", "John Doe"));
    }

    @TearDown
    public void tearDown() throws Exception {
        client.close();
        server.close();
        IgniteUtils.deleteIfExists(WORK_DIR);
    }

    @Benchmark
    public void clientGet() {
        clientTable.get(null, key);
    }

    @Benchmark
    public void embeddedGet() {
        embeddedTable.get(null, key);
    }

    /**
     * Runner.
     *
     * @param args Arguments.
     * @throws RunnerException Exception.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ItThinClientPutGetBenchmark.class.getSimpleName())
                .addProfiler(JavaFlightRecorderProfiler.class)
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(5))
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
