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
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Benchmark)
public class ItThinClientPutGetBenchmark {
    private Ignite server;

    private IgniteClient client;

    private RecordView<Tuple> embeddedTable;

    private RecordView<Tuple> clientTable;

    private final Tuple key = Tuple.create().set("id", 1);

    @Setup
    public void init(@WorkDirectory Path workDir) {
        String node0Name = "ItThinClientPutGetBenchmark";

        String cfg = "{\n"
                + "  network.port: 3344,\n"
                + "  network.nodeFinder.netClusterNodes: [ \"localhost:3344\" ]\n"
                + "  clientConnector.port: 10800,\n"
                + "  rest.port: 10300\n"
                + "}";

        CompletableFuture<Ignite> fut = TestIgnitionManager.start(node0Name, cfg, workDir.resolve(node0Name));

        String metaStorageNode = node0Name;

        InitParameters initParameters = InitParameters.builder()
                .destinationNodeName(metaStorageNode)
                .metaStorageNodeNames(List.of(metaStorageNode))
                .clusterName("cluster")
                .build();
        TestIgnitionManager.init(initParameters);

        server = fut.join();

        server.sql().createSession().execute(null, "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)");
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
        // TODO: addProfiler
        Options opt = new OptionsBuilder()
                .include(ItThinClientPutGetBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(5))
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
