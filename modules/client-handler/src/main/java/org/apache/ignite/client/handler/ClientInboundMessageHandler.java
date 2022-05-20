/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.requests.cluster.ClientClusterGetNodesRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteColocatedRequest;
import org.apache.ignite.client.handler.requests.compute.ClientComputeExecuteRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlCloseRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlColumnMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteBatchRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlFetchRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlPreparedStmntBatchRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlPrimaryKeyMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlQueryMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlSchemasMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlTableMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.table.ClientSchemasGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableIdDoesNotExistException;
import org.apache.ignite.client.handler.requests.table.ClientTablesGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleContainsKeyRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteAllExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndDeleteRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndReplaceRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndUpsertRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleInsertAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleInsertRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleReplaceExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleReplaceRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertRequest;
import org.apache.ignite.client.handler.requests.tx.ClientTransactionBeginRequest;
import org.apache.ignite.client.handler.requests.tx.ClientTransactionCommitRequest;
import org.apache.ignite.client.handler.requests.tx.ClientTransactionRollbackRequest;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorView;
import org.apache.ignite.internal.client.proto.ClientErrorCode;
import org.apache.ignite.internal.client.proto.ClientMessageCommon;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.client.proto.ServerMessageType;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Handles messages from thin clients.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientInboundMessageHandler extends ChannelInboundHandlerAdapter {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ClientInboundMessageHandler.class);

    /** Ignite tables API. */
    private final IgniteTables igniteTables;

    /** Ignite tables API. */
    private final IgniteTransactions igniteTransactions;

    /** JDBC Handler. */
    private final JdbcQueryEventHandler jdbcQueryEventHandler;

    /** Configuration. */
    private final ClientConnectorView configuration;

    /** Compute. */
    private final IgniteCompute compute;

    /** Cluster. */
    private final ClusterService clusterService;

    /** Session handler. */
    private final ClientSessionHandler sessionHandler;

    /** Context. */
    private volatile ClientContext clientContext;

    /** Session. */
    private volatile ClientSession session;

    /**
     * Constructor.
     *
     * @param sessionHandler     Client session handler.
     * @param igniteTables       Ignite tables API entry point.
     * @param igniteTransactions Transactions API.
     * @param processor          Sql query processor.
     * @param configuration      Configuration.
     * @param compute            Compute.
     * @param clusterService     Cluster.
     */
    public ClientInboundMessageHandler(
            ClientSessionHandler sessionHandler,
            IgniteTables igniteTables,
            IgniteTransactions igniteTransactions,
            QueryProcessor processor,
            ClientConnectorView configuration,
            IgniteCompute compute,
            ClusterService clusterService) {
        assert igniteTables != null;
        assert igniteTransactions != null;
        assert processor != null;
        assert configuration != null;
        assert compute != null;
        assert clusterService != null;

        this.sessionHandler = sessionHandler;
        this.igniteTables = igniteTables;
        this.igniteTransactions = igniteTransactions;
        this.configuration = configuration;
        this.compute = compute;
        this.clusterService = clusterService;

        jdbcQueryEventHandler = new JdbcQueryEventHandlerImpl(processor, new JdbcMetadataCatalog(igniteTables));
    }

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Each inbound handler in a pipeline has to release the received messages.
        try (var unpacker = getUnpacker((ByteBuf) msg)) {
            // Packer buffer is released by Netty on send, or by inner exception handlers below.
            var packer = getPacker(ctx.alloc());

            if (clientContext == null) {
                handshake(ctx, unpacker, packer);
            } else {
                processOperation(ctx, unpacker, packer);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        var ses = session;

        if (ses != null) {
            ses.channelInactive();
        }

        super.channelInactive(ctx);
    }

    private void handshake(ChannelHandlerContext ctx, ClientMessageUnpacker unpacker, ClientMessagePacker packer) {
        try {
            writeMagic(ctx);
            var clientVer = ProtocolVersion.unpack(unpacker);

            if (!clientVer.equals(ProtocolVersion.LATEST_VER)) {
                throw new IgniteException("Unsupported version: "
                        + clientVer.major() + "." + clientVer.minor() + "." + clientVer.patch());
            }

            var clientCode = unpacker.unpackInt();

            var featuresLen = unpacker.unpackBinaryHeader();
            var features = BitSet.valueOf(unpacker.readPayload(featuresLen));

            // New connection -> null, restored connection -> non-null.
            var existingSessionId = unpacker.tryUnpackNil() ? null : unpacker.unpackUuid();

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValues(extensionsLen);

            clientContext = new ClientContext(clientVer, clientCode, features);

            LOG.debug("Handshake: " + clientContext);

            session = sessionHandler.getOrCreateSession(existingSessionId);

            // Response.
            ProtocolVersion.LATEST_VER.pack(packer);
            packer.packInt(ClientErrorCode.SUCCESS);

            packer.packLong(configuration.idleTimeout());

            ClusterNode localMember = clusterService.topologyService().localMember();
            packer.packString(localMember.id());
            packer.packString(localMember.name());

            packer.packBinaryHeader(0); // Features.
            packer.packUuid(session.id());
            packer.packMapHeader(0); // Extensions.

            write(packer.getBuffer(), ctx);

            session.channelActive(buf -> write(buf, ctx));
        } catch (Throwable t) {
            packer.close();

            var errPacker = getPacker(ctx.alloc());

            try {
                ProtocolVersion.LATEST_VER.pack(errPacker);

                String message = t.getMessage();

                if (message == null) {
                    message = t.getClass().getName();
                }

                errPacker.packInt(ClientErrorCode.FAILED);
                errPacker.packString(message);

                write(errPacker.getBuffer(), ctx);
            } catch (Throwable t2) {
                errPacker.close();
                exceptionCaught(ctx, t2);
            }
        }
    }

    private void writeMagic(ChannelHandlerContext ctx) {
        ctx.write(Unpooled.wrappedBuffer(ClientMessageCommon.MAGIC_BYTES));
    }

    private void write(ByteBuf buf, ChannelHandlerContext ctx) {
        ctx.writeAndFlush(buf).addListener(f -> {
            if (f.cause() != null) {
                var ses = session;

                if (ses != null) {
                    // Failed to send a message due to connection loss.
                    // Save the message to the session and resend on reconnect.
                    buf.retain();
                    buf.resetReaderIndex();
                    ses.send(buf);
                }
            }
        });
    }

    private void writeError(long requestId, Throwable err, ChannelHandlerContext ctx) {
        LOG.error("Error processing client request", err);

        var packer = getPacker(ctx.alloc());

        try {
            assert err != null;

            packer.packInt(ServerMessageType.RESPONSE);
            packer.packLong(requestId);

            var errorCode = err instanceof ClientTableIdDoesNotExistException
                    ? ClientErrorCode.TABLE_ID_DOES_NOT_EXIST
                    : ClientErrorCode.FAILED;

            packer.packInt(errorCode);

            String msg = err.getMessage();

            if (msg == null) {
                msg = err.getClass().getName();
            }

            packer.packString(msg);

            session.send(packer.getBuffer());
        } catch (Throwable t) {
            packer.close();
            exceptionCaught(ctx, t);
        }
    }

    private ClientMessagePacker getPacker(ByteBufAllocator alloc) {
        // Outgoing messages are released on write.
        return new ClientMessagePacker(alloc.buffer());
    }

    private ClientMessageUnpacker getUnpacker(ByteBuf buf) {
        return new ClientMessageUnpacker(buf);
    }

    private void processOperation(ChannelHandlerContext ctx, ClientMessageUnpacker in, ClientMessagePacker out) {
        long requestId = -1;

        try {
            final int opCode = in.unpackInt();
            requestId = in.unpackLong();

            out.packInt(ServerMessageType.RESPONSE);
            out.packLong(requestId);
            out.packInt(ClientErrorCode.SUCCESS);

            var fut = processOperation(in, out, opCode);

            if (fut == null) {
                // Operation completed synchronously.
                session.send(out.getBuffer());
            } else {
                final var reqId = requestId;

                fut.whenComplete((Object res, Object err) -> {
                    if (err != null) {
                        out.close(); // TODO: Don't close, reuse!
                        writeError(reqId, (Throwable) err, ctx);
                    } else {
                        session.send(out.getBuffer());
                    }
                });
            }
        } catch (Throwable t) {
            // TODO IGNITE-16928 Thin 3.0: Implement sessions for Java client
            // Once we got a complete message from the client, we must prepare the response and keep it until the session expires.
            out.close();  // TODO: Don't close, reuse!

            writeError(requestId, t, ctx);
        }
    }

    private CompletableFuture processOperation(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            int opCode
    ) throws IgniteInternalCheckedException {
        switch (opCode) {
            case ClientOp.HEARTBEAT:
                return null;

            case ClientOp.TABLES_GET:
                return ClientTablesGetRequest.process(out, igniteTables);

            case ClientOp.SCHEMAS_GET:
                return ClientSchemasGetRequest.process(in, out, igniteTables);

            case ClientOp.TABLE_GET:
                return ClientTableGetRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_UPSERT:
                return ClientTupleUpsertRequest.process(in, igniteTables, session.resources());

            case ClientOp.TUPLE_GET:
                return ClientTupleGetRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_UPSERT_ALL:
                return ClientTupleUpsertAllRequest.process(in, igniteTables, session.resources());

            case ClientOp.TUPLE_GET_ALL:
                return ClientTupleGetAllRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_GET_AND_UPSERT:
                return ClientTupleGetAndUpsertRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_INSERT:
                return ClientTupleInsertRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_INSERT_ALL:
                return ClientTupleInsertAllRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_REPLACE:
                return ClientTupleReplaceRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_REPLACE_EXACT:
                return ClientTupleReplaceExactRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_GET_AND_REPLACE:
                return ClientTupleGetAndReplaceRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_DELETE:
                return ClientTupleDeleteRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_DELETE_ALL:
                return ClientTupleDeleteAllRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_DELETE_EXACT:
                return ClientTupleDeleteExactRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_DELETE_ALL_EXACT:
                return ClientTupleDeleteAllExactRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_GET_AND_DELETE:
                return ClientTupleGetAndDeleteRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.TUPLE_CONTAINS_KEY:
                return ClientTupleContainsKeyRequest.process(in, out, igniteTables, session.resources());

            case ClientOp.SQL_EXEC:
                return ClientSqlExecuteRequest.execute(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_EXEC_BATCH:
                return ClientSqlExecuteBatchRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_EXEC_PS_BATCH:
                return ClientSqlPreparedStmntBatchRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_NEXT:
                return ClientSqlFetchRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_CURSOR_CLOSE:
                return ClientSqlCloseRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_TABLE_META:
                return ClientSqlTableMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_COLUMN_META:
                return ClientSqlColumnMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_SCHEMAS_META:
                return ClientSqlSchemasMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_PK_META:
                return ClientSqlPrimaryKeyMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.SQL_QUERY_META:
                return ClientSqlQueryMetadataRequest.process(in, out, jdbcQueryEventHandler);

            case ClientOp.TX_BEGIN:
                return ClientTransactionBeginRequest.process(out, igniteTransactions, session.resources());

            case ClientOp.TX_COMMIT:
                return ClientTransactionCommitRequest.process(in, session.resources());

            case ClientOp.TX_ROLLBACK:
                return ClientTransactionRollbackRequest.process(in, session.resources());

            case ClientOp.COMPUTE_EXECUTE:
                return ClientComputeExecuteRequest.process(in, out, compute, clusterService);

            case ClientOp.COMPUTE_EXECUTE_COLOCATED:
                return ClientComputeExecuteColocatedRequest.process(in, out, compute, igniteTables);

            case ClientOp.CLUSTER_GET_NODES:
                return ClientClusterGetNodesRequest.process(out, clusterService);

            default:
                throw new IgniteException("Unexpected operation code: " + opCode);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error(cause.getMessage(), cause);

        ctx.close();
    }
}
