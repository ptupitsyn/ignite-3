package org.apache.ignite.client.handler;

import io.netty.buffer.ByteBuf;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

public final class ClientSessionHandler {
    /** Executor. */
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    /** Sessions. */
    private final ConcurrentHashMap<UUID, ClientSession> sessions = new ConcurrentHashMap<>();

    private final int connectionRestoreTimeout;

    public ClientSessionHandler(int connectionRestoreTimeout) {
        this.connectionRestoreTimeout = connectionRestoreTimeout;
    }

    public ClientSession getOrCreateSession(UUID existingSessionId, Consumer<ByteBuf> messageConsumer) {
        if (existingSessionId != null) {
            ClientSession existingSession = sessions.get(existingSessionId);

            if (existingSession != null && existingSession.activate(messageConsumer)) {
                return existingSession;
            }
        }

        var session = new ClientSession(scheduledExecutor, connectionRestoreTimeout, messageConsumer, this::onSessionClosed);

        sessions.put(session.id(), session);

        return session;
    }

    private void onSessionClosed(ClientSession session) {
        sessions.remove(session.id());
    }
}
