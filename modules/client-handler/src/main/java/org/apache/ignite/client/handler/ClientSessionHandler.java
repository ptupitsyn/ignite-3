package org.apache.ignite.client.handler;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class ClientSessionHandler {
    /** Executor. */
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    /** Sessions. */
    private final ConcurrentHashMap<UUID, ClientSession> sessions = new ConcurrentHashMap<>();

    public ClientSession getOrCreateSession(UUID existingSessionId) {
        if (existingSessionId != null) {
            ClientSession existingSession = sessions.get(existingSessionId);

            // If session exists, and is not closed, then reset its timer and return.
            // If it is not activated within the timeout, it will be closed (e.g. handshake failed midway).
            // TODO: Timers are not reliable. Instead, we can lock the session until it is activated.
            if (existingSession != null && existingSession.scheduleExpiration()) {
                return existingSession;
            }
        }

        var session = new ClientSession(scheduledExecutor, this::onSessionClosed);

        sessions.put(session.id(), session);

        return session;
    }

    private void onSessionClosed(ClientSession session) {
        sessions.remove(session.id());
    }
}
