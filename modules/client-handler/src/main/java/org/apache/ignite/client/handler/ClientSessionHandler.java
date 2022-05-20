package org.apache.ignite.client.handler;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public final class ClientSessionHandler {
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    private final ConcurrentHashMap<UUID, ClientSession> sessions = new ConcurrentHashMap<>();

    public ClientSession getOrCreateSession(UUID existingSessionId) {
        if (existingSessionId != null) {
            // TODO: Check expiry here or rely on timer?
            ClientSession existingSession = sessions.get(existingSessionId);

            if (existingSession != null) {
                return existingSession;
            }
        }

        var session = new ClientSession();

        sessions.put(session.id(), session);

        return session;
    }

    public ClientSession getSession(UUID id) {
        // TODO: Check if session has timed out.
        // TODO: Periodically check timed out sessions and release their resources? Or start a timer for every session?
        return sessions.get(id);
    }
}
