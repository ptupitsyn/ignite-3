package org.apache.ignite.client.handler;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class ClientSessionHandler {
    private final ConcurrentHashMap<UUID, ClientSession> sessions = new ConcurrentHashMap<>();

    public ClientSession createSession() {
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
