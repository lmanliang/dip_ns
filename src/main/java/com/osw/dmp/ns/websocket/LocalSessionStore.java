package com.osw.dmp.ns.websocket;

import org.springframework.web.reactive.socket.WebSocketSession;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Local memory storage for WebSocket sessions
 * Maps eventId -> WebSocketSession for fast lookup
 */
public class LocalSessionStore {

    // eventId → WebSocket Session
    private final ConcurrentHashMap<String, WebSocketSession> eventIdToSession = new ConcurrentHashMap<>();

    // Session ID → subscribed eventIds (for cleanup on disconnect)
    private final ConcurrentHashMap<String, Set<String>> sessionToEventIds = new ConcurrentHashMap<>();

    // Session ID → WebSocketSession (for direct access)
    private final ConcurrentHashMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    /**
     * Register a new session
     */
    public void registerSession(WebSocketSession session) {
        sessions.put(session.getId(), session);
        sessionToEventIds.put(session.getId(), new CopyOnWriteArraySet<>());
    }

    /**
     * Subscribe session to eventId
     */
    public void subscribe(String eventId, WebSocketSession session) {
        eventIdToSession.put(eventId, session);
        sessionToEventIds.computeIfAbsent(session.getId(), k -> new CopyOnWriteArraySet<>()).add(eventId);
    }

    /**
     * Unsubscribe session from eventId
     */
    public void unsubscribe(String eventId, WebSocketSession session) {
        eventIdToSession.remove(eventId, session);
        Set<String> eventIds = sessionToEventIds.get(session.getId());
        if (eventIds != null) {
            eventIds.remove(eventId);
        }
    }

    /**
     * Get session by eventId
     */
    public Optional<WebSocketSession> getSessionByEventId(String eventId) {
        WebSocketSession session = eventIdToSession.get(eventId);
        if (session != null && session.isOpen()) {
            return Optional.of(session);
        }
        return Optional.empty();
    }

    /**
     * Get session by session ID
     */
    public Optional<WebSocketSession> getSession(String sessionId) {
        return Optional.ofNullable(sessions.get(sessionId));
    }

    /**
     * Get all eventIds subscribed by a session
     */
    public Set<String> getEventIdsBySession(String sessionId) {
        return sessionToEventIds.getOrDefault(sessionId, Set.of());
    }

    /**
     * Remove session and all its subscriptions
     */
    public Set<String> removeSession(WebSocketSession session) {
        sessions.remove(session.getId());
        Set<String> eventIds = sessionToEventIds.remove(session.getId());
        if (eventIds != null) {
            eventIds.forEach(eventIdToSession::remove);
        }
        return eventIds != null ? eventIds : Set.of();
    }

    /**
     * Check if eventId has active subscription
     */
    public boolean hasSubscription(String eventId) {
        WebSocketSession session = eventIdToSession.get(eventId);
        return session != null && session.isOpen();
    }

    /**
     * Get total connection count
     */
    public int getConnectionCount() {
        return (int) sessions.values().stream().filter(WebSocketSession::isOpen).count();
    }

    /**
     * Get total subscription count
     */
    public int getSubscriptionCount() {
        return eventIdToSession.size();
    }
}
