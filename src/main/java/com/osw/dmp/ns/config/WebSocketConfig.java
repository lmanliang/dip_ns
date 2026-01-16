package com.osw.dmp.ns.config;

import com.osw.dmp.ns.websocket.NotificationWebSocketHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.HashMap;
import java.util.Map;

/**
 * WebSocket Configuration
 */
@Configuration
@RequiredArgsConstructor
public class WebSocketConfig {

    private final NotificationWebSocketHandler notificationWebSocketHandler;

    @Value("${app.websocket.path:/ws/notifications}")
    private String websocketPath;

    @Bean
    public HandlerMapping webSocketHandlerMapping() {
        Map<String, Object> map = new HashMap<>();
        map.put(websocketPath, notificationWebSocketHandler);

        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setOrder(1);
        mapping.setUrlMap(map);
        return mapping;
    }

    @Bean
    public WebSocketHandlerAdapter handlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}
