package com.sandrovsky;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import javax.websocket.*;
import java.io.IOException;
import java.net.URI;

@ClientEndpoint
public class BlockchainInfoClient {
    private final String SOCKET_URI = "wss://ws.blockchain.info/inv";
    private static final Logger LOG = org.apache.log4j.Logger.getLogger(BlockchainInfoClient.class);

    private StringBuilder messageBuffer = new StringBuilder();
    private Session userSession;
    private MessageHandler messageHandler;

    public BlockchainInfoClient() {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            container.connectToServer(this, URI.create(SOCKET_URI));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @OnOpen
    public void onOpen(final Session session) throws IOException {
        session.setMaxIdleTimeout(0);
        session.setMaxBinaryMessageBufferSize(16384);
        session.setMaxTextMessageBufferSize(16384);

        JSONObject subscriptionMessage = new JSONObject();
        subscriptionMessage.put("op", "unconfirmed_sub");
        session.getBasicRemote().sendText(subscriptionMessage.toString());

        LOG.info(subscriptionMessage.toString());

        userSession = session;

        LOG.info("Subscribed for unconfirmed transactions from Blockchain.info.");
    }

    @OnClose
    public void onClose(final Session session, final CloseReason reason) {
        userSession = null;
        // try to reconnect

        LOG.info("Connection to Blockchain.info is closed");

    }

    @OnMessage
    public void onMessage(final String message, boolean isLastPartOfMessage) {
        messageBuffer.append(message);
        if (isLastPartOfMessage) {
            try {
                if (messageHandler != null) {
                    messageHandler.handleMessage(messageBuffer.toString());
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            } finally {
                messageBuffer = new StringBuilder();
            }
        }

        LOG.info("Message received from Blockchain.info");
    }

    public void addMessageHandler(final MessageHandler msgHandler) {
        messageHandler = msgHandler;
    }

    public static interface MessageHandler {
        public void handleMessage(String message);
    }
}