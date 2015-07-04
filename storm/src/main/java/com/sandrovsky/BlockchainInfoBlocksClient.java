package com.sandrovsky;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.websocket.*;
import java.io.IOException;
import java.net.URI;

@ClientEndpoint
public class BlockchainInfoBlocksClient {
    private final String SOCKET_URI = "wss://ws.blockchain.info/inv";
    private final Logger LOG = LoggerFactory.getLogger(BlockchainInfoBlocksClient.class);

    private StringBuilder messageBuffer = new StringBuilder();
    private Session userSession;
    private MessageHandler messageHandler;

    public BlockchainInfoBlocksClient() {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            container.connectToServer(this, URI.create(SOCKET_URI));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @OnOpen
    public void onOpen(final Session userSession) throws IOException {
        userSession.setMaxIdleTimeout(0);
        userSession.setMaxBinaryMessageBufferSize(16384);
        userSession.setMaxTextMessageBufferSize(16384);

        JSONObject subscriptionMessage = new JSONObject();
        subscriptionMessage.put("op", "blocks_sub");
        userSession.getBasicRemote().sendText(subscriptionMessage.toString());

        LOG.info(subscriptionMessage.toString());

        this.userSession = userSession;

        LOG.info("Subscribed for unconfirmed transactions from Blockchain.info.");
    }

    @OnClose
    public void onClose(final Session userSession, final CloseReason reason) {
        this.userSession = null;
        // try to reconnect

        LOG.info("Connection to Blockchain.info is closed");

    }

    @OnMessage
    public void onMessage(final String message, boolean isLastPartOfMessage) {
        messageBuffer.append(message);
        if (isLastPartOfMessage) {
            try {
                if (messageHandler != null) {
                    messageHandler.handleMessage(message);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e, message);
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