package protocols.dht2.data.Handler;

import babel.handlers.ProtocolMessageHandler;
import babel.protocol.event.ProtocolMessage;
import network.ISerializer;

public interface MessageHandler {
    void handle(short id, ProtocolMessageHandler handler, ISerializer<? extends ProtocolMessage> serializer );
}
