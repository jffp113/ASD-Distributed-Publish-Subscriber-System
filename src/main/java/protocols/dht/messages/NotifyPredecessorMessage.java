package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class NotifyPredecessorMessage extends ProtocolMessage {
    public static short MSG_CODE = 11113;

    public NotifyPredecessorMessage() {
        super(MSG_CODE);
    }

    public static final ISerializer<NotifyPredecessorMessage> serializer = new ISerializer<NotifyPredecessorMessage>() {
        @Override
        public void serialize(NotifyPredecessorMessage m, ByteBuf out) {
        }

        @Override
        public NotifyPredecessorMessage deserialize(ByteBuf in) {
            return new NotifyPredecessorMessage();
        }

        @Override
        public int serializedSize(NotifyPredecessorMessage m) {
            return 0;
        }
    };
}
