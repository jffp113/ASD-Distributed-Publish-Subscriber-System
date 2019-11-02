package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

public class HeartbeatMessage extends ProtocolMessage  {
    public static short MSG_CODE = 14321;

    public HeartbeatMessage() {
        super(MSG_CODE);
    }

    public static final ISerializer<HeartbeatMessage> serializer = new ISerializer<HeartbeatMessage>() {
        @Override
        public void serialize(HeartbeatMessage m, ByteBuf out) {
        }

        @Override
        public HeartbeatMessage deserialize(ByteBuf in) {
            return new HeartbeatMessage();
        }

        @Override
        public int serializedSize(HeartbeatMessage m) {
            return 0;
        }
    };
}
