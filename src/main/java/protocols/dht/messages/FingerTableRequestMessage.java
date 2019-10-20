package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.io.Serializable;
import java.util.UUID;

public class FingerTableRequestMessage extends ProtocolMessage implements Serializable {

    public final static short MSG_CODE = 24301;

    public FingerTableRequestMessage() {
        super(FingerTableRequestMessage.MSG_CODE);
    }

    public static final ISerializer<FingerTableRequestMessage> serializer = new ISerializer<FingerTableRequestMessage>() {
        @Override
        public void serialize(FingerTableRequestMessage m, ByteBuf out) {
        }

        @Override
        public FingerTableRequestMessage deserialize(ByteBuf in) {
            return new FingerTableRequestMessage();
        }

        @Override
        public int serializedSize(FingerTableRequestMessage m) {
            return 0;
        }
    };
}
