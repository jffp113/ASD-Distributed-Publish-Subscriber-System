package protocols.dht.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import protocols.dissemination.message.ScribeMessage;

import java.net.UnknownHostException;

public class ForwardMessage extends ProtocolMessage {
    public static short MSG_CODE = 14400;

    private ScribeMessage scribeMessage;

    public ForwardMessage(ScribeMessage scribeMessage) {
        super(MSG_CODE);
        this.scribeMessage = scribeMessage;
    }

    public ScribeMessage getScribeMessage() {
        return scribeMessage;
    }

    public static final ISerializer<ForwardMessage> serializer = new ISerializer<ForwardMessage>() {

        @Override
        public void serialize(ForwardMessage forwardMessage, ByteBuf byteBuf) {
            ScribeMessage.serializer.serialize(forwardMessage.scribeMessage, byteBuf);
        }

        @Override
        public ForwardMessage deserialize(ByteBuf byteBuf) throws UnknownHostException {
            ScribeMessage scribeMessage = ScribeMessage.serializer.deserialize(byteBuf);
            return new ForwardMessage(scribeMessage);
        }

        @Override
        public int serializedSize(ForwardMessage forwardMessage) {
            return ScribeMessage.serializer.serializedSize(forwardMessage.scribeMessage);
        }
    };
}
