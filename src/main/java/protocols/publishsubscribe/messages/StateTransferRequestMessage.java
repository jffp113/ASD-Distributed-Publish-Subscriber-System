package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;

public class StateTransferRequestMessage extends ProtocolMessage {
    public final static short MSG_CODE = 1203;
    public static final ISerializer<StateTransferRequestMessage> serializer = new ISerializer<StateTransferRequestMessage>() {
        @Override
        public void serialize(StateTransferRequestMessage m, ByteBuf out) {

        }

        @Override
        public StateTransferRequestMessage deserialize(ByteBuf in) throws UnknownHostException {
            return new StateTransferRequestMessage();
        }

        @Override
        public int serializedSize(StateTransferRequestMessage PSProtocolMessage) {
            return 0;
        }
    };

    public StateTransferRequestMessage() {
        super(MSG_CODE);
    }

}
