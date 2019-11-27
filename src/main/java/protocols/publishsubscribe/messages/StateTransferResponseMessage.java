package protocols.publishsubscribe.messages;

import babel.protocol.event.ProtocolMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class StateTransferResponseMessage extends ProtocolMessage {
    public final static short MSG_CODE = 12000;
    public static final ISerializer<StateTransferResponseMessage> serializer = new ISerializer<StateTransferResponseMessage>() {
        @Override
        public void serialize(StateTransferResponseMessage m, ByteBuf out) {
            out.writeInt(m.state.size());
            for (Map.Entry<String, byte[]> entry : m.state.entrySet()) {
                out.writeInt(entry.getKey().length());
                out.writeBytes(entry.getKey().getBytes());
                out.writeInt(entry.getValue().length);
                out.writeBytes(entry.getValue());
            }

        }

        @Override
        public StateTransferResponseMessage deserialize(ByteBuf in) throws UnknownHostException {
            int size = in.readInt();
            Map<String, byte[]> state = new HashMap<>();
            for (int i = 0; i < size; i++) {
                byte[] topicBytes = new byte[in.readInt()];
                in.readBytes(topicBytes);
                byte[] contentBytes = new byte[in.readInt()];
                in.readBytes(contentBytes);
                state.put(new String(topicBytes), contentBytes);
            }

            return new StateTransferResponseMessage(state);
        }

        @Override
        public int serializedSize(StateTransferResponseMessage m) {
            int size = Integer.BYTES;
            for (Map.Entry<String, byte[]> entry : m.state.entrySet()) {
                size += entry.getKey().length() + entry.getValue().length + 2 * Integer.BYTES;
            }

            return size;
        }
    };
    private Map<String, byte[]> state;

    public StateTransferResponseMessage(Map<String, byte[]> state) {
        super(MSG_CODE);
        this.state = state;
    }

    public Map<String, byte[]> getState() {
        return state;
    }
}
