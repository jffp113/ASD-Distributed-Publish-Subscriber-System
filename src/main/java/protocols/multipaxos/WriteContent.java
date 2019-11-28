package protocols.multipaxos;

import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

public class WriteContent implements Content {

    private String topic;
    private List<String> messages;

    public WriteContent(String topic, List<String> messages) {
        this.topic = topic;
        this.messages = messages;
    }

    public static Content deserialize(ByteBuf in) {
        byte[] topic = new byte[in.readInt()];
        in.readBytes(topic);
        int size = in.readInt();
        List<String> messages = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            byte[] msgBytes = new byte[in.readInt()];
            in.readBytes(msgBytes);
            messages.add(new String(msgBytes));
        }

        return new WriteContent(new String(topic), messages);
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void serialize(ByteBuf out) {
        out.writeInt(topic.length());
        out.writeBytes(topic.getBytes());
        out.writeInt(messages.size());
        for (String msg : messages) {
            out.writeInt(msg.length());
            out.writeBytes(msg.getBytes());
        }
    }

    public int serializedSize() {
        int size = 2 * Integer.BYTES + this.topic.length();
        for (String msg : messages) {
            size += msg.length() + Integer.BYTES;
        }

        return size;

    }
}
