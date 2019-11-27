package protocols.multipaxos;

import io.netty.buffer.ByteBuf;

import java.net.UnknownHostException;

public class Operation {

    private int id;
    private int instance;
    private Type type;
    private Content content;

    public Operation(int id, int instance, Type type, Content content) {
        this.id = id;
        this.instance = instance;
        this.type = type;
        this.content = content;
    }

    public void serialize(ByteBuf out) {
        out.writeInt(id);
        out.writeInt(instance);
        out.writeInt(type.ordinal());
        switch (type) {
            case ADD_REPLICA:
            case REMOVE_REPLICA:((MembershipUpdateContent) content).serialize(out);
                break;
            default:
                System.err.println("YOO type is wrong!");
        }

    }

    public static Operation deserialize(ByteBuf in) throws UnknownHostException {
        int id = in.readInt();
        int instance = in.readInt();
        Type type = Type.values()[in.readInt()];
        Content content;
        switch (type) {
            case ADD_REPLICA:
            case REMOVE_REPLICA:
                content = MembershipUpdateContent.deserialize(in);
                break;
            default:
                content = null;
                System.err.println("YOO type is wrong!");
        }

        return new Operation(id, instance, type, content);
    }

    public int serializedSize() {
        int contentSize = 0;
        switch (type) {
            case ADD_REPLICA:
            case REMOVE_REPLICA:
                contentSize = ((MembershipUpdateContent) content).serializedSize();
                break;
            default:
                System.err.println("YOO type is wrong!");
        }
        return contentSize + 3 * Integer.BYTES;
    }

    public int getId() {
        return id;
    }

    public int getInstance() {
        return instance;
    }

    public Content getContent() {
        return content;
    }

    public Type getType() {
        return type;
    }

    /*public static Operation deserialize(ByteBuf in) {
                        byte[] topicBytes = new byte[in.readInt()];
                        in.readBytes(topicBytes);
                        int size = in.readInt();
                        List<String> messages = new LinkedList<>();

                        for (int i = 0; i < size; i++) {
                            byte[] messageBytes = new byte[in.readInt()];
                            in.readBytes(messageBytes);
                            messages.add(new String(messageBytes));
                        }

                        return new Operation(new String(topicBytes), messages);
                    }

                    public String getTopic() {
                        return topic;
                    }

                    public List<String> getMessages() {
                        return messages;
                    }

                    public void serialize(ByteBuf out) {
                        out.writeInt(this.topic.length());
                        out.writeBytes(this.topic.getBytes());
                        out.writeInt(this.messages.size());
                        for (String message : messages) {
                            out.writeInt(message.length());
                            out.writeBytes(message.getBytes());
                        }
                    }

                    public int serializedSize(){
                        int size = 2 * Integer.BYTES + this.topic.length();
                        for (String msg : this.messages) {
                            size += msg.length() + Integer.BYTES;
                        }

                        return size;
                    }
                */
    enum Type {
        ADD_REPLICA, REMOVE_REPLICA, WRITE
    }
}
