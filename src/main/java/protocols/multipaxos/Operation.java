package protocols.multipaxos;

import io.netty.buffer.ByteBuf;

import java.net.UnknownHostException;

public class Operation {

    private int id;
    private int instance;
    private int sequenceNumber;
    private Type type;
    private Content content;

    public Operation(int id, int instance, int sequenceNumber, Type type, Content content) {
        this.id = id;
        this.instance = instance;
        this.sequenceNumber = sequenceNumber;
        this.type = type;
        this.content = content;
    }

    public static Operation deserialize(ByteBuf in) throws UnknownHostException {
        int id = in.readInt();
        int instance = in.readInt();
        Type type = Type.values()[in.readInt()];
        int sequenceNumber = in.readInt();
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

        return new Operation(id, instance, sequenceNumber, type, content);
    }

    public void serialize(ByteBuf out) {
        out.writeInt(id);
        out.writeInt(instance);
        out.writeInt(type.ordinal());
        out.writeInt(sequenceNumber);
        switch (type) {
            case ADD_REPLICA:
            case REMOVE_REPLICA:
                ((MembershipUpdateContent) content).serialize(out);
                break;
            default:
                System.err.println("YOO type is wrong!");
        }

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
        return contentSize + 4 * Integer.BYTES;
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

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    enum Type {
        ADD_REPLICA, REMOVE_REPLICA, WRITE
    }
}
