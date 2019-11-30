package protocols.multipaxos;

import io.netty.buffer.ByteBuf;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.Objects;

public class Operation implements Comparable<Operation>, Serializable {

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

    public Operation(int id, Type type, Content content) {
        this.id = id;
        this.instance = -1;
        this.sequenceNumber = -1;
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
            case WRITE:
                content = WriteContent.deserialize(in);
                break;
            case NO_OP:
                content = new NoContent();
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
            case WRITE:
                ((WriteContent) content).serialize(out);
                break;
            case NO_OP:
                break;
            default:
                System.err.println("YOO type is wrong!");
        }

    }

//    public String serializeToString(){
//        return String.format("%d|%d|%d|%d|[%s]", id,instance,type,sequenceNumber,); //TODO
//    }
//
//    public Operation deserializeToString(){
//        return null;
//    }

    public int serializedSize() {
        int contentSize = 0;
        switch (type) {
            case ADD_REPLICA:
            case REMOVE_REPLICA:
                contentSize = ((MembershipUpdateContent) content).serializedSize();
                break;
            case WRITE:
                contentSize = ((WriteContent) content).serializedSize();
                break;
            case NO_OP:
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return id == operation.id &&
                instance == operation.instance &&
                type == operation.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, instance, type);
    }

    @Override
    public int compareTo(Operation o) {
        return Integer.compare(this.getInstance(), o.getInstance());
    }

    public enum Type {
        ADD_REPLICA, REMOVE_REPLICA, WRITE, NO_OP
    }
}
