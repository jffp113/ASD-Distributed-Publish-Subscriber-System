package persistence;

public interface PersistentWritable {

    String serializeToString();

    PersistentWritable deserialize(String object);
}
