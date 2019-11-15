package protocols.dht2.data;

import io.netty.buffer.ByteBuf;
import network.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.PropertiesUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

public class ID {
    final static Logger logger = LogManager.getLogger(ID.class.getName());

    private static final String DISPERSION_ALGORITHM = "SHA1";
    private static final int MAX_BITS_OF_ID = PropertiesUtils.getPropertyAsInt(PropertiesUtils.MAX_BITS_OF_ID_PROPERTY);
    private static final int MAX_AS_INT = (int) Math.pow(2, MAX_BITS_OF_ID);
    private static final ID MAX_ID = new ID((int)Math.pow(2,MAX_BITS_OF_ID));//TODO is max id negative
    private static final ID MIN_ID = new ID(0);

    private Integer idAsInteger;

    public ID(Host host) {
        this(host.toString());
    }

    public ID(String seed) {
        idAsInteger = calculateId(seed);
    }

    protected ID(int id) {
        idAsInteger = id;
    }


    public void serialize(ByteBuf out) {
        out.writeInt(idAsInteger);
    }

    public static ID deserialize(ByteBuf in) {
        return new ID(in.readInt());
    }

    public static int serializeSize(){
        return Integer.BYTES;
    }

    @Override
    public String toString() {
        return idAsInteger.toString();
    }


    public final boolean isInInterval(ID fromID, ID toID) {
        if (fromID.equals(toID)) {
            return (!this.equals(fromID));
        }

        if (fromID.compareTo(toID) < 0) {
            return (this.compareTo(fromID) > 0 && this.compareTo(toID) < 0);
        }

        return ((!fromID.equals(MAX_ID) && this.compareTo(fromID) > 0 && this
                .compareTo(MAX_ID) <= 0) ||

                (!MIN_ID.equals(toID) && this.compareTo(MIN_ID) >= 0 && this
                        .compareTo(toID) < 0));
    }

    private Integer calculateId(String seed) {
        try {
            String code = new String(MessageDigest.getInstance(DISPERSION_ALGORITHM).digest(seed.getBytes()));
            return Math.abs(code.hashCode() % MAX_AS_INT);
        } catch (NoSuchAlgorithmException e) {
            logger.error(e);
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ID id = (ID) o;
        return Objects.equals(idAsInteger, id.idAsInteger);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idAsInteger);
    }

    public int compareTo(ID toID) {
        return idAsInteger.compareTo(toID.idAsInteger);
    }

    public static int maxIDSize(){
        return MAX_AS_INT;
    }

    public ID sumID(ID id){
        return new ID((this.idAsInteger+ id.idAsInteger)%maxIDSize());
    }
}
