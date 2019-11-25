
import org.junit.Test;
import persistence.PersistentMap;
import persistence.PersistentWritable;

import java.util.List;
import java.util.Map;


public class EmptyTest {

    @Test
    public void test1() throws Exception{
        PersistentMap<String, StringWrittable> map = new PersistentMap<>("1");

        int a = map.put("Jorge", new StringWrittable("1"));
        map.put("Jorge", new StringWrittable("2"));
        map.put("Jorge", new StringWrittable("3"));
        map.put("Jorge", new StringWrittable("4"));
        map.put("Jorge", new StringWrittable("5"));
        map.put("Jorge", new StringWrittable("6"));
        map.put("Jorge", new StringWrittable("7"));
        map.put("Jorge", new StringWrittable("8"));
        map.put("Jorge", new StringWrittable("9"));
        map.put("Jorge", new StringWrittable("10"));
        map.put("Jorge", new StringWrittable("11"));
        map.put("Jorge", new StringWrittable("12"));
        map.put("Jorge", new StringWrittable("13"));
        Thread.sleep(1);
        List<PersistentWritable> list = map.get("Jorge",5,10);
        System.out.println(list);
        list = map.get("Jorge",1,10);
        Map<String,byte[]> result =  map.getState();
        System.out.println(list);
    }

    class StringWrittable implements PersistentWritable {
        public String ola;

        public StringWrittable(String ola) {
            this.ola = ola;
        }

        @Override
        public String serializeToString() {
            return ola;
        }

        @Override
        public PersistentWritable deserialize(String object) {
            return new StringWrittable(object);
        }

        @Override
        public String toString() {
            return ola;
        }
    }
}
