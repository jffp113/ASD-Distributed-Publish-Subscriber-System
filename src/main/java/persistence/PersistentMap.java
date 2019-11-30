package persistence;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PersistentMap<K extends Serializable, V extends Serializable> {

    private static final String PATH = "./map/";
    private static int IN_MEMORY = 10;

    BlockingQueue<MyEntry<K, V>> vBlockingQueue;
    private Map<K, File> mapFiles;
    private Map<K,Boolean> isFirstWrite;
    private Map<K, Integer> currentSeq;
    private ObjectOutputStream out;
    private String id;
    private Map<K, Queue<V>> map;

    public PersistentMap(String id) {
        this.map = new HashMap<>(64);
        this.mapFiles = new HashMap<>(64);
        this.currentSeq = new HashMap<>(64);
        this.isFirstWrite = new HashMap<>();
        vBlockingQueue = new LinkedBlockingQueue();
        this.id = id;

        new Thread(() -> {
            while (true) {
                try {
                    writeToFile(vBlockingQueue.take());

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    public Map<K, byte[]> getState() throws IOException {
        Map<K, byte[]> result = new HashMap<>(map.size());
        for (Map.Entry<K, File> entry : mapFiles.entrySet()) {
            result.put(entry.getKey(), Files.readAllBytes(entry.getValue().toPath()));
        }
        return result;
    }

    private void writeToFile(MyEntry<K, V> toWrite) throws Exception {
        File f = mapFiles.get(toWrite.getKey());
        ObjectOutputStream objectOutputStream  = new ObjectOutputStream(
                new FileOutputStream(f));

        if(isFirstWrite.getOrDefault(toWrite.getKey(),true)){
            isFirstWrite.put(toWrite.getKey(),false);
        }else{
            objectOutputStream = new AppendingObjectOutputStream(objectOutputStream);

        }

        objectOutputStream.writeObject(toWrite.getValue());
        objectOutputStream.flush();
        objectOutputStream.close();
    }

    public List<V> get(K topic, int offset, int max) throws Exception {
        List<V> result = new LinkedList<>();
        Integer current = currentSeq.get(topic);

        if (current - IN_MEMORY <= offset) {
            int pos = current - map.get(topic).size() + 1;
            for (V value : map.get(topic)) {
                if (pos >= offset) {
                    if (pos <= max)
                        result.add(value);
                }

                pos++;
            }
        } else {
            ObjectInputStream input = new ObjectInputStream(new FileInputStream(mapFiles.get(topic)))
                    ;
            int i = 1;
            V o;
            while ((o = (V)input.readObject()) != null) {
                if (offset <= i) {
                    if (i > max)
                        break;

                    result.add(o);
                }
                i++;
            }
        }

        return result;
    }

    public List<V> get(K topic, int offset) throws Exception {
        return get(topic, offset, currentSeq.get(topic));
    }

    public int put(K key, V value) {
        Queue<V> list = map.get(key);
        Integer seq = currentSeq.get(key);
        if (list == null) {
            list = new LinkedList<>();
            map.put(key, list);
            createNewFile(PATH + id + key, key);
            seq = new Integer(0);
        }
        list.add(value);
        askFileWriting(new MyEntry<>(key, value));
        currentSeq.put(key, seq + 1);
        map.put(key, list);
        if (list.size() > IN_MEMORY) {
            list.remove();
        }

        return seq + 1;
    }

    private void createNewFile(String path, K topic) {
        File f = new File(path);
        try {
            f.createNewFile();
            mapFiles.put(topic, f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void askFileWriting(MyEntry entry) {
        try {
            vBlockingQueue.put(entry);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void setState(Map<K, byte[]> state) {
        for(Map.Entry<K,byte[]> entry: state.entrySet()){
            createNewFile(PATH + id +entry.getKey(), entry.getKey());
            writeToFile(entry.getValue(),(String)entry.getKey());
            isFirstWrite.put(entry.getKey(),false);
        }
    }

    private void writeToFile(byte[] bytes,String key) {
        File f = mapFiles.get(key);
        FileOutputStream in;
        IN_MEMORY = 0;
        try {
            in = new FileOutputStream(f, true);
            in.write(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
