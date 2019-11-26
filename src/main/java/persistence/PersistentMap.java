package persistence;


import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PersistentMap<K extends Serializable> {

    private static final String PATH = "map/";
    private static final int IN_MEMORY = 10;

    BlockingQueue<MyEntry<K, String>> vBlockingQueue;
    private Map<K, File> mapFiles;
    private Map<K, Integer> currentSeq;
    private ObjectOutputStream out;
    private String id;
    private Map<K, Queue<String>> map;

    public PersistentMap(String id) {
        this.map = new HashMap<>(64);
        this.mapFiles = new HashMap<>(64);
        this.currentSeq = new HashMap<>(64);
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

    public PersistentMap(String id, HashMap<K, byte[]> state) throws Exception {
        this(id);
        loadState(state);
    }

    private void loadState(HashMap<K, byte[]> state) {

    }

    public Map<K, byte[]> getState() throws IOException {
        Map<K, byte[]> result = new HashMap<>(map.size());
        for (Map.Entry<K, File> entry : mapFiles.entrySet()) {
            result.put(entry.getKey(), Files.readAllBytes(entry.getValue().toPath()));
        }
        return result;
    }

    private void writeToFile(MyEntry<K, String> toWrite) throws Exception {
        File f = mapFiles.get(toWrite.getKey());
        BufferedWriter writer = new BufferedWriter(
                new FileWriter(f, true));
        writer.write(toWrite.getValue());
        writer.newLine();
        writer.flush();
    }

    public List<String> get(K topic, int offset, int max) throws Exception {
        List<String> result = new LinkedList<>();
        Integer current = currentSeq.get(topic);

        if (current - IN_MEMORY <= offset) {
            int pos = current - map.get(topic).size() + 1;
            for (String value : map.get(topic)) {
                if (pos >= offset) {
                    if (pos <= max)
                        result.add(value);
                }

                pos++;
            }
        } else {
            BufferedReader reader = new BufferedReader(new FileReader(mapFiles.get(topic)));
            int i = 1;
            String o;
            while ((o = reader.readLine()) != null) {
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

    public List<String> get(K topic, int offset) throws Exception {
        return get(topic, offset, currentSeq.get(topic));
    }

    public int put(K key, String value) {
        Queue<String> list = map.get(key);
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


}
