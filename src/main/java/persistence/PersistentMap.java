package persistence;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.util.*;

public class PersistentMap<K extends Serializable, V extends Serializable> implements Map<K, V> {

    private Map<K, V> map;
    private ObjectOutputStream out;
    private File f;

    public PersistentMap(Map<K, V> map, String fileName) throws Exception {
        this.map = map;
        f = new File(fileName);

        if (!f.exists()) {
            f.createNewFile();
        } else {
            fillMap();
        }

        this.out = new ObjectOutputStream(new FileOutputStream(f));
    }

    private void fillMap() throws Exception {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(f));

        try {
            while (true) {
                Entry<K, V> entry = (Entry) in.readObject();
                map.put(entry.getKey(), entry.getValue());
            }
        } catch (IOException e) {
            return;
        }

    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        Entry<K, V> entry = new MyEntry<>(key, value);

        try {
            out.writeObject(entry);
        } catch (IOException e) {
            return null;
        }

        return map.put(key, value);
    }

    @Override
    public V remove(Object key) {
        throw new NotImplementedException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new NotImplementedException();
    }

    @Override
    public void clear() {
        f.deleteOnExit();
        try {
            f.createNewFile();
            out = new ObjectOutputStream(new FileOutputStream(f));
            this.map.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

}
