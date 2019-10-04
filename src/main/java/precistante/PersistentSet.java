package precistante;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;

public class PersistentSet<E extends Serializable> implements Set<E> {

    private Set<E> set;
    private ObjectOutputStream out;
    private File f;

    public PersistentSet(Set<E> set, String fileName) throws Exception {
        this.set = set;
        f = new File(fileName);//todo

        if(!f.exists())
            f.createNewFile();
        else
            fillSet();

        this.out = new ObjectOutputStream(new FileOutputStream(f));
    }

    private void fillSet() throws Exception {
        ObjectInputStream in = new ObjectInputStream(new FileInputStream(f));

        try{
            while(true)
                set.add((E)in.readObject());
        }catch(IOException e){
            return;
        }

    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return set.contains(o);
    }

    @Override
    public Iterator iterator() {
        return set.iterator();
    }

    @Override
    public Object[] toArray() {
        return set.toArray();
    }

    @Override
    public Object[] toArray(Object[] a) {
        return set.toArray(a);
    }

    @Override
    public boolean add(E o) {
        try {
            out.writeObject(o);
        } catch (IOException e) {
            return false;
        }

        return set.add(o);
    }

    @Override
    public boolean remove(Object o) {
         throw new NotImplementedException();
    }

    @Override
    public boolean containsAll(Collection c) {
        return set.contains(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean result = false;
        for(E element : c){
            boolean r = this.add(element);

            if(r)
                result = r;
        }
        return result;
    }

    @Override
    public boolean retainAll(Collection c) {
        throw new NotImplementedException();
    }

    @Override
    public boolean removeAll(Collection c) {
        throw new NotImplementedException();
    }

    @Override
    public void clear() {
        f.deleteOnExit();
        try {
            f.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Spliterator spliterator() {
        return set.spliterator();
    }
}
