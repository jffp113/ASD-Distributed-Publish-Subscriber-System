package precistante;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.function.UnaryOperator;

public class PrecistantList<E> implements List<E> {

    List<E> list;

    private PrecistantList(List l){
        this.list = l;
    }

    public static List getPrecistanteLinkedList(){
        return new PrecistantList<>(new LinkedList());
    }
    public static List getPrecistanteArrayList(int size){
        return new PrecistantList<>(new ArrayList(size));
    }

    @Override
    public int size() {
        return this.list.size();
    }

    @Override
    public boolean isEmpty() {
        return this.list.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.list.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return this.list.iterator();
    }

    @Override
    public Object[] toArray() {
        return this.list.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.list.toArray(a);
    }

    @Override
    public boolean add(E e) {
        //TODO add precistence
        return this.list.add(e);
    }

    @Override
    public boolean remove(Object o) {
        //Not implemented
        throw new NotImplementedException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.list.contains(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return this.list.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        return this.list.addAll(index,c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        //Not implemented
        throw new NotImplementedException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        //Not implemented
        throw new NotImplementedException();
    }

    @Override
    public void clear() {
        //Not implemented
        throw new NotImplementedException();
    }

    @Override
    public E get(int index) {
        return this.list.get(index);
    }

    @Override
    public E set(int index, E element) {
        //Not implemented
        throw new NotImplementedException();
    }

    @Override
    public void add(int index, E element) {
        //Not implemented
        throw new NotImplementedException();
    }

    @Override
    public E remove(int index) {
        return null;
    }

    @Override
    public int indexOf(Object o) {
        return 0;
    }

    @Override
    public int lastIndexOf(Object o) {
        return 0;
    }

    @Override
    public ListIterator<E> listIterator() {
        return null;
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return null;
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        return null;
    }

    @Override
    public void replaceAll(UnaryOperator<E> operator) {

    }

    @Override
    public void sort(Comparator<? super E> c) {

    }

    @Override
    public Spliterator<E> spliterator() {
        return null;
    }
}
