package com.cachekit.utils.struct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@SuppressWarnings(
        {"unchecked", "rawtypes"})
public class DoubleLinkedList<T extends DoubleLinkedListNode> {
    private static final Log log = LogFactory.getLog(DoubleLinkedList.class);

    private int size = 0;

    private T first;

    private T last;

    public DoubleLinkedList() {
        super();
    }

    public synchronized void addFirst(T me) {
        //last为null表示没有节点
        if (last == null) {
            last = me;
        } else {
            first.prev = me;
            me.next = first;
        }
        first = me;
        size++;
    }

    public synchronized void addFirst2(T me) {
        //first为null表示没有节点
        if (first == null) {
            last = me;
        } else {
            first.prev = me;
            me.next = first;
        }
        first = me;
        size++;
    }

    public synchronized void addLast(T me) {
        if (first == null) {
            first = me;
        } else {
            last.next = me;
            me.prev = last;
        }
        last = me;
        size++;
    }

    public synchronized T getFirst() {
        if (log.isDebugEnabled()) {
            log.debug("return first node");
        }
        return first;
    }

    public synchronized T getLast() {
        if (log.isDebugEnabled()) {
            log.debug("return last node");
        }
        return last;
    }

    public synchronized void makeFirst(T ln) {
        if (ln.prev == null) {
            return;
        }
        ln.prev.next = ln.next;

        if (ln.next == null) {
            last = (T) ln.prev;
            last.next = null;
        } else {
            ln.next.prev = ln.prev;
        }
        first.prev = ln;
        ln.next = first;
        ln.prev = null;
        first = ln;
    }

    /**
     * 有问题
     */
    public synchronized void makeLast_back(T ln) {
        if (ln.next == null) {
            return;
        }
        if (ln.prev != null) {
            ln.prev.next = ln.next;
        } else {
            //是不是 first=first.next;?
            first = last;
        }
        if (last != null) {
            last.next = ln;
        }
        ln.prev = last;
        ln.next = null;
        last = ln;
    }

    public synchronized void makeLast(T ln) {
        if (ln.next == null) {
            return;
        }
        if (ln.prev != null) {
            ln.prev.next = ln.next;
            ln.next.prev = ln.prev;
        } else {
            first = (T) ln.next;
        }
        if (last != null) {
            last.next = ln;
        }
        ln.prev = last;
        ln.next = null;
        last = ln;
    }

    /**
     * 只需删除一个引用即可，不形成互相引用
     */
    public synchronized void removeAll() {
        //只删除了pre引用，next引用没有删除？
        for (T me = first; me != null; ) {
            if (me.prev != null) {
                me.prev = null;
            }
            T next = (T) me.next;
            me = next;
        }
        first = last = null;
        size = 0;
    }

    public synchronized boolean remove(T me) {
        if (log.isDebugEnabled()) {
            log.debug("remove node");
        }
        if (me.next == null) {
            if (me.prev == null) {
                if (me == first && me == last) {
                    first = last = null;
                }
            } else {
                last = (T) me.prev;
                last.next = null;
                me.prev = null;
            }
        } else if (me.prev == null) {
            first = (T) me.next;
            first.prev = null;
            me.next = null;
        } else {
            me.prev.next = me.next;
            me.next.prev = me.prev;
            me.prev = me.next = null;
        }
        size--;
        return true;
    }

    public synchronized T removeLast() {
        if (log.isDebugEnabled()) {
            log.debug("remove last node");
        }
        T temp = last;
        if (last != null) {
            remove(last);
        }
        return temp;
    }

    public synchronized int size() {
        return size;
    }

    public synchronized void debugDumpEntries() {
        if (log.isDebugEnabled()) {
            log.debug("dump Entries");
            for (T me = first; me != null; me = (T) me.next) {
                log.debug("dump Entries> payload= '" + me.getPayload() + "'");
            }
        }
    }
}
