package com.sandrovsky;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class SlotBasedSum<T> implements Serializable {

    private static final long serialVersionUID = 4858185737378394432L;

    private final Map<T, long[]> objToSums = new HashMap<T, long[]>();
    private final int numSlots;

    public SlotBasedSum(int numSlots) {
        if (numSlots <= 0) {
            throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
        }
        this.numSlots = numSlots;
    }

    public void add(T obj, int slot, long value) {
        long[] sums = objToSums.get(obj);
        if (sums == null) {
            sums = new long[this.numSlots];
            objToSums.put(obj, sums);
        }
        sums[slot] += value;
    }

    public long getSum(T obj, int slot) {
        long[] sums = objToSums.get(obj);
        if (sums == null) {
            return 0;
        }
        else {
            return sums[slot];
        }
    }

    public Map<T, Long> getSums() {
        Map<T, Long> result = new HashMap<T, Long>();
        for (T obj : objToSums.keySet()) {
            result.put(obj, computeTotalSum(obj));
        }
        return result;
    }

    private long computeTotalSum(T obj) {
        long[] curr = objToSums.get(obj);
        long total = 0;
        for (long l : curr) {
            total += l;
        }
        return total;
    }

    /**
     * Reset the slot count of any tracked objects to zero for the given slot.
     *
     * @param slot
     */
    public void wipeSlot(int slot) {
        for (T obj : objToSums.keySet()) {
            resetSlotCountToZero(obj, slot);
        }
    }

    private void resetSlotCountToZero(T obj, int slot) {
        long[] sums = objToSums.get(obj);
        sums[slot] = 0;
    }

    private boolean shouldBeRemoved(T obj) {
        return computeTotalSum(obj) == 0;
    }

    /**
     * Remove any object from the counter whose total count is zero (to free up memory).
     */
    public void wipeZeros() {
        Set<T> objToBeRemoved = new HashSet<T>();
        for (T obj : objToSums.keySet()) {
            if (shouldBeRemoved(obj)) {
                objToBeRemoved.add(obj);
            }
        }
        for (T obj : objToBeRemoved) {
            objToSums.remove(obj);
        }
    }

}