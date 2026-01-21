package io.datadynamics.hive.udf.fastuitl.ints;

import org.junit.Test;
import static org.junit.Assert.*;
import java.util.Arrays;
import java.util.Random;

public class IntArraysTest {

    private final IntComparator naturalOrder = new AbstractIntComparator() {
        @Override
        public int compare(int k1, int k2) {
            return Integer.compare(k1, k2);
        }
    };

    @Test
    public void testQuickSortSmallArray() {
        // len < 7 case (selectionSort)
        int[] a = {5, 2, 9, 1, 5};
        int[] expected = {1, 2, 5, 5, 9};
        IntArrays.quickSort(a, 0, a.length, naturalOrder);
        assertArrayEquals(expected, a);
    }

    @Test
    public void testQuickSortMediumArray() {
        // 7 <= len <= 50 case
        int[] a = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
        int[] expected = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        IntArrays.quickSort(a, 0, a.length, naturalOrder);
        assertArrayEquals(expected, a);
    }

    @Test
    public void testQuickSortLargeArray() {
        // len > 50 case
        int size = 100;
        int[] a = new int[size];
        int[] expected = new int[size];
        Random rand = new Random(42);
        for (int i = 0; i < size; i++) {
            a[i] = rand.nextInt(1000);
            expected[i] = a[i];
        }
        Arrays.sort(expected);
        IntArrays.quickSort(a, 0, a.length, naturalOrder);
        assertArrayEquals(expected, a);
    }

    @Test
    public void testQuickSortRange() {
        int[] a = {9, 8, 7, 6, 5, 4, 3, 2, 1};
        // Sort only {7, 6, 5, 4} at indices 2 to 6 (exclusive)
        int[] expected = {9, 8, 4, 5, 6, 7, 3, 2, 1};
        IntArrays.quickSort(a, 2, 6, naturalOrder);
        assertArrayEquals(expected, a);
    }

    @Test
    public void testQuickSortEmptyAndSingle() {
        int[] empty = {};
        IntArrays.quickSort(empty, 0, 0, naturalOrder);
        assertArrayEquals(new int[]{}, empty);

        int[] single = {1};
        IntArrays.quickSort(single, 0, 1, naturalOrder);
        assertArrayEquals(new int[]{1}, single);
    }

    @Test
    public void testQuickSortAlreadySorted() {
        int[] a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int[] expected = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        IntArrays.quickSort(a, 0, a.length, naturalOrder);
        assertArrayEquals(expected, a);
    }

    @Test
    public void testQuickSortDuplicateElements() {
        int[] a = {3, 1, 2, 3, 1, 2, 3};
        int[] expected = {1, 1, 2, 2, 3, 3, 3};
        IntArrays.quickSort(a, 0, a.length, naturalOrder);
        assertArrayEquals(expected, a);
    }

    @Test
    public void testQuickSortAllSameElements() {
        int[] a = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5};
        int[] expected = {5, 5, 5, 5, 5, 5, 5, 5, 5, 5};
        IntArrays.quickSort(a, 0, a.length, naturalOrder);
        assertArrayEquals(expected, a);
    }
}
