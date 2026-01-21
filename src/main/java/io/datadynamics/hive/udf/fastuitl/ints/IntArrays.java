package io.datadynamics.hive.udf.fastuitl.ints;

/**
 * int 배열 처리를 위한 유틸리티 클래스입니다.
 * 주로 성능 최적화된 정렬 알고리즘(Quicksort)을 제공합니다.
 */
public class IntArrays {

    /**
     * 지정된 범위에 대해 선택 정렬(Selection Sort)을 수행합니다.
     * 주로 배열의 크기가 매우 작을 때(7 미만) 사용됩니다.
     *
     * @param a    정렬할 배열
     * @param from 시작 인덱스 (inclusive)
     * @param to   끝 인덱스 (exclusive)
     * @param comp 비교를 위한 Comparator
     */
    private static void selectionSort(int[] a, int from, int to, IntComparator comp) {
        for (int i = from; i < to - 1; ++i) {
            int m = i;

            int u;
            for (u = i + 1; u < to; ++u) {
                if (comp.compare(a[u], a[m]) < 0) {
                    m = u;
                }
            }

            if (m != i) {
                u = a[i];
                a[i] = a[m];
                a[m] = u;
            }
        }
    }

    /**
     * 두 인덱스의 요소를 서로 교환합니다.
     *
     * @param x 대상 배열
     * @param a 첫 번째 인덱스
     * @param b 두 번째 인덱스
     */
    private static void swap(int[] x, int a, int b) {
        int t = x[a];
        x[a] = x[b];
        x[b] = t;
    }

    /**
     * 배열의 한 구간을 다른 구간과 통째로 교환합니다.
     * Bentley-McIlroy 3-way partitioning에서 피벗과 같은 값들을 양 끝에서 중간으로 옮길 때 사용됩니다.
     *
     * @param x 대상 배열
     * @param a 첫 번째 구간의 시작 위치
     * @param b 두 번째 구간의 시작 위치
     * @param n 교환할 요소의 개수
     */
    private static void vecSwap(int[] x, int a, int b, int n) {
        for (int i = 0; i < n; ++b) {
            swap(x, a, b);
            ++i;
            ++a;
        }
    }

    /**
     * 세 값 중 중간값(median)을 선택하여 인덱스를 반환합니다.
     *
     * @param x    대상 배열
     * @param a    첫 번째 인덱스
     * @param b    두 번째 인덱스
     * @param c    세 번째 인덱스
     * @param comp 비교를 위한 Comparator
     * @return 중간값에 해당하는 인덱스
     */
    private static int med3(int[] x, int a, int b, int c, IntComparator comp) {
        int ab = comp.compare(x[a], x[b]);
        int ac = comp.compare(x[a], x[c]);
        int bc = comp.compare(x[b], x[c]);
        return ab < 0 ? (bc < 0 ? b : (ac < 0 ? c : a)) : (bc > 0 ? b : (ac > 0 ? c : a));
    }

    /**
     * Bentley-McIlroy 3-way partitioning 알고리즘을 사용한 Quicksort입니다.
     * 이 알고리즘은 중복된 키가 많은 배열에서 효율적으로 작동합니다.
     *
     * @param x    정렬할 배열
     * @param from 시작 인덱스 (inclusive)
     * @param to   끝 인덱스 (exclusive)
     * @param comp 비교를 위한 Comparator
     */
    public static void quickSort(int[] x, int from, int to, IntComparator comp) {
        int len = to - from;

        // 1. 작은 배열은 선택 정렬로 처리
        if (len < 7) {
            selectionSort(x, from, to, comp);
        } else {
            // 2. 피벗 선택 (중간값 선택 전략)
            int m = from + len / 2;
            int v;
            int a;
            int b;
            if (len > 7) {
                v = from;
                a = to - 1;
                if (len > 50) {
                    // 아주 큰 배열의 경우 'pseudo-median of nine' 전략 사용
                    b = len / 8;
                    v = med3(x, from, from + b, from + 2 * b, comp);
                    m = med3(x, m - b, m, m + b, comp);
                    a = med3(x, a - 2 * b, a - b, a, comp);
                }
                m = med3(x, v, m, a, comp);
            }

            // 3. 3-way Partitioning (Bentley-McIlroy)
            // v: 피벗 값
            // [from, a): 피벗과 같은 값들 (왼쪽 끝)
            // [a, b): 피벗보다 작은 값들
            // (c, d]: 피벗보다 큰 값들
            // (d, to-1]: 피벗과 같은 값들 (오른쪽 끝)
            v = x[m];
            a = from;
            b = from;
            int c = to - 1;
            int d = c;

            while (true) {
                int s;
                // b를 오른쪽으로 이동시키며 피벗과 비교
                while (b <= c && (s = comp.compare(x[b], v)) <= 0) {
                    if (s == 0) swap(x, a++, b); // 피벗과 같으면 왼쪽 끝으로 이동
                    b++;
                }

                // c를 왼쪽으로 이동시키며 피벗과 비교
                while (c >= b && (s = comp.compare(x[c], v)) >= 0) {
                    if (s == 0) swap(x, c, d--); // 피벗과 같으면 오른쪽 끝으로 이동
                    c--;
                }

                if (b > c) break;
                swap(x, b++, c--); // 피벗보다 작은 값과 큰 값을 교환
            }

            // 4. 양 끝에 모아둔 피벗과 같은 값들을 중간으로 이동
            int n = to;
            int s;
            s = Math.min(a - from, b - a);
            vecSwap(x, from, b - s, s);
            s = Math.min(d - c, n - d - 1);
            vecSwap(x, b, n - s, s);

            // 5. 피벗을 제외한 좌우 구간에 대해 재귀 호출
            if ((s = b - a) > 1) {
                quickSort(x, from, from + s, comp);
            }

            if ((s = d - c) > 1) {
                quickSort(x, n - s, n, comp);
            }
        }
    }
}
