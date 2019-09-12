import java.util.PriorityQueue;

/**
 * Created by 长春 on 2019/5/31.
 */
class KthLargest {
    PriorityQueue<Integer> pq = null;
    final int k;

    public KthLargest(int k, int[] nums) {
        this.k = k;
        pq = new PriorityQueue<Integer>();
        int i = 0;
        // while ( i < nums.length) {
        // pq.add(nums[i++]);
        // }
        for (int x:nums ) pq.add(x);




    }

    public int add(int val) {
        if (pq.size() < k) {pq.offer(val);}
        else {
            if (val > pq.peek()) {
                pq.poll();
                pq.offer(val);
            }
        }
        return pq.peek();

    }
}

