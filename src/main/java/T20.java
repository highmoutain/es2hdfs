import java.util.LinkedList;


class T20 {
    public static int[] maxSlidingWindow(int[] nums, int k) {
        if(nums==null||nums.length<2) return nums;
        LinkedList<Integer> list=new LinkedList<Integer>();
        int result[] = new int[nums.length-k+1];

        for (int i=0;i<nums.length;i++) {
            while(!list.isEmpty()&&list.peekLast() <= nums[i]) {
                list.pollLast();}
            list.addLast(i);
            if(list.peekFirst() <= i-k) list.pollFirst();
            if (i-k+1 >= 0)
                result[i-k+1] = nums[list.peekFirst()];
        }
        return result;

    }

    public static void main(String[] args) {
        int nums[] = {7,2,4};
        int k =2;
        maxSlidingWindow(nums,k);
    }

}
