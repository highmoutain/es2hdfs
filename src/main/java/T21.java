import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by 长春 on 2019/6/14.
 */
class T21 {
    public List<List<Integer>> threeSum(int[] nums) {
        HashMap<Integer,Integer> hm = new HashMap<Integer,Integer>();
        int temp ;
        List<List<Integer>> aal = new ArrayList<>();

        for(int i = 0;i<nums.length;i++)
            hm.put(nums[i],i);

        for(int i=0; i< nums.length-1;i++) {
            for (int j=i;j<nums.length;j++)
            {
                hm.remove(nums[i],i);
                hm.remove(nums[j],j);
                temp = 0 - (nums[i]+nums[j]);
                if (hm.containsKey(temp)) {
                    ArrayList<Integer> al = new ArrayList<Integer>();
                    al.add(nums[i]);
                    al.add(nums[j]);
                    al.add(hm.get(temp));
                    aal.add(al);
                    hm.put(nums[i],i);
                    hm.put(nums[j],j);
                }

            }
        }
        return aal;

    }
}
