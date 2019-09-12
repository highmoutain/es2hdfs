/**
 * Created by 长春 on 2019/5/28.
 */

    public class T9 {
        public boolean hasCycle(ListNode head) {
            if (head == null) return false;
            ListNode fast = head;
            ListNode slow = head;
            while(slow.next != null && fast.next != null && fast.next.next != null ) {
                slow = slow.next;
                fast = fast.next.next;
                if (slow == fast) return true;



            }
            return false;
        }
    }

