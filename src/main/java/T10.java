/**
 * Created by 长春 on 2019/5/30.
 */
//递归法每 k 个节点一组翻转链表
public class T10 {
    public ListNode reverseKGroup(ListNode head, int k) {
        int length=0;
        ListNode curr = head;
        while(curr!= null && length < k) {
            curr = curr.next;
            length++;
        }
        if (length == k) {
            ListNode pre = reverseKGroup(curr,k);
            while (k-- > 0) {
                ListNode nextTemp = head.next;
                head.next = pre;
                pre = head;
                head = nextTemp;
            }
            return pre;
        }
        return head;

    }
}
