/**
 * Created by ���� on 2019/5/24.
 */
//递归法链表反转
class ListNode {
    int val;
    ListNode next;
    ListNode(int x) { val = x; }
}
public class T7 {

    public static ListNode reverseList(ListNode head) {
        if (head == null || head.next == null) return head;
        ListNode p = reverseList(head.next);
        head.next.next = head;
        head.next = null;
        return p;
    }

    public static void main(String[] args) {
        ListNode a = new ListNode(1);
        ListNode b = new ListNode(2);
        ListNode c = new ListNode(3);
        a.next = b;
        b.next = c;
        ListNode t = reverseList(a);

    }

}
