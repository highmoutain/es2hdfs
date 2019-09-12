/**
 * Created by 长春 on 2019/5/28.
 */


class T8 {
    public static ListNode swapPairs(ListNode head) {
        if (head == null || head.next == null) return head;
        ListNode pre = head;
        ListNode rethead = head.next;
        ListNode curr = head.next;

        ListNode lastpre = null;
        while (curr != null) {
            ListNode nextTemp = curr.next;
            pre.next = nextTemp;
            curr.next = pre;
            if (lastpre != null) lastpre.next = curr;
            //指针移动
            lastpre = pre;
            pre = nextTemp;
            if (pre != null) {curr = pre.next;}
            else {curr = null;}

        }
        return rethead;


    }
    public static void main(String[] args) {
        ListNode a = new ListNode(1);
        ListNode b = new ListNode(2);
        ListNode c = new ListNode(3);
        ListNode d = new ListNode(4);
        a.next = b;
        b.next = c;
        c.next = d;
        swapPairs(a);

    }
}
