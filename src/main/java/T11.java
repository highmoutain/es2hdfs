/**
 * Created by 长春 on 2019/5/30.
 */
//递归法两两反转
public class T11 {
    public ListNode swapPairs(ListNode head) {
        if(head==null)
            return null;
        if(head.next==null)
            return head; //deal with single node
        ListNode temp=head.next;
        head.next=head.next.next;
        temp.next=head;
        temp.next.next=swapPairs(temp.next.next); //Iterate the next pair
        return temp;
    }
}
