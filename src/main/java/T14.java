import java.util.Stack;

/**
 * Created by 长春 on 2019/5/30.
 */
class MyQueue {
    Stack<Integer> input = null;
    Stack<Integer> output = null;
    /** Initialize your data structure here. */
    public MyQueue() {
        input = new Stack<Integer>();
        output = new Stack<Integer>();

    }

    /** Push element x to the back of queue. */
    public void push(int x) {
        input.push(x);

    }

    /** Removes the element from in front of queue and returns that element. */
    public int pop() {
        if (output.isEmpty()) {
            while(!input.isEmpty()) {
                output.push(input.pop());
            }
        }
        return output.pop();

    }

    /** Get the front element. */
    public int peek() {
        if (output.isEmpty()) {
            while(!input.isEmpty()) {
                output.push(input.pop());
            }
        }
        return output.peek();
    }

    /** Returns whether the queue is empty. */
    public boolean empty() {
        return output.empty()&&input.empty();

    }
}

/**
 * Your MyQueue object will be instantiated and called as such:
 * MyQueue obj = new MyQueue();
 * obj.push(x);
 * int param_2 = obj.pop();
 * int param_3 = obj.peek();
 * boolean param_4 = obj.empty();
 */