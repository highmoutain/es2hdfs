import java.util.HashMap;
import java.util.Stack;

/**
 * Created by 长春 on 2019/5/30.
 */
//20.有效的括号
public class T13 {
    HashMap<Character,Character> m ;
    public T13() {
        m = new HashMap<Character,Character>();
        m.put(')','(');
        m.put(']','[');
        m.put('}','{');
    }

    public boolean isValid(String s) {
        Stack<Integer> s1;
        Stack<Character> stack = new Stack<Character>();
        int i = 0;
        while(i <s.length()) {
            char c = s.charAt(i);
            if (m.containsKey(c) && !stack.isEmpty()) {
                Character ele = stack.pop();
                if (ele != m.get(c)) return false;

            } else stack.push(c);
            i++;

        }
        return stack.isEmpty();
    }
}
