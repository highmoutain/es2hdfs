import java.util.ArrayList;
import java.util.List;

/**
 * Created by ³¤´º on 2019/8/6.
 */
public class T22 {
    public static List<String> result = new ArrayList<String>();
    public static List<String> generateParenthesis(int n) {
        String str = "";
        int left = n;
        int right = n;
        generate( str, left, right );
        return result;

    }
    public static void generate(String str, int left, int right) {
        if (left == 0 && right == 0) {
            result.add(str);
            return;
        }
        if (left > 0) generate(str+"(",left-1,right);
        if (right > 0 ) generate(str+")",left,right-1);
    }
    public static void main(String[] args) {
        generateParenthesis(3);
    }

}
