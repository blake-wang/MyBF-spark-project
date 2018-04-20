public class TestArray {
    public static void main(String[] args) {
        String[] arr = new String[]{"wanglei", "aixu", "junhai"};
        for (int i = 0; i < arr.length; i++) {
            if(i==1){
                arr[i] = "haha " + i;
            }


        }

        for (int i = 0; i < arr.length; i++) {
            String s = arr[i];
            System.out.println(s);
        }
    }
}
