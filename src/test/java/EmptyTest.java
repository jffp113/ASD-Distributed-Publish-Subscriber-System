import protocols.dht.ChordWithSalt;

public class EmptyTest {

    private static boolean isIdBetween(int nodeId, int n, int succesor, int k) {

        if (nodeId > n && nodeId <= succesor)
            return true;
        if (nodeId < n && nodeId <= succesor)
            return true;
        if (nodeId > n && nodeId >= succesor)
            return true;

        return false;
    }

    public static boolean isbeetween(int n ,int nodeiD,int sucessor, int k){
        if (n > k) return false;
        while(true){
            nodeiD = (nodeiD + 1) % k;
            if(nodeiD == n)
                return true;
            if(nodeiD == sucessor)
                return false;
        }


    }

    public static void main(String[] args) {
        /*System.out.println(isIdBetween(2,1,4)) ;
        System.out.println(isIdBetween(1,7,2));
        System.out.println(isIdBetween(8,7,2));
        System.out.println(isIdBetween(6,7,2));
        System.out.println(isIdBetween(8,1,4));*/

        System.out.println(isbeetween(2,1,4,9)) ;
        System.out.println(isbeetween(1,7,2,9));
        System.out.println(isbeetween(8,7,2,9));
        System.out.println(isbeetween(6,7,2,9));
        System.out.println(isbeetween(8,1,4,9));


        System.out.println(Math.abs("localhost:8004".hashCode()) % 1000);
    }
}
