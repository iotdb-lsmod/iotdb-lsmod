import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpSizeUDF {

    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;
        /**
         TIANYUAN
            k = 50, r = 10, w = 20 min, s = 10 min
         WANHUA
            k = 50, r = 10, w = 20 min, s = 10 min
         JINFENG
            k = 50, r = 20, w = 20 min, s = 10 min
         PAMAP2
            k = 50, r = 0.4, w = 20 min, s = 10 min
         */


        String[] datasetList = {"tianyuan", "wanhua", "jinfeng", "PAMAP2", "tao", "gas"};
        String curDataset = datasetList[0];
        double r = 10;
        int k=50, w=20*60, s=10*60;

        int[] sizeList = new int[10];
        String[] outputSizeList;
        if (curDataset == "tianyuan" ||curDataset == "wanhua" || curDataset == "jinfeng"  ) {
            outputSizeList = new String[]{"0.1m", "0.2m", "0.5m", "1m", "2m",
                    "5m", "10m", "20m", "50m", "100m"};
            int[] numList = new int[]{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000};
            for (int i=0; i<10; i++){
                sizeList[i] = 99960 * numList[i];
            }
        }else if (curDataset =="tao" ){
            outputSizeList = new String[]{"60k", "120k", "180k","240k", "300k", "360k",
                    "420k", "480k", "540k", "600k"};
            for (int i=0; i<10; i++){
                sizeList[i] = 57540 * (i + 1);
            }
        }else if (curDataset == "gas"){
            outputSizeList = new String[]{"90k", "180k", "270k","360k", "450k", "540k",
                    "630k", "720k", "810k", "900k"};
            for (int i=0; i<10; i++){
                sizeList[i] = 90000 * (i + 1);
            }
        }
        else{
            outputSizeList = new String[]{"30k", "60k", "90k", "120k", "150k",
                    "180k", "210k", "240k", "270k", "300k"};
            for (int i=0; i<10; i++){
                sizeList[i] = 29940 * (i+1);
            }
        }
        Map<String, Integer> map = new HashMap<>();
        map.put("jinfeng", 1000000);
        map.put("tianyuan", 1000002);
        map.put("PAMAP2", 299999);
        map.put("wanhua", 1000000);
        map.put("tao", 575468);
        map.put("gas", 928990);



        LogWriter lw = new LogWriter("./result/ExpSize/" + curDataset + "_res.dat");
        lw.open();

        lw.log("Size\tCPOD" + "\n");
        Random random = new Random();

        int iterNum = 10;
        for (int j= 0; j<sizeList.length; j++) {
            StringBuilder str = new StringBuilder();
            int size = sizeList[j];

            startTime = System.currentTimeMillis();
            for (int i = 0; i < iterNum; i++) {
                long minTime = random.nextInt(map.get(curDataset)- size) * 1000L;
                long maxTime = minTime + size * 1000L;
                String sql = "select cpod(s9, " +
                        "'r'='" + r + "', " +
                        "'k'='" + k + "', " +
                        "'w'='" + w + "', " +
                        "'s'='" + s + "') " +
                        "from root." + curDataset + ".d0 " +
                        "where time>=" + minTime + " and time<" + maxTime;
                System.out.println(sql);
                result = session.executeQueryStatement(sql);
            }
            endTime = System.currentTimeMillis();
            str.append(outputSizeList[j] + "\t");
            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");

//            startTime = System.currentTimeMillis();
//            for (int i = 0; i < iterNum; i++) {
//                long minTime = random.nextInt(575468 - size) * 1000L;
//                long maxTime = minTime + size * 1000L;
//                String sql = "select mcod(s10, " +
//                        "'radius'='" + r + "', " +
//                        "'threshold'='" + k + "', " +
//                        "'window'='" + w + "', " +
//                        "'slide'='" + s + "') " +
//                        "from root." + curDataset + ".d0 " +
//                        "where time>=" + minTime + " and time<" + maxTime;
//                System.out.println(sql);
//                result = session.executeQueryStatement(sql);
//            }
//            endTime = System.currentTimeMillis();
//            str.append(outputSizeList[j] + "\t");
//            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / iterNum) + "\n");
//            lw.log(str);
        }
    }
}
