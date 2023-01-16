import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.filter.operator.In;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ExpSizeTsfile {


    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session("localhost", 6667, "root", "root");
        session.open();
        SessionDataSet result = null;
        long startTime, endTime;



        /**
        TIANYUAN
            bucket_width=2
            k = 50, r = 10, w = 20 min, s = 10 min
        WANHUA
            bucket_width=2
            k = 50, r = 10, w = 20 min, s = 10 min
        JINFENG
            bucket_width=5
            k = 50, r = 20, w = 20 min, s = 10 min
        PAMAP2
            buck_width=0.1
            k = 50, r = 0.4, w = 20 min, s = 10 min
         TAO
            bucket_width=5
            r=15
         GAS
            bucket_width=0.1
            r=3
         */

        // TODO: modify here
        String[] datasetList = {"tianyuan", "wanhua", "jinfeng", "PAMAP2", "tao", "gas"};
        String curDataset = datasetList[0];
        double r = 10;
        int k=50, w=20, s=10;

        int[] sizeList = new int[10];
        String[] outputSizeList;
        int allNum = 0;
        if (curDataset == "tianyuan" ||curDataset == "wanhua" || curDataset == "jinfeng"  ) {
            allNum = 100000000;
            outputSizeList = new String[]{"0.1m", "0.2m", "0.5m", "1m", "2m",
                    "5m", "10m", "20m", "50m", "100m"};
            int[] numList = new int[]{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000};
            for (int i=0; i<10; i++){
                sizeList[i] = 99960 * numList[i];
            }
        }else if (curDataset =="tao" ){
            allNum = 575460;
            outputSizeList = new String[]{"60k", "120k", "180k","240k", "300k", "360k",
                    "420k", "480k", "540k", "600k"};
            for (int i=0; i<10; i++){
                sizeList[i] = 57540 * (i + 1);
            }
        }else if (curDataset == "gas"){
            allNum = 928990;
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


        LogWriter lw = new LogWriter("./result/ExpSize/" + curDataset + "_res.dat");
        lw.open();
        lw.log("Size\tTsfile" + "\n");

        Random random = new Random();
        for (int j= 0; j<sizeList.length; j++) {
            int size = sizeList[j];
            System.out.println(size);
            for (int i = 0; i < 30; i++) {
                long minTime = random.nextInt(allNum - size) * 1000L;
                long maxTime = minTime + size * 1000L;
                String sql = "select dodds(s9, " +
                        "'r'='" + r + "', " +
                        "'k'='" + k + "', " +
                        "'w'='" + w + "', " +
                        "'s'='" + s + "') " +
                        "from root." + curDataset + ".d0 " +
                        "where time>=" + minTime + " and time<" + maxTime;
                System.out.println(sql);
                result = session.executeQueryStatement(sql);
            }
            startTime = System.currentTimeMillis();
            for (int i = 0; i < 20; i++) {
                long minTime = random.nextInt(allNum - size) * 1000L;
                long maxTime = minTime + size * 1000L;
                String sql = "select dodds(s9, " +
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
            StringBuilder str = new StringBuilder();
            str.append(outputSizeList[j] + "\t");
            str.append(String.format("%.4f", (endTime - startTime) / 1000.0 / 20) + "\n");
            lw.log(str);
        }
    }
}
