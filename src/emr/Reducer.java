package emr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

public class Reducer {

    private boolean DEBUG_MODE = false;
    private final String MONTH = "201508";
    private final Integer PAGE_VIEW_LIMIT;
    private Map<String, Integer> daily_counter;

    public Reducer() {
        this.PAGE_VIEW_LIMIT = 100000;
    }

    public void reset_daily_counter() {
        if (daily_counter == null) {
            daily_counter = new TreeMap<>();
        } else {
            daily_counter.clear();
        }

        for (int i = 1; i <= 31; i++) {
            if (i < 10) {
                daily_counter.put(MONTH + "0" + i, 0);
            } else {
                daily_counter.put(MONTH + i, 0);
            }
        }
    }

    /**
     * This is the main reductron DEBUG_MODE is just for testing purposes
     *
     * @param filename
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void read(String filename) throws FileNotFoundException, IOException {
        reset_daily_counter();
        BufferedWriter bw=new BufferedWriter(new FileWriter(new File("mapperOutput")));
        try {
            BufferedReader br
                    = new BufferedReader(new InputStreamReader(System.in));
            //Initialize Variables
            String input;
            String word = null;
            String currentWord = null;
            int currentCount = 0;

            //While we have input on stdin
            while ((input = br.readLine()) != null) {
                bw.write(input+"\n");
                try {
                    String[] parts = input.split("\t");
                    word = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    String _date = parts[2];
                    //We have sorted input, so check if we
                    //are we on the same word?
                    if (currentWord != null && currentWord.equals(word)) {
                        //currentCount+=Integer.parseInt(parts[1]);
                        //Integer _count = Integer.parseInt(parts[1]);
                        daily_counter.put(_date, daily_counter.get(_date) + count);
                    } else //The word has changed
                    {
                        if (currentWord != null) //Is this the first word, if not output count
                        {
                            int sum = sum();
                            if (sum > PAGE_VIEW_LIMIT) {
                                System.out.print(sum + "\t" + currentWord + "\t");
                                System.out.println(displayList());
                            }
                            reset_daily_counter();
                        }
                        currentWord = word;
                        currentCount = count;
                    }
                } catch (NumberFormatException e) {
                    continue;
                }
            }

            //Print out last word if missed
            if (currentWord != null && currentWord.equals(word)) {
                int sum = sum();
                if (sum > PAGE_VIEW_LIMIT) {
                    System.out.print(sum + "\t" + currentWord + "\t");
                    System.out.println(displayList());
                }
                reset_daily_counter();
            }

        } catch (IOException io) {
            io.printStackTrace();
        }

    }

    public Integer sum() {
        int sum = 0;
        for (String i : daily_counter.keySet()) {
            sum += daily_counter.get(i);
        }
        return sum;
    }

    public String displayList() {
        String _temp = "";
        for (String i : daily_counter.keySet()) {
            _temp += i + ":" + daily_counter.get(i) + "\t";
        }
        return _temp;
    }

    public static void main(String[] args) {
        try {
            new Reducer().read("mapper_output");
        } catch (IOException ex) {

        }
    }
}