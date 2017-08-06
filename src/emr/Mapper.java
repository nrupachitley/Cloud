package emr;

import java.io.BufferedReader;
        import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
        import java.io.OutputStreamWriter;
import java.util.logging.Level;
        import java.util.logging.Logger;
        import java.io.InputStreamReader;
public class Mapper {

    private final String DELIMITER="<CUSTOM_DELIMITER>";
    private final String LANGUAGE_INDENTIFIER = "en";
    private final String SUBPROJECT_SUFFIX = " ";
    private final String FIELD_DELIMITER = " ";
    private final boolean DEBUG_MODE=false;
    private final String[] wikiSpecialPages = {
            "Media:",
            "Special:",
            "Talk:",
            "User:",
            "User_talk:",
            "Project:",
            "Project_talk:",
            "File:",
            "File_talk:",
            "MediaWiki:",
            "MediaWiki_talk:",
            "Template:",
            "Template_talk:",
            "Help:",
            "Help_talk:",
            "Category:",
            "Category_talk:",
            "Portal:",
            "Wikipedia:",
            "Wikipedia_talk:"
    };
    private final String[] boilerPlates = {
            "404_error/",
            "Main_Page",
            "Hypertext_Transfer_Protocol",
            "Search"
    };
    private final String[] imageExtensions = {
            ".jpg",
            ".gif",
            ".png",
            ".JPG",
            ".GIF",
            ".PNG",
            ".txt",
            ".ico"
    };

    /**
     * reads the file and filters it according to the required format
     *
     * @param filename
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void read(String filename) throws FileNotFoundException, IOException {

        BufferedReader br=new BufferedReader(new InputStreamReader(System.in));
        String date=filename.split("-")[2];
        //System.out.println(date);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        String currentLine;
        int debug = 0;
        while ((currentLine = br.readLine()) != null) {
            if (currentLine.startsWith(LANGUAGE_INDENTIFIER + SUBPROJECT_SUFFIX)) {

                String[] delimitedString = currentLine.split(FIELD_DELIMITER);
                if (!containsAnyOf(delimitedString[1], "BOILER_PLATE") && !containsAnyOf(delimitedString[1], "IMAGE_EXTENSIONS") && !containsAnyOf(delimitedString[1], "SPECIAL_PAGES") && !startsWithLowerCase(delimitedString[1]) && !delimitedString[1].equals("")) {
                    //if(delimitedString[1].contains("CTG"))
                    //    System.err.println("");
                    //bw.write(delimitedString[1] + DELIMITER + date+"\t"+delimitedString[2]+"\n");
                    System.out.println(delimitedString[1] + DELIMITER + date + "\t" + delimitedString[2]);
                }

            }
        }
    }

    /**
     * print out the map in the format that can be easily sorted for different
     * dates
     *
     * public void print_hourly_count(String filename) throws IOException{
     *
     * for(String s:hourly_counter.keySet()){
     *
     * }
     * }
     */
    /**
     * removes all the entries from the data that contains ant of the string in
     * the instance arrays: Special Wiki pages, boiler plate template and image
     * extensions
     *
     * @param source
     * @param target
     * @return
     */
    public boolean containsAnyOf(String source, String target) {
        switch (target) {
            case "BOILER_PLATE": {
                for (String s : boilerPlates) {
                    if (source.equals(s)) {
                        return true;
                    }
                }
                break;
            }
            case "IMAGE_EXTENSIONS": {
                for (String s : imageExtensions) {
                    if (source.endsWith(s)) {
                        return true;
                    }
                }
                break;
            }
            case "SPECIAL_PAGES": {
                for (String s : wikiSpecialPages) {
                    if (source.startsWith(s)) {
                        return true;
                    }
                }
                break;
            }
        }
        return false;
    }

    /**
     * removes all the page titles that start with lower case english characters
     *
     * @param source
     * @return
     */
    public boolean startsWithLowerCase(String source) {
        try {
            if ((int) source.charAt(0) >= 97 && (int) source.charAt(0) <= 122) {
                return true;
            }
            return false;
        } catch (StringIndexOutOfBoundsException se) {
            return false;
        }

    }

    public static void main(String[] args) {
        try {
            Mapper lf = new Mapper();
            //System.out.println(System.getenv("mapreduce_map_input_file"));
            lf.read(System.getenv("mapreduce_map_input_file"));
            //lf.print_hourly_count(System.getenv("mapreduce_map_input_file"));

        } catch (IOException ex) {
            Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
