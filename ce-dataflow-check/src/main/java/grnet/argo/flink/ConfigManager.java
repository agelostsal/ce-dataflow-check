package grnet.argo.flink;

import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1> ConfigManager class </h1>
 * <p> The ConfigManager class is responsible for providing methods that help the
 * process of creating and extracting information from various configuration objects. </p>
 */

public class ConfigManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

    /**
     * The core method of the class that takes as input a class and a location for a configuration file
     * and returns an instance of the input class, with the fields represented in the configuration file, filled with the respective values.
     * @param cls Class
     * @param filepath A string representing the location of the configuration file
     * @param <T> The type of configuration object it will return. e.g. ComputeEngineConfig
     * @return An instance of the configuration class passed in
     * @throws Exception
     */
    public static <T> T configurationSetup(Class<T> cls, String filepath) throws Exception {

        BufferedReader br = null;
        InputStream is = null;
        String err_msg ;
        String line;
        StringBuilder sb = new StringBuilder();

        try {
            // find the configuration file in the classpath, else search in the file system
            is = ConfigManager.class.getResourceAsStream("/".concat(filepath));

            if (is != null) {
                br = new BufferedReader(new InputStreamReader(is));
            } else {
                br = new BufferedReader(new FileReader(filepath));
            }

            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

            Object obj = loadJsonToObject(sb.toString(), cls);
            checkForNulls(obj);
            return cls.cast(obj);

        } catch (FileNotFoundException e) {
            err_msg = e.getMessage() + ". File was not found neither in the classpath nor in the filesystem.";
            LOGGER.error(err_msg);
            throw new FileNotFoundException(err_msg);
        } finally {
            IOUtils.closeQuietly(br);
            IOUtils.closeQuietly(is);
        }
    }

    public static <T> T loadJsonToObject(String json_config, Class<T> cls) {
        return new GsonBuilder().serializeNulls().create().fromJson(json_config, cls);
    }

    // Method that checks if all the fields of an object have been properly filled with values
    public static <T> void checkForNulls(T ce_cfg) throws Exception {
        Field[] cfg_fields = ce_cfg.getClass().getDeclaredFields();
        for (Field fl : cfg_fields) {
            fl.setAccessible(true);
            if(fl.get(ce_cfg) == null){
                throw new NullPointerException("Null value for field : " + fl.getName() + ". Check the configuration file and make sure you have declared all fields.");
            }
        }
    }

    /**
     * A method that takes in a url e.g. hdfs://{{host_nn}}:{{hdfs_port}}/{{hdfs_path_to_file}},
     * finds where the String interpolation happens, with the help of regular expressions, and then replaces
     * the placeholders with the respective values found in the configuration object, using the getter methods.
     * E.g. for {{hdfs_nn}}, it will try to find a getter method getHdfs_nn().
     * @param url A string representing a url, containing patterns that should be replaced with actual values
     * @param regex The regex matching the patterns of the url
     * @param config Configuration object , containing the values that will be used for filling up the url string
     * @return The original url, filled up with actual values
     * @throws Exception
     */
    public static String composeURL(String url, String regex, Object config) throws Exception {
        Pattern p = Pattern.compile(regex);
        Matcher matcher = p.matcher(url);
        while (matcher.find()) {
            String matched_reg = matcher.group();
            String me = "get".concat(StringUtils.capitalize(matcher.group().replaceAll("\\{", "").replaceAll("\\}", "")));
            Method m = config.getClass().getDeclaredMethod(me);
            url = StringUtils.replace(url, matched_reg, m.invoke(config).toString());
        }
        return url;
    }
}
