package io.datadynamics.hive.udf.utils;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ConfigUtils {

    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    public static List<String> loadFile(String fileName) throws IOException {
        ArrayList<String> strings = Lists.newArrayList();
        Closer closer = Closer.create();
        try {
            InputStream inputStream = ConfigUtils.class.getResourceAsStream(fileName);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            closer.register(bufferedReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (Strings.isNullOrEmpty(line) || line.startsWith("#")) {
                    continue;
                }
                strings.add(line);
            }
        } catch (IOException e) {
            logger.error("loadFile {} error. error is {}.", fileName, e);
            throw e;
        } finally {
            closer.close();
        }

        return strings;
    }
}
