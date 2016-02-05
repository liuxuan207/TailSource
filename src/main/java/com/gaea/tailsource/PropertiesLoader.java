package com.gaea.tailsource;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesLoader {

    private static final Logger log = LoggerFactory.getLogger(PropertiesLoader.class);

    /**
     * @param loaderClass Class instance that is used to load properties
     * @param classpath Classpath location of properties file
     * @param charset Charset of properties file
     * @return Properties
     */
    public static Properties loadClasspathProperties(Class<?> loaderClass, String classpath, String charset) {
        Reader reader = null;
        try {
            Properties props = new Properties();
            InputStream in = loaderClass.getResourceAsStream(classpath);
            reader = new InputStreamReader(in, charset);
            props.load(reader);

            return props;
        } catch (Exception e) {
            log.error("Error occurs when loading properties, loader class: {}, classpath: {}", e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    /**
     * @param loaderClass Class instance that is used to load properties
     * @param charset Charset of properties file
     * @return Properties
     */
    public static Properties loadClasspathProperties(Class<?> loaderClass, String charset) {
        String classpath = loaderClass.getSimpleName() + ".properties";
        return loadClasspathProperties(loaderClass, classpath, charset);
    }

    /**
     * @param file File location
     * @param charset Charset of properties file
     * @return Properties
     */
    public static Properties loadProperties(String file, String charset) {
        Reader reader = null;
        try {
            InputStream in = new FileInputStream(file);
            Properties props = new Properties();
            reader = new InputStreamReader(in, charset);
            props.load(reader);
            return props;
        } catch (Exception e) {
            log.error("Error occurs when loading properties, file: {}", file, e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }
}
