package com.sankuai.inf.leaf.snowflake;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanResult;

import java.util.Properties;
import java.util.Set;

public class SnowflakeIDGenImplTest {



    @Test
    public void testGetId() {
        Properties properties = PropertyFactory.getProperties();

        IDGen idGen = new SnowflakeIDGenImpl(
                properties.getProperty("leaf.redis.ip"),
                Integer.valueOf(properties.getProperty("leaf.redis.port")),
                properties.getProperty("leaf.redis.password"),
                8080);
        for (int i = 1; i < 1000; ++i) {
            Result r = idGen.get("a");
            System.out.println(r);
        }
    }
}
