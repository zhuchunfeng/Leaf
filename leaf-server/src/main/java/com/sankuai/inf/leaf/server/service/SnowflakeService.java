package com.sankuai.inf.leaf.server.service;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.ZeroIDGen;
import com.sankuai.inf.leaf.server.Constants;
import com.sankuai.inf.leaf.server.exception.InitException;
import com.sankuai.inf.leaf.snowflake.SnowflakeIDGenImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service("SnowflakeService")
public class SnowflakeService {

//    @Value("#{server.port}")
//    private int port;
//    @Value("#{leaf.redis.ip}")
//    private String redisIp;
//    @Value("${leaf.redis.port}")
//    private int redisPort;
//    @Value("${leaf.redis.password:123131}")
//    private String redisPassword;


    private Logger logger = LoggerFactory.getLogger(SnowflakeService.class);

    private IDGen idGen;

    public SnowflakeService() throws InitException {

        Properties properties = PropertyFactory.getProperties();
        boolean flag = Boolean.parseBoolean(properties.getProperty(Constants.LEAF_SNOWFLAKE_ENABLE, "true"));
        if (flag) {

            int port = Integer.parseInt(properties.getProperty(Constants.LEAF_SNOWFLAKE_PORT));
            String redisIp = properties.getProperty(Constants.LEAF_REDIS_IP);
            int redisPort = Integer.parseInt(properties.getProperty(Constants.LEAF_REDIS_PORT));
            String redisPassword = properties.getProperty(Constants.LEAF_REDIS_PASSWORD);

            logger.info(port + "  |  " + redisIp + "  |  " + redisPort + "  |  " + redisPassword + "  |  " + port);
            idGen = new SnowflakeIDGenImpl(redisIp, redisPort, redisPassword, port);

            if (idGen.init()) {
                logger.info("Snowflake Service Init Successfully");
            } else {
                throw new InitException("Snowflake Service Init Fail");
            }
        } else {
            idGen = new ZeroIDGen();
            logger.info("Zero ID Gen Service Init Successfully");
        }
    }

    public Result getId(String key) {
        return idGen.get(key);
    }
}
