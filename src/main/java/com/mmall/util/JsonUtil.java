package com.mmall.util;

import com.google.common.collect.Lists;
import com.mmall.pojo.User;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.type.JavaType;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;


/**
 * Created by taoshiliu on 2018/2/4.
 */
@Slf4j
public class JsonUtil {

    private static ObjectMapper objectMapper = new ObjectMapper();

    static{
        objectMapper.setSerializationInclusion(JsonSerialize.Inclusion.ALWAYS);
        objectMapper.configure(SerializationConfig.Feature.WRITE_DATE_KEYS_AS_TIMESTAMPS,false);
        objectMapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS,false);
        objectMapper.setDateFormat(new SimpleDateFormat(DateTimeUtil.STANDARD_FORMAT));

        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,false);
    }

    public static <T> String obj2String(T obj) {
        if(obj == null){
            return null;
        }
        try {
            return obj instanceof String ? (String)obj : objectMapper.writeValueAsString(obj);
        } catch (IOException e) {
            log.warn("Parse object to String error",e);
            return null;
        }
    }

    public static <T> String obj2StringPretty(T obj) {
        if(obj == null){
            return null;
        }
        try {
            return obj instanceof String ? (String)obj : objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (IOException e) {
            log.warn("Parse object to String error",e);
            return null;
        }
    }

    public static <T> T string2Obj(String str,Class<T> clazz) {
        if(StringUtils.isEmpty(str) || clazz == null) {
            return null;
        }

        try {
            return clazz.equals(String.class) ? (T)str : objectMapper.readValue(str,clazz);
        } catch (IOException e) {
            log.warn("Parse String to Object error",e);
            return null;
        }
    }

    public static <T> T string2Obj(String str, TypeReference<T> typeReference) {

        if(StringUtils.isEmpty(str) || typeReference == null) {
            return null;
        }

        try {
            return (T)(typeReference.getType().equals(String.class) ? str : objectMapper.readValue(str,typeReference));
        } catch (IOException e) {
            log.warn("Parse String to Object error",e);
            return null;
        }

    }

    public static <T> T string2Obj(String str, Class<?> collectionClass,Class<?>... elementClasses) {
        JavaType javaType = objectMapper.getTypeFactory().constructParametricType(collectionClass,elementClasses);
        try {
            return objectMapper.readValue(str,javaType);
        } catch (IOException e) {
            log.warn("Parse String to Object error",e);
            return null;
        }

    }


    public static void main(String[] args) {
        User u1 = new User();
        u1.setId(1);
        u1.setEmail("liutao1@happy");

        User u2 = new User();
        u2.setId(2);
        u2.setEmail("liutao2@happy");

        String user1Json = JsonUtil.obj2String(u1);
        String user1JsonPretty = JsonUtil.obj2StringPretty(u1);

        log.info("user1Json:{}",user1Json);
        log.info("user1JsonPretty:{}",user1JsonPretty);

        User user = JsonUtil.string2Obj(user1Json,User.class);

        List<User> userList = Lists.newArrayList();
        userList.add(u1);
        userList.add(u2);

        String userListStr = JsonUtil.obj2StringPretty(userList);
        log.info("=================");
        log.info(userListStr);

        List<User> userListObj1 = JsonUtil.string2Obj(userListStr, new TypeReference<List<User>>() {
        });

        List<User> userListObj2= JsonUtil.string2Obj(userListStr,List.class,User.class);

        System.out.println("end");
    }
}
