package ru.vk.competition.minchecker.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.util.function.BiFunction;
import java.util.function.Function;

@Slf4j(topic = "Call")
public class OkHttp {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static int get(String url, int resultId, boolean isLog) {
    return wrap(toHttp(url, resultId), Request.Builder::get, isLog);
  }

  public static int delete(String url, int resultId, boolean isLog) {
    return wrap(toHttp(url, resultId), Request.Builder::delete, isLog);
  }

  public static <T> int post(String url, T requestBody, boolean isLog) {
    return post(toHttp(url), requestBody, isLog);
  }

  public static <T> int post(String url, int resultId, T requestBody, boolean isLog) {
    return post(toHttp(url, resultId), requestBody, isLog);
  }

  public static <T> int put(String url, int resultId, T requestBody, boolean isLog) {
    return put(toHttp(url, resultId), requestBody, isLog);
  }

  private static <T> int post(HttpUrl url, T requestBody, boolean isLog) {
    return wrap(url, requestBody, Request.Builder::post, isLog);
  }

  private static <T> int put(HttpUrl url, T requestBody, boolean isLog) {
    return wrap(url, requestBody, Request.Builder::put, isLog);
  }

  private static <T> int wrap(HttpUrl url, T requestBody, BiFunction<Request.Builder, RequestBody, Request.Builder> requestMapper, boolean isLog) {
    try {
      String json;
      try {
        json = objectMapper.writeValueAsString(requestBody);
      }
      catch (Exception e) {
        log.error(e.getMessage());
        return -1;
      }
      var body = RequestBody.create(json, APPLICATION_JSON);
      return call(requestMapper.apply(new Request.Builder().url(url), body).build(), json, isLog);
    } catch (Exception e) {
      log.error(e.getMessage());
      return -1;
    }
  }

  private static int wrap(HttpUrl url, Function<Request.Builder, Request.Builder> requestMapper, boolean isLog) {
    try {
      return call(requestMapper.apply(new Request.Builder().url(url)).build(), "<empty>", isLog);
    } catch (Exception e) {
      log.error(e.getMessage());
      return -1;
    }
  }

  private static int call(Request request, String requestBody, boolean isLog) {
    Call call = new OkHttpClient().newCall(request);
    try (Response response = call.execute()) {
      int code = response.code();
      ResponseBody rBody = response.body();
      String responseBody = "";
      if (rBody != null) {
        responseBody = rBody.string();
      }
      if (responseBody.isEmpty()) {
        responseBody = "<empty>";
      }
      if (isLog) {
        System.out.println(request.method() + " " + request.url() + " = " + code + ": " + responseBody);
      }
      return code;
    } catch (Exception e) {
      System.out.println(request.method() + " " + request.url() + " = " + e.getMessage());
      return -1;
    }
  }

  private static HttpUrl toHttp(String url, Integer resultId) {
    return HttpUrl.get(API_ROOT + url).newBuilder().addQueryParameter("resultId", resultId.toString()).build();
  }

  private static HttpUrl toHttp(String url) {
    return HttpUrl.get(API_ROOT + url);
  }

  private static final MediaType APPLICATION_JSON = MediaType.parse("application/json");
  private static final String API_ROOT = new SystemVariables().api();
}
