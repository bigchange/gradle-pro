package com.lieluobo.urllist;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.higgs.bi.protobuf.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.okhttp.OkHttpChannelBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Jerry on 2017/12/4.
 * LieluoUrlListClient
 */
public class LieluoUrlListClient {
  private final ManagedChannel channel;
  private final ActionEventServiceGrpc.ActionEventServiceBlockingStub blockingStub;
  private static Logger logger = LoggerFactory.getLogger(LieluoUrlListClient.class);
  private static  JsonFormat.Parser parser = JsonFormat.parser();
  private static  JsonFormat.Printer printer = JsonFormat.printer();

  /**
   * Construct client connecting to server at {@code host:port}.
   */
  public LieluoUrlListClient(String host, int port) {
    this(OkHttpChannelBuilder.forAddress(host, port).usePlaintext(true));
  }

  /**
   * Construct client for accessing RouteGuide server using the existing channel.
   */
  LieluoUrlListClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = ActionEventServiceGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  public PageActionEventResponse page(PageActionEventRequest request) {
    PageActionEventResponse pageActionEventResponse = null;
    try {
      pageActionEventResponse = blockingStub.page(request);
    } catch (Exception e) {
      logger.error("page error :" + e.getMessage());
    }
    return pageActionEventResponse;
  }

  /**
   *按字符缓冲写入 BufferedWriter and BufferedOutputStream
   * @param fileName
   * @param  list 写入字符串数组
   */
  public static void output(String fileName, ArrayList<String> list) {
    File f = new File(fileName);
    BufferedOutputStream bos = null;
    OutputStreamWriter writer = null;
    BufferedWriter bw = null;
    try {
      // 文件名存在的话，往后面添加到文件末尾；每天生成一个新的文件
      OutputStream os = new FileOutputStream(f, true);
      bos = new BufferedOutputStream(os);
      writer = new OutputStreamWriter(bos);
      bw = new BufferedWriter(writer);
      for (int i = 0; i < list.size(); i++) {
        bw.write(list.get(i) + "\n");
      }
      bw.flush();
    } catch (FileNotFoundException e) {
      logger.info("[exception] => FileNotFoundException ");
    } catch (IOException e) {
      logger.info("[exception] => IOException");
    } finally {
      try {
        if (bw != null) {
          bw.close();
        }
        if (writer != null) {
          writer.close();
        }
        if (bos != null) {
          bos.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void convertJsonToProto(JsonObject request, com.higgs.bi.protobuf.grpc.PageActionEventRequest.Builder
            pageActionEventRequestBuilder) {
    try {
      parser.ignoringUnknownFields().merge(request.toString(), pageActionEventRequestBuilder);
    } catch (InvalidProtocolBufferException e) {
      logger.info("convertJsonToProto error !!" + e);
    }
  }

  public static DateFormat getDataFormator() {
    return new SimpleDateFormat("yyyy-MM-dd");
  }

  protected static String formatDate(long millisecond) {
    Date date = new Date();
    date.setTime(millisecond);
    return  getDataFormator().format(date);
  }

  protected static void toArrayList(String[] str, ArrayList<String> list) {
    int size = str.length;
    for (String s : str) {
      list.add(s);
    }
  }

  protected static  ArrayList<String>  outputFormat(List<ActionEvent> actionEvents) {
    ArrayList<String> ret = new ArrayList<>();
    int size = actionEvents.size();
    for (int i = 0; i < size; i++) {
      JsonObject jsonObject = new JsonObject();
      ActionEvent actionEvent = actionEvents.get(i);

    }
    return  ret;
  }

  protected void task(JsonObject config) throws InterruptedException {
    System.out.println("============ start ===============");
    String dir = config.getString("data.dir", "");
    String host = config.getString("grpc.host", "");
    int port = config.getInteger("grpc.port", 17117);
    int offset = config.getInteger("day.offset", -1);
    String startDay = config.getString("start.date", "");
    String endDay = config.getString("end.date", "");
    int typeMethod = config.getInteger("type.get.method", -1);

    // pageActionEventRequest
    JsonObject request = config.getJsonObject("pageActionEventRequest", new JsonObject());
    try {
      String fileName;
      PageActionEventRequest.Builder builder = PageActionEventRequest.newBuilder();
      convertJsonToProto(request,  builder);
      long started;
      long ended;
      // parserJson(request, builder);
      // 设置事件发生的时间(指定的时间段)
      if (typeMethod > 0) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(getDataFormator().parse(startDay));
        started = calendar.getTimeInMillis();
        System.out.println("started millis:" + started + " of " + startDay);
        String startFileName = getDataFormator().format(calendar.getTime());
        calendar.setTime(getDataFormator().parse(endDay));
        ended = calendar.getTimeInMillis();
        System.out.println("ended millis:" + ended + " of " + endDay);
        String endFileName = getDataFormator().format(calendar.getTime());
        fileName = startFileName + "--" + endFileName;
      } else {
        // 确保时间开始为当天的凌晨（定时任务）
        ended = System.currentTimeMillis();
        String endFileName = formatDate(ended);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(getDataFormator().parse(endFileName));
        calendar.add(Calendar.DAY_OF_MONTH, offset);
        started = calendar.getTimeInMillis();
        String startFileName = formatDate(started);
        System.out.println("started millis:" + started + " of " + startFileName) ;
        System.out.println("ended millis:" + ended + " of " + endFileName);
        fileName = startFileName + "--" + endFileName;
      }
      // 获取的数据不包括 endFileName 当天的内容
      Timestamp startedTimeStamp = Timestamp.newBuilder().setSeconds(started / 1000)
              .setNanos((int) ((started % 1000) * 1000000)).build();
      Timestamp endedTimeStamp = Timestamp.newBuilder()
              .setSeconds(ended / 1000).setNanos((int) ((ended % 1000) * 1000000)).build();
      int queryCount = builder.getQueryCount();
      for (int i = 0; i < queryCount; i++) {
        PageActionEventRequest.Query query = builder.getQuery(i);
        query = query.toBuilder().setStart(startedTimeStamp).setEnd(endedTimeStamp).build();
        builder.setQuery(i, query);
      }
      String saveFile = dir + "/" + fileName + ".txt";
      long timeStart = System.currentTimeMillis();
      ArrayList<String> ret = new ArrayList<>();
      long totalPages = requestData(builder, ret, 1);
      output(saveFile, ret);
      int getTotalData = ret.size();
      for (int i = 2; i <= totalPages; i ++) {
        if (i % (totalPages / 4) == 0 || i == totalPages) {
          System.out.println("fetch page:" + i + " / " + totalPages + "...");
        }
        ret.clear();
        builder.setPage(i);
        requestData(builder, ret, i);
        output(saveFile, ret);
        getTotalData += ret.size();
      }
      long timeEnd= System.currentTimeMillis();
      System.out.println("fetched total data:" + getTotalData + ", time: " + ( timeEnd - timeStart ) / 1000 + " s, task finished!!");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected long requestData(PageActionEventRequest.Builder builder , ArrayList<String> ret, int page) throws InvalidProtocolBufferException {
    PageActionEventResponse pageActionEventResponse = page(builder.build());
    List<ActionEvent> actionEventList = pageActionEventResponse.getActionEventsList();
    for (int i = 0; i < actionEventList.size(); i++) {
      ret.add(
              new String(printer.print(actionEventList.get(i).toBuilder())
                      .replace("\n", "")
              )
      );
    }
    Pagination pagination = pageActionEventResponse.getMeta().getPagination();
    if (page == 1) {
      System.out.println("page:" + pagination.getPage() + ", size:" + pagination.getTotal() + ", total:" + pagination.getTotal() + ", totalPage:" + pagination.getTotalPages());
    }
    return pagination.getTotalPages();
  }

  protected static void parserJson(JsonObject request, PageActionEventRequest.Builder builder) {
    JsonArray querys = request.getJsonArray("query", new JsonArray());
    for (int i = 0; i < querys.size(); i++) {
      JsonObject query = querys.getJsonObject(i);
      //平台
      String platform = query.getString("platform", "");
      //类型
      String type = query.getString("type", "");
      //事件KEY
      String action = query.getString("action", "");
      //事件触发所在的模块名
      String module = query.getString("module", "");
      //规则化url
      String path = query.getString("path", "");
      //事件发生开始时间
      //事件发生结束时间
      //渠道，如：c,cw,hr
      String channel = query.getString("channel", "");
      builder.addQuery(PageActionEventRequest.Query.newBuilder()
              .setPlatform(platform)
              .setType(type)
              .setAction(action)
              .setModule(module)
              .setPath(path)
              .setChannel(channel).build());
    }

  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
    }
    String confPath = args[0];

    String configStr = null;
    try {
      configStr = new String(Files.readAllBytes(Paths.get(confPath)));
    } catch (IOException e) {
      e.printStackTrace();
    }
    JsonObject config = new JsonObject(configStr);
    String host = config.getString("grpc.host", "");
    int port = config.getInteger("grpc.port", 17117);
    int typeMethod = config.getInteger("type.get.method", -1);
    LieluoUrlListClient client = new LieluoUrlListClient(host, port);
    try {
      if (typeMethod == 0) {
        ScheduledExecutorService scheduExec = Executors.newScheduledThreadPool(4);
        scheduExec.scheduleAtFixedRate(() -> {
          try {
            client.task(config);
          } catch (InterruptedException e) {
            System.err.println("task error!!");
            e.printStackTrace();
            try {
              client.shutdown();
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          }
        }, 0, 2, TimeUnit.MINUTES);
      } else if (typeMethod > 0) {
        client.task(config);
        client.shutdown();
        System.err.println("task finished!!");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
