package org.apache.hadoop.hbase.trace;

import com.google.protobuf.ByteString;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SpanReceiverInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TraceUtils {

  static final String DEFAULT_HBASE_PREFIX = "hbase.htrace.";

  static final Logger LOG = LoggerFactory.getLogger(org.apache.hadoop.tracing.TraceUtils.class);



  public static io.opentracing.Tracer createAndRegisterTracer(String name) {
    if (!GlobalTracer.isRegistered()) {
      io.jaegertracing.Configuration config = io.jaegertracing.Configuration.fromEnv(name);
      Tracer tracer = config.getTracerBuilder().build();
      GlobalTracer.register(tracer);
    }
    return GlobalTracer.get();
  }

  public static SpanContext byteStringToSpanContext(ByteString byteString) {
    if (byteString == null || byteString.isEmpty()) {
      LOG.debug("The provided serialized context was null or empty");
      return null;
    }

    SpanContext context = null;
    ByteArrayInputStream stream = new ByteArrayInputStream(byteString.toByteArray());

    try {
      ObjectInputStream objStream = new ObjectInputStream(stream);
      Map<String, String> carrier = (Map<String, String>) objStream.readObject();

      context =
        GlobalTracer.get().extract(Format.Builtin.TEXT_MAP, new TextMapExtractAdapter(carrier));
    } catch (Exception e) {
      LOG.warn("Could not deserialize context {}", e);
    }

    return context;
  }

  public static ByteString spanContextToByteString(SpanContext context) {
    if (context == null) {
      LOG.debug("No SpanContext was provided");
      return null;
    }

    Map<String, String> carrier = new HashMap<String, String>();
    GlobalTracer.get().inject(context, Format.Builtin.TEXT_MAP, new TextMapInjectAdapter(carrier));
    if (carrier.isEmpty()) {
      LOG.warn("SpanContext was not properly injected by the Tracer.");
      return null;
    }

    ByteString byteString = null;
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    try {
      ObjectOutputStream objStream = new ObjectOutputStream(stream);
      objStream.writeObject(carrier);
      objStream.flush();

      byteString = ByteString.copyFrom(stream.toByteArray());
      LOG.debug("SpanContext serialized, resulting byte length is {}", byteString.size());
    } catch (IOException e) {
      LOG.warn("Could not serialize context {}", e);
    }

    return byteString;
  }

}
