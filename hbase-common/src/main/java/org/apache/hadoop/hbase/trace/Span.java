package org.apache.hadoop.hbase.trace;

import io.opentracing.SpanContext;
import org.apache.hadoop.hbase.util.Bytes;

public class Span {
  public io.opentracing.Span otSpan;

  public Span(io.opentracing.Span span) {
    this.otSpan = span;
  }

  public io.opentracing.Span span() {
    return this.otSpan;
  }

  public Span addKVAnnotation(String key, String value) {
    this.otSpan = otSpan.setTag(key, value);
    return this;
  }

  public Span addTimelineAnnotation(String msg) {
    this.otSpan = otSpan.log(msg);
    return this;
  }

  public SpanContext context() {
    return this.otSpan.context();
  }

  public void finish() {
    this.otSpan.finish();
  }

  public void close() {
  }

  public void addKVAnnotation(byte[] exceptions, byte[] toBytes) {
    //TODO: Review this if needed ot not
    this.otSpan = otSpan.setTag(Bytes.toString(exceptions), Bytes.toString(toBytes));
  }
}
