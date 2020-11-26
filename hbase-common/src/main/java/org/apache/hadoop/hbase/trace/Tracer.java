package org.apache.hadoop.hbase.trace;

import io.opentracing.Scope;
import org.apache.hadoop.hbase.trace.Span;
import io.opentracing.SpanContext;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.hbase.trace.TraceUtils;

public class Tracer {
  private static Tracer globalTracer;
  public io.opentracing.Tracer tracer;

  public Tracer(io.opentracing.Tracer tracer) {
    this.tracer = tracer;
  }

  public static io.opentracing.Tracer get() {
    return GlobalTracer.get();
  }

  public static Runnable wrap(String description, Runnable runnable){
    if(isTracing()){
      io.opentracing.Tracer tracer = get();
      try(Scope scope = tracer.buildSpan(description).startActive(false)){
        return new TracedRunnable(runnable, tracer);
      }
    } else {
      return runnable;
    }
  }

  public static Runnable wrap(Runnable runnable){
    if(isTracing()){
      io.opentracing.Tracer tracer = get();
      return new TracedRunnable(runnable, tracer);
    } else {
      return runnable;
    }
  }


  // Keeping this function at the moment for HTrace compatiblity,
  // in fact all threads share a single global tracer for OpenTracing.
  public static Tracer curThreadTracer() {
    if (globalTracer == null) {
      globalTracer = new Tracer(GlobalTracer.get());
    }
    return globalTracer;
  }

  /***
   * Return active span.
   * @return org.apache.hadoop.tracing.Span
   */
  public static Span getCurrentSpan() {
    io.opentracing.Span span = GlobalTracer.get().activeSpan();
    if (span != null) {
      // Only wrap the OpenTracing span when it isn't null
      return new Span(span);
    } else {
      return null;
    }
  }

  public TraceScope newScope(String description) {
    Scope scope = tracer.buildSpan(description).startActive(true);
    return new TraceScope(scope);
  }

  public Span newSpan(String description, SpanContext spanCtx) {
    io.opentracing.Span otspan = tracer.buildSpan(description).asChildOf(spanCtx).start();
    return new Span(otspan);
  }

  public TraceScope newScope(String description, SpanContext spanCtx) {
    io.opentracing.Scope otscope =
      tracer.buildSpan(description).asChildOf(spanCtx).startActive(true);
    return new TraceScope(otscope);
  }

  public TraceScope newScope(String description, Span span) {
    if(span != null){
      io.opentracing.Scope otscope =
        tracer.buildSpan(description).asChildOf(span.context()).startActive(true);
      return new TraceScope(otscope);
    }
    return null;
  }

  public TraceScope newScope(String description, SpanContext spanCtx, boolean finishSpanOnClose) {
    io.opentracing.Scope otscope =
      tracer.buildSpan(description).asChildOf(spanCtx).startActive(finishSpanOnClose);
    return new TraceScope(otscope);
  }

  public TraceScope activateSpan(Span span) {
    return new TraceScope(tracer.scopeManager().activate(span.otSpan, true));
  }

  //TODO: Check if this is working as expected
  public TraceScope continueSpan(Span span){
    if(span != null){
      return new TraceScope(tracer.scopeManager().activate(span.otSpan, false));
    }
    return null;
  }

  public void close() {
  }

  public static boolean isTracing(){
    //TODO: Check how to see is tracing happenning
    return GlobalTracer.get().activeSpan() != null;
  }

  public static void addTimelineAnnotation(String tag){
    if(getCurrentSpan() != null){
      getCurrentSpan().addTimelineAnnotation(tag);
    }
  }

  public static class Builder {
    static Tracer globalTracer;

    private String name;

    public Builder(final String name) {
      this.name = name;
    }

    public Tracer build() {
      if (globalTracer == null) {
        io.opentracing.Tracer oTracer = TraceUtils.createAndRegisterTracer(name);
        globalTracer = new Tracer(oTracer);
      }
      return globalTracer;
    }
  }

}
