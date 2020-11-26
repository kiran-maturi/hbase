package org.apache.hadoop.hbase.trace;

import java.io.Closeable;

public class TraceScope implements Closeable {
  private io.opentracing.Scope otScope;

  public TraceScope(io.opentracing.Scope scope) {
    this.otScope = scope;
  }

  // Add tag to the span
  public Span addKVAnnotation(String key, String value) {
    // TODO: Try to reduce overhead from "new" object by returning void?
    return new Span(this.otScope.span().setTag(key, value));
  }

  public Span addKVAnnotation(String key, Number value) {
    return new Span(this.otScope.span().setTag(key, value));
  }

  public Span addTimelineAnnotation(String msg) {
    return new Span(this.otScope.span().log(msg));
  }

  public Span span() {
    return new Span(this.otScope.span());
  }

  public Span getSpan() {
        /* e.g.
      TraceScope scope = tracer.newScope(instance.getCommandName());
      if (scope.getSpan() != null) {
    */
    return new Span(this.otScope.span());
  }

  public void reattach() {
    // TODO: Server.java:2820
    // scope = GlobalTracer.get().scopeManager().activate(call.span, true);
  }

  public Span detach() {
    //TODO: detach the scope
    if (otScope != null && otScope.span() != null) {
      return new Span(otScope.span());
    }
    return null;
  }

  public void close() {
    otScope.close();
  }
}
