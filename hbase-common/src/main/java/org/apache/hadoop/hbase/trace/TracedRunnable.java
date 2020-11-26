package org.apache.hadoop.hbase.trace;

import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.Span;
/*
This class is similar to TracedRunnable (https://github.com/opentracing-contrib/java-concurrent/blob/master/src/main/java/io/opentracing/contrib/concurrent/TracedRunnable.java)
 */
public class TracedRunnable implements Runnable {

  private final Runnable delegate;
  private final Span span;
  private final Tracer tracer;

  public TracedRunnable(Runnable delegate, Tracer tracer) {
    this(delegate, tracer, tracer.activeSpan());
  }

  public TracedRunnable(Runnable delegate, Tracer tracer, Span span) {
    this.delegate = delegate;
    this.tracer = tracer;
    this.span = span;
  }

  @Override
  public void run() {
    try (Scope scope = tracer.scopeManager().activate(span, true)) {
      delegate.run();
    }
  }
}
