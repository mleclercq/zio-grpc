package scalapb.zio_grpc

import io.grpc.CallOptions
import io.grpc.MethodDescriptor
import io.grpc.StatusException
import zio._
import zio.stream.ZStream
import java.util.concurrent.TimeUnit
import io.grpc.Deadline
import io.grpc.Status

/** A wrapper of a gRPC client call which returns a single value.
  */
final case class CallEffect[-R, Req, Res](
    call: (MethodDescriptor[Req, Res], CallOptions, UIO[SafeMetadata]) => ZIO[R, StatusException, Res]
) {
  def apply(
      method: MethodDescriptor[Req, Res],
      options: CallOptions,
      metadata: UIO[SafeMetadata]
  ): ZIO[R, StatusException, Res] =
    call(method, options, metadata)
}

/** A wrapper of a gRPC client call which returns a stream.
  */
final case class CallStream[-R, Req, Res](
    call: (MethodDescriptor[Req, Res], CallOptions, UIO[SafeMetadata]) => ZStream[R, StatusException, Res]
) {
  def apply(
      method: MethodDescriptor[Req, Res],
      options: CallOptions,
      metadata: UIO[SafeMetadata]
  ): ZStream[R, StatusException, Res] =
    call(method, options, metadata)
}

/** `ClientAspect` are transformations applied to client services by modifying `CallEffect` and `CallStream`.
  */
trait ClientAspect[-R] { self =>
  def effect[R1 <: R, Req, Res](call: CallEffect[R1, Req, Res]): CallEffect[R1, Req, Res]
  def stream[R1 <: R, Req, Res](call: CallStream[R1, Req, Res]): CallStream[R1, Req, Res]

  def andThen[R1 <: R](that: ClientAspect[R1]): ClientAspect[R1] =
    new ClientAspect[R1] {
      def effect[R2 <: R1, Req, Res](call: CallEffect[R2, Req, Res]): CallEffect[R2, Req, Res] =
        that.effect(self.effect(call))
      def stream[R2 <: R1, Req, Res](call: CallStream[R2, Req, Res]): CallStream[R2, Req, Res] =
        that.stream(self.stream(call))
    }

  def @@[R1 <: R](that: ClientAspect[R1]): ClientAspect[R1]  = andThen(that)
  def >>>[R1 <: R](that: ClientAspect[R1]): ClientAspect[R1] = andThen(that)
}

object ClientAspect {

  val identity: ClientAspect[Any] =
    new ClientAspect[Any] {
      def effect[R, Req, Res](call: CallEffect[R, Req, Res]): CallEffect[R, Req, Res] = call
      def stream[R, Req, Res](call: CallStream[R, Req, Res]): CallStream[R, Req, Res] = call
    }

  def mapCallOptions(f: CallOptions => CallOptions): ClientAspect[Any] = new ClientAspect[Any] {
    def effect[R, Req, Res](call: CallEffect[R, Req, Res]): CallEffect[R, Req, Res] =
      CallEffect((method, options, metadata) => call(method, f(options), metadata))
    def stream[R, Req, Res](call: CallStream[R, Req, Res]): CallStream[R, Req, Res] =
      CallStream((method, options, metadata) => call(method, f(options), metadata))
  }

  def mapMetadataZIO(f: SafeMetadata => UIO[SafeMetadata]): ClientAspect[Any] = new ClientAspect[Any] {
    def effect[R, Req, Res](call: CallEffect[R, Req, Res]): CallEffect[R, Req, Res] =
      CallEffect((method, options, metadata) => call(method, options, metadata.flatMap(f)))
    def stream[R, Req, Res](call: CallStream[R, Req, Res]): CallStream[R, Req, Res] =
      CallStream((method, options, metadata) => call(method, options, metadata.flatMap(f)))
  }

  def withMetadataZIO(metadata: UIO[SafeMetadata]): ClientAspect[Any] =
    mapMetadataZIO(_ => metadata)

  def withCallOptions(callOptions: CallOptions): ClientAspect[Any] =
    mapCallOptions(_ => callOptions)

  def withDeadline(deadline: Deadline): ClientAspect[Any] =
    mapCallOptions(_.withDeadline(deadline))

  def withTimeout(duration: Duration): ClientAspect[Any] =
    mapCallOptions(_.withDeadlineAfter(duration.toNanos, TimeUnit.NANOSECONDS))

  def withTimeoutMillis(millis: Long): ClientAspect[Any] =
    withTimeout(Duration.fromMillis(millis))
}

trait AspectTransformableClient[-R, Repr[-_]] {
  def transform[R1 <: R](aspect: ClientAspect[R1]): Repr[R1]

  def @@[R1 <: R](aspect: ClientAspect[R1]): Repr[R1] = transform(aspect)

  def mapCallOptions(f: CallOptions => CallOptions): Repr[R]        = transform(ClientAspect.mapCallOptions(f))
  def mapMetadataZIO(f: SafeMetadata => UIO[SafeMetadata]): Repr[R] = transform(ClientAspect.mapMetadataZIO(f))
  def withMetadataZIO(metadata: UIO[SafeMetadata]): Repr[R]         = transform(ClientAspect.withMetadataZIO(metadata))
  def withCallOptions(callOptions: CallOptions): Repr[R]            = transform(ClientAspect.withCallOptions(callOptions))
  def withDeadline(deadline: Deadline): Repr[R]                     = transform(ClientAspect.withDeadline(deadline))
  def withTimeout(duration: Duration): Repr[R]                      = transform(ClientAspect.withTimeout(duration))
  def withTimeoutMillis(millis: Long): Repr[R]                      = transform(ClientAspect.withTimeoutMillis(millis))
}

// code-gen mockup
class MyService[-R](channel: ZChannel, aspects: ClientAspect[R]) extends AspectTransformableClient[R, MyService] {

  val helloWorldMethod: MethodDescriptor[Int, String] = ???

  def helloWorld(req: Int): ZIO[R, StatusException, String] = {
    // The "raw" CallEffect that does the actual call
    val callEffect = CallEffect[Any, Int, String]((method, options, metadata) =>
      metadata.flatMap(
        scalapb.zio_grpc.client.ClientCalls.unaryCall(channel, method, options, _, req)
      )
    )

    // Modify the CallEffect with the aspects and call it
    aspects.effect(callEffect).call(helloWorldMethod, CallOptions.DEFAULT, SafeMetadata.make)

  }
  def transform[R1 <: R](aspect: ClientAspect[R1]): MyService[R1] =
    new MyService[R1](channel, aspects >>> aspect) {}
}

object MyService {
  def scoped[R](channel: ZManagedChannel, aspect: ClientAspect[R]): ZIO[Scope, Throwable, MyService[R]] =
    channel.map(new MyService[R](_, aspect))

  def scoped(channel: ZManagedChannel): ZIO[Scope, Throwable, MyService[Any]] = scoped(channel, ClientAspect.identity)
}

// mockup of a tracing service
trait Tracer

object Tracer {

  class SpanAttributes
  sealed trait SpanKind
  object SpanKind {
    case object CLIENT extends SpanKind
    case object SERVER extends SpanKind
  }

  def span[R, E, A](name: String, attributes: SpanAttributes, kind: SpanKind)(
      zio: ZIO[R, E, A]
  ): ZIO[R with Tracer, E, A] = ???
  def spanStream[R, E, A](name: String, attributes: SpanAttributes, kind: SpanKind)(
      zio: ZStream[R, E, A]
  ): ZStream[R with Tracer, E, A] = ???
  def injectContext[C](carrier: C): UIO[C] = ???
  def addAttributes(attributes: SpanAttributes): UIO[Unit] = ???
}

// Example of a client aspect that adds tracing to a gRPC client service
object GrpcTracer {

  def spanAttributes[Req, Res](method: MethodDescriptor[Req, Res], options: CallOptions): Tracer.SpanAttributes = ???
  def statusAttribute(status: Status): Tracer.SpanAttributes                                                    = ???

  val grpcClientTracer = new ClientAspect[Tracer] {

    def effect[R <: Tracer, Req, Res](call: CallEffect[R, Req, Res]): CallEffect[R, Req, Res] =
      CallEffect((method, options, metadata) =>
        Tracer.span(method.getFullMethodName, spanAttributes(method, options), Tracer.SpanKind.CLIENT)(
          for {
            m <- Tracer.injectContext(metadata)
            a <- call(method, options, m)
            _ <- Tracer.addAttributes(statusAttribute(Status.OK))
          } yield a
        )
      )

    def stream[R <: Tracer, Req, Res](call: CallStream[R, Req, Res]): CallStream[R, Req, Res] =
      CallStream((method, options, metadata) =>
        Tracer.spanStream(method.getFullMethodName, spanAttributes(method, options), Tracer.SpanKind.CLIENT)(
          for {
            m <- ZStream.fromZIO(Tracer.injectContext(metadata))
            a <- call(method, options, m) ++ ZStream.execute(Tracer.addAttributes(statusAttribute(Status.OK)))
          } yield a
        )
      )
  }
}

// Usage mockup
object MyServiceUsage {

  val myService: ZIO[Scope, Throwable, MyService[Tracer]] =
    MyService.scoped(???, ClientAspect.withTimeout(1.second) >>> GrpcTracer.grpcClientTracer)
}
