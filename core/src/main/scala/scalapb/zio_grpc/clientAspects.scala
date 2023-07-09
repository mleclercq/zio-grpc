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
final case class CallEffect[Req, Res](
    call: (MethodDescriptor[Req, Res], CallOptions, UIO[SafeMetadata]) => ZIO[Any, StatusException, Res]
) {
  def apply(
      method: MethodDescriptor[Req, Res],
      options: CallOptions,
      metadata: UIO[SafeMetadata]
  ): ZIO[Any, StatusException, Res] =
    call(method, options, metadata)
}

/** A wrapper of a gRPC client call which returns a stream.
  */
final case class CallStream[Req, Res](
    call: (MethodDescriptor[Req, Res], CallOptions, UIO[SafeMetadata]) => ZStream[Any, StatusException, Res]
) {
  def apply(
      method: MethodDescriptor[Req, Res],
      options: CallOptions,
      metadata: UIO[SafeMetadata]
  ): ZStream[Any, StatusException, Res] =
    call(method, options, metadata)
}

/** `ClientAspect` are transformations applied to client services by modifying `CallEffect` and `CallStream`.
  */
trait ClientAspect { self =>
  def effect[Req, Res](call: CallEffect[Req, Res]): CallEffect[Req, Res]
  def stream[Req, Res](call: CallStream[Req, Res]): CallStream[Req, Res]

  def andThen(that: ClientAspect): ClientAspect =
    new ClientAspect {
      def effect[Req, Res](call: CallEffect[Req, Res]): CallEffect[Req, Res] =
        that.effect(self.effect(call))
      def stream[Req, Res](call: CallStream[Req, Res]): CallStream[Req, Res] =
        that.stream(self.stream(call))
    }

  def @@(that: ClientAspect): ClientAspect  = andThen(that)
  def >>>(that: ClientAspect): ClientAspect = andThen(that)
}

object ClientAspect {

  val identity: ClientAspect =
    new ClientAspect {
      def effect[Req, Res](call: CallEffect[Req, Res]): CallEffect[Req, Res] = call
      def stream[Req, Res](call: CallStream[Req, Res]): CallStream[Req, Res] = call
    }

  def mapCallOptions(f: CallOptions => CallOptions): ClientAspect = new ClientAspect {
    def effect[Req, Res](call: CallEffect[Req, Res]): CallEffect[Req, Res] =
      CallEffect((method, options, metadata) => call(method, f(options), metadata))
    def stream[Req, Res](call: CallStream[Req, Res]): CallStream[Req, Res] =
      CallStream((method, options, metadata) => call(method, f(options), metadata))
  }

  def mapMetadataZIO(f: SafeMetadata => UIO[SafeMetadata]): ClientAspect = new ClientAspect {
    def effect[Req, Res](call: CallEffect[Req, Res]): CallEffect[Req, Res] =
      CallEffect((method, options, metadata) => call(method, options, metadata.flatMap(f)))
    def stream[Req, Res](call: CallStream[Req, Res]): CallStream[Req, Res] =
      CallStream((method, options, metadata) => call(method, options, metadata.flatMap(f)))
  }

  def withMetadataZIO(metadata: UIO[SafeMetadata]): ClientAspect =
    mapMetadataZIO(_ => metadata)

  def withCallOptions(callOptions: CallOptions): ClientAspect =
    mapCallOptions(_ => callOptions)

  def withDeadline(deadline: Deadline): ClientAspect =
    mapCallOptions(_.withDeadline(deadline))

  def withTimeout(duration: Duration): ClientAspect =
    mapCallOptions(_.withDeadlineAfter(duration.toNanos, TimeUnit.NANOSECONDS))

  def withTimeoutMillis(millis: Long): ClientAspect =
    withTimeout(Duration.fromMillis(millis))
}

trait AspectTransformableClient[Repr] {
  def transform(aspect: ClientAspect): Repr

  def @@(aspect: ClientAspect): Repr = transform(aspect)

  def mapCallOptions(f: CallOptions => CallOptions): Repr        = transform(ClientAspect.mapCallOptions(f))
  def mapMetadataZIO(f: SafeMetadata => UIO[SafeMetadata]): Repr = transform(ClientAspect.mapMetadataZIO(f))
  def withMetadataZIO(metadata: UIO[SafeMetadata]): Repr         = transform(ClientAspect.withMetadataZIO(metadata))
  def withCallOptions(callOptions: CallOptions): Repr            = transform(ClientAspect.withCallOptions(callOptions))
  def withDeadline(deadline: Deadline): Repr                     = transform(ClientAspect.withDeadline(deadline))
  def withTimeout(duration: Duration): Repr                      = transform(ClientAspect.withTimeout(duration))
  def withTimeoutMillis(millis: Long): Repr                      = transform(ClientAspect.withTimeoutMillis(millis))
}

// code-gen mockup
class MyService(channel: ZChannel, aspects: ClientAspect = ClientAspect.identity)
    extends AspectTransformableClient[MyService] {

  val helloWorldMethod: MethodDescriptor[Int, String] = ???

  def helloWorld(req: Int): ZIO[Any, StatusException, String] = {
    // The "raw" CallEffect that does the actual call
    val callEffect = CallEffect[Int, String]((method, options, metadata) =>
      metadata.flatMap(
        scalapb.zio_grpc.client.ClientCalls.unaryCall(channel, method, options, _, req)
      )
    )

    // Modify the CallEffect with the aspects and call it
    aspects.effect(callEffect).call(helloWorldMethod, CallOptions.DEFAULT, SafeMetadata.make)

  }
  def transform(aspect: ClientAspect): MyService =
    new MyService(channel, aspects >>> aspect) {}
}

object MyService {
  def scoped(channel: ZManagedChannel, aspect: ClientAspect = ClientAspect.identity): ZIO[Scope, Throwable, MyService] =
    channel.map(new MyService(_, aspect))
}

// mockup of a tracing service
trait Tracer {
  import Tracer._

  def span[R, E, A](name: String, attributes: SpanAttributes, kind: SpanKind)(
      zio: ZIO[R, E, A]
  ): ZIO[R, E, A]
  def spanStream[R, E, A](name: String, attributes: SpanAttributes, kind: SpanKind)(
      zio: ZStream[R, E, A]
  ): ZStream[R, E, A]
  def injectContext[C](carrier: C): UIO[C]
  def addAttributes(attributes: SpanAttributes): UIO[Unit]
}

object Tracer {
  class SpanAttributes
  sealed trait SpanKind
  object SpanKind {
    case object CLIENT extends SpanKind
    case object SERVER extends SpanKind
  }
}

// Example of a client aspect that adds tracing to a gRPC client service
object GrpcTracer {

  def spanAttributes[Req, Res](method: MethodDescriptor[Req, Res], options: CallOptions): Tracer.SpanAttributes = ???
  def statusAttribute(status: Status): Tracer.SpanAttributes                                                    = ???

  val grpcClientTracer: URIO[Tracer, ClientAspect] =
    ZIO.serviceWith[Tracer](tracer =>
      new ClientAspect {

        def effect[Req, Res](call: CallEffect[Req, Res]): CallEffect[Req, Res] =
          CallEffect((method, options, metadata) =>
            tracer.span(method.getFullMethodName, spanAttributes(method, options), Tracer.SpanKind.CLIENT)(
              for {
                m <- tracer.injectContext(metadata)
                a <- call(method, options, m)
                _ <- tracer.addAttributes(statusAttribute(Status.OK))
              } yield a
            )
          )

        def stream[Req, Res](call: CallStream[Req, Res]): CallStream[Req, Res] =
          CallStream((method, options, metadata) =>
            tracer.spanStream(method.getFullMethodName, spanAttributes(method, options), Tracer.SpanKind.CLIENT)(
              for {
                m <- ZStream.fromZIO(tracer.injectContext(metadata))
                a <- call(method, options, m) ++ ZStream.execute(tracer.addAttributes(statusAttribute(Status.OK)))
              } yield a
            )
          )
      }
    )
}

// Usage mockup
object MyServiceUsage {

  val myService: ZIO[Scope with Tracer, Throwable, MyService] =
    for {
      clientTracer <- GrpcTracer.grpcClientTracer
      service      <- MyService.scoped(???, ClientAspect.withTimeout(1.second) >>> clientTracer)
    } yield service
}
