package amalgam

import amalgam.ingest.{FixtureSnapshotProvider, RemoteSnapshotProvider}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentLinkedQueue
import utest._

/** Download behavior against an in-JVM fake HTTP server — real HTTP, no network. */
object DownloadSuite extends TestSuite {

  private case class Request(method: String, path: String, range: Option[String])

  /** Serves one fixed byte payload with HEAD and HTTP Range support. */
  private def withServer[A](content: Array[Byte])(body: (String, ConcurrentLinkedQueue[Request]) => A): A = {
    val requests = new ConcurrentLinkedQueue[Request]()
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/file",
      (exchange: HttpExchange) => {
        try {
          requests.add(
            Request(
              exchange.getRequestMethod,
              exchange.getRequestURI.getPath,
              Option(exchange.getRequestHeaders.getFirst("Range"))
            )
          )
          if (exchange.getRequestMethod == "HEAD") {
            exchange.getResponseHeaders.set("Content-Length", content.length.toString)
            exchange.sendResponseHeaders(200, -1)
          } else {
            val range = Option(exchange.getRequestHeaders.getFirst("Range"))
            val from = range match {
              case Some(r) if r.startsWith("bytes=") =>
                r.stripPrefix("bytes=").stripSuffix("-").toInt
              case _ => 0
            }
            val slice = content.slice(from, content.length)
            exchange.getResponseHeaders.set("Content-Length", slice.length.toString)
            if (from > 0) {
              exchange.getResponseHeaders.set(
                "Content-Range",
                s"bytes $from-${content.length - 1}/${content.length}"
              )
              exchange.sendResponseHeaders(206, slice.length)
            } else {
              exchange.sendResponseHeaders(200, slice.length)
            }
            exchange.getResponseBody.write(slice)
          }
        } finally exchange.close()
      }
    )
    server.start()
    try body(s"http://127.0.0.1:${server.getAddress.getPort}/file", requests)
    finally server.stop(0)
  }

  val tests = Tests {

    test("fixture provider throws on a missing dir instead of fetching") {
      val boom =
        try {
          new FixtureSnapshotProvider(os.pwd / "definitely-not-here")
            .ensureSnapshot(os.pwd / "out")
          throw new java.lang.AssertionError("expected IllegalArgumentException")
        } catch {
          case e: IllegalArgumentException => e
        }
      assert(boom.getMessage.contains("refusing to fetch"))
    }

    test("fresh download writes the whole file") {
      val content = ("amalgam content line\n" * 100).getBytes("UTF-8")
      withServer(content) { (url, requests) =>
        val dest = os.temp.dir(prefix = "amalgam-dl") / "f.txt"
        new RemoteSnapshotProvider("unused").download(url, dest)
        assert(os.read.bytes(dest).sameElements(content))
        assert(requests.toArray.exists {
          // the client always sends a Range header; a fresh download starts at byte 0
          case r: Request => r.method == "GET" && r.range.forall(_ == "bytes=0-")
        })
      }
    }

    test("interrupted download resumes via HTTP Range from the .partial file") {
      val content = ("amalgam content line\n" * 100).getBytes("UTF-8")
      withServer(content) { (url, requests) =>
        val dest = os.temp.dir(prefix = "amalgam-dl-resume") / "f.txt"
        val partial = dest / os.up / (dest.last + ".partial")
        os.write(partial, content.slice(0, 500)) // simulate an interrupted run

        new RemoteSnapshotProvider("unused").download(url, dest)

        assert(os.read.bytes(dest).sameElements(content))
        assert(!os.exists(partial))
        assert(requests.toArray.exists { case r: Request =>
          r.range.contains("bytes=500-")
        })
      }
    }

    test("complete files are skipped after a HEAD size check") {
      val content = "already here".getBytes("UTF-8")
      withServer(content) { (url, requests) =>
        val dest = os.temp.dir(prefix = "amalgam-dl-skip") / "f.txt"
        os.write(dest, content)

        new RemoteSnapshotProvider("unused").download(url, dest)

        assert(!requests.toArray.exists { case r: Request =>
          r.method == "GET"
        })
      }
    }
  }
}
