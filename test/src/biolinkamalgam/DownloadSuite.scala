package biolinkamalgam

import biolinkamalgam.config.Config
import biolinkamalgam.ingest.{Aria2BulkDownloader, BulkDownloader, FixtureSnapshotProvider, JdkBulkDownloader, RemoteSnapshotProvider}
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
      val content = ("biolinkamalgam content line\n" * 100).getBytes("UTF-8")
      withServer(content) { (url, requests) =>
        val dest = os.temp.dir(prefix = "biolinkamalgam-dl") / "f.txt"
        new JdkBulkDownloader().download(url, dest)
        assert(os.read.bytes(dest).sameElements(content))
        assert(requests.toArray.exists {
          // the client always sends a Range header; a fresh download starts at byte 0
          case r: Request => r.method == "GET" && r.range.forall(_ == "bytes=0-")
        })
      }
    }

    test("interrupted download resumes via HTTP Range from the .partial file") {
      val content = ("biolinkamalgam content line\n" * 100).getBytes("UTF-8")
      withServer(content) { (url, requests) =>
        val dest = os.temp.dir(prefix = "biolinkamalgam-dl-resume") / "f.txt"
        val partial = dest / os.up / (dest.last + ".partial")
        os.write(partial, content.slice(0, 500)) // simulate an interrupted run

        new JdkBulkDownloader().download(url, dest)

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
        val dest = os.temp.dir(prefix = "biolinkamalgam-dl-skip") / "f.txt"
        os.write(dest, content)

        new JdkBulkDownloader().download(url, dest)

        assert(!requests.toArray.exists { case r: Request =>
          r.method == "GET"
        })
      }
    }

    test("listing parser keeps files, drops dirs, sort links, and anything conflated") {
      val html =
        """<a href="?C=N;O=D">Name</a> <a href="../">Parent</a> <a href="sub/">sub</a>
          |<a href="Gene.txt">Gene.txt</a> <a href="Gene.txt">dup</a>
          |<a href="DrugChemicalConflated.txt.gz">x</a> <a href="Protein.txt.gz">p</a>
          |<a href="README.md">readme</a>""".stripMargin
      assert(RemoteSnapshotProvider.parseListing(html, "compendia") == Seq("Gene.txt", "Protein.txt.gz", "README.md"))
      // synonyms: .txt / .txt.gz only
      assert(RemoteSnapshotProvider.parseListing(html, "synonyms") == Seq("Gene.txt", "Protein.txt.gz"))
    }

    test("aria2 input file preserves the snapshot layout per URL") {
      val dl = new Aria2BulkDownloader(Config.Aria2Config())
      val body = dl.inputFile(
        Seq(
          os.sub / "compendia" / "Gene.txt" -> "https://h/v/compendia/Gene.txt",
          os.sub / "synonyms" / "Gene.txt.gz" -> "https://h/v/synonyms/Gene.txt.gz"
        )
      )
      assert(
        body ==
          "https://h/v/compendia/Gene.txt\n  out=compendia/Gene.txt\n" +
          "https://h/v/synonyms/Gene.txt.gz\n  out=synonyms/Gene.txt.gz\n"
      )
    }

    test("bulk skip-complete drops only fully downloaded files (batched HEADs)") {
      val content = "payload".getBytes("UTF-8")
      withServer(content) { (url, requests) =>
        val dir = os.temp.dir(prefix = "biolinkamalgam-dl-bulk")
        os.write(dir / "done.txt", content)
        os.write(dir / "short.txt", content.take(3))
        val remaining = BulkDownloader.skipComplete(
          java.net.http.HttpClient.newHttpClient(),
          Seq(os.sub / "done.txt" -> url, os.sub / "short.txt" -> url, os.sub / "absent.txt" -> url),
          dir
        )
        assert(remaining.map(_._1) == Seq(os.sub / "short.txt", os.sub / "absent.txt"))
        assert(requests.toArray.forall { case r: Request => r.method == "HEAD" })
      }
    }

    test("aria2 engine fetches a whole file set into the snapshot layout") {
      val hasAria2 = scala.util.Try(os.proc("aria2c", "--version").call(check = false).exitCode == 0).getOrElse(false)
      if (!hasAria2) println("aria2c not on PATH — skipping live aria2 engine test")
      else {
        val content = ("biolinkamalgam aria2 line\n" * 2000).getBytes("UTF-8")
        withServer(content) { (url, _) =>
          val dir = os.temp.dir(prefix = "biolinkamalgam-dl-aria2")
          new Aria2BulkDownloader(Config.Aria2Config(minSplitSize = "1M"))
            .downloadAll(Seq(os.sub / "compendia" / "A.txt" -> url, os.sub / "synonyms" / "A.txt" -> url), dir)
          assert(os.read.bytes(dir / "compendia" / "A.txt").sameElements(content))
          assert(os.read.bytes(dir / "synonyms" / "A.txt").sameElements(content))
          assert(!os.exists(dir / "aria2-input.txt"))
        }
      }
    }

    test("download config loads defaults and rejects unknown engines") {
      val dir = os.temp.dir(prefix = "biolinkamalgam-dlcfg")
      assert(Config.loadDownload(dir / "missing.yaml") == Config.DownloadConfig())
      os.write(dir / "ok.yaml", "engine: jdk\naria2:\n  connections_per_file: 4\n")
      val ok = Config.loadDownload(dir / "ok.yaml")
      assert(ok.engine == "jdk" && ok.aria2.connectionsPerFile == 4 && ok.aria2.split == 16)
      os.write(dir / "bad.yaml", "engine: wget\n")
      val err = intercept[IllegalArgumentException](Config.loadDownload(dir / "bad.yaml"))
      assert(err.getMessage.contains("wget"))
    }
  }
}
