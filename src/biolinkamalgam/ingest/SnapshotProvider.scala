package biolinkamalgam.ingest

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{StandardOpenOption => SO}
import java.util.concurrent.{CompletableFuture, Semaphore}

/** The download/lookup seam. Production uses RemoteSnapshotProvider (async HTTP); tests inject FixtureSnapshotProvider,
  * which serves local fixture files and THROWS on any non-local path — tests are offline by construction, not by
  * convention.
  */
trait SnapshotProvider {

  /** Ensure a snapshot dir containing `compendia/` and `synonyms/` exists locally and return it. Implementations may
    * download, copy, or just point at fixtures.
    */
  def ensureSnapshot(destDir: os.Path): os.Path
}

/** Serves a pre-existing local directory as the snapshot. Used by tests (fixture dirs) and by `--babel <dir>` runs that
  * point at an already-downloaded tree.
  */
class FixtureSnapshotProvider(dir: os.Path) extends SnapshotProvider {
  override def ensureSnapshot(destDir: os.Path): os.Path = {
    if (!os.exists(dir))
      throw new IllegalArgumentException(
        s"snapshot dir $dir does not exist — refusing to fetch anything remotely"
      )
    dir
  }
}

/** Downloads a BABEL snapshot from stars.renci.org with the JDK's async HttpClient: bounded concurrency (no
  * thread-per-connection), `<file>.partial` + atomic rename, HTTP Range resume, size/ETag skip. Interrupted runs
  * resume, never restart.
  */
class RemoteSnapshotProvider(
    version: String,
    baseUrl: String = "https://stars.renci.org/var/babel_outputs",
    maxConcurrent: Int = 8
) extends SnapshotProvider {

  private val client = HttpClient.newHttpClient()

  override def ensureSnapshot(destDir: os.Path): os.Path = {
    val files = listRemote(version)
    val gate = new Semaphore(maxConcurrent)
    val futures = files.map { case (sub, url) =>
      CompletableFuture.runAsync(() => {
        gate.acquire()
        try download(url, destDir / sub)
        finally gate.release()
      })
    }
    CompletableFuture.allOf(futures: _*).join()
    destDir
  }

  /** Fetch the directory listings for compendia/ and synonyms/ (excluding anything conflated — drug/chemical and
    * gene/protein cliques stay distinct by design). Returns (relative path, URL) pairs.
    */
  def listRemote(version: String): Seq[(os.SubPath, String)] = {
    val hrefRe = "<a href=\"([^\"]+)\"".r
    Seq("compendia", "synonyms").flatMap { sub =>
      val listingUrl = s"$baseUrl/$version/$sub/"
      val body = client
        .send(
          HttpRequest.newBuilder(URI.create(listingUrl)).GET().build(),
          HttpResponse.BodyHandlers.ofString()
        )
        .body()
      hrefRe
        .findAllMatchIn(body)
        .map(_.group(1))
        .filterNot(_.contains("/"))
        .filterNot(_.contains("Conflated"))
        .filter(name => sub != "synonyms" || name.endsWith(".txt.gz") || name.endsWith(".txt"))
        .map(name => os.sub / sub / name -> s"$listingUrl$name")
        .toSeq
    }
  }

  /** Download one file with resume: append to `<file>.partial` via HTTP Range, then atomically rename into place. Skips
    * files already complete (size match via HEAD).
    */
  def download(url: String, dest: os.Path): Unit = {
    os.makeDir.all(dest / os.up)
    val target = dest.toNIO
    val partial = dest.toNIO.resolveSibling(dest.last + ".partial")

    val head = client.send(
      HttpRequest.newBuilder(URI.create(url)).method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
      HttpResponse.BodyHandlers.discarding()
    )
    val remoteSize =
      head.headers().firstValueAsLong("Content-Length").orElse(-1L)
    if (
      remoteSize >= 0 && java.nio.file.Files.isRegularFile(target) &&
      java.nio.file.Files.size(target) == remoteSize
    ) return // already complete

    val resumeFrom =
      if (java.nio.file.Files.isRegularFile(partial)) java.nio.file.Files.size(partial) else 0L
    val req = HttpRequest
      .newBuilder(URI.create(url))
      .header("Range", s"bytes=$resumeFrom-")
      .GET()
      .build()

    val future: CompletableFuture[Void] = client
      .sendAsync(req, HttpResponse.BodyHandlers.ofPublisher())
      .thenCompose { resp =>
        if (resp.statusCode() == 200 && resumeFrom > 0) {
          // Server ignored the Range header — restart the partial file.
          java.nio.file.Files.deleteIfExists(partial)
        } else if (resp.statusCode() != 200 && resp.statusCode() != 206) {
          throw new RuntimeException(s"download failed: HTTP ${resp.statusCode()} for $url")
        }
        val channel = FileChannel.open(
          partial,
          SO.CREATE,
          SO.WRITE,
          if (resp.statusCode() == 206) SO.APPEND else SO.TRUNCATE_EXISTING
        )
        val done = new CompletableFuture[Void]()
        resp
          .body()
          .subscribe(new java.util.concurrent.Flow.Subscriber[java.util.List[ByteBuffer]] {
            private var sub: java.util.concurrent.Flow.Subscription = _
            override def onSubscribe(s: java.util.concurrent.Flow.Subscription): Unit = {
              sub = s; s.request(1)
            }
            override def onNext(bs: java.util.List[ByteBuffer]): Unit = {
              bs.forEach(b => while (b.hasRemaining) channel.write(b))
              sub.request(1)
            }
            override def onError(t: Throwable): Unit = { channel.close(); done.completeExceptionally(t) }
            override def onComplete(): Unit = { channel.close(); done.complete(null) }
          })
        done
      }
    future.join()

    if (remoteSize >= 0 && java.nio.file.Files.size(partial) != remoteSize)
      throw new RuntimeException(
        s"incomplete download for $url: ${java.nio.file.Files.size(partial)} != $remoteSize"
      )
    java.nio.file.Files.move(
      partial,
      target,
      java.nio.file.StandardCopyOption.REPLACE_EXISTING,
      java.nio.file.StandardCopyOption.ATOMIC_MOVE
    )
  }
}
