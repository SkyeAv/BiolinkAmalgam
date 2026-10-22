package biolinkamalgam.ingest

import biolinkamalgam.config.Config.Aria2Config

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{StandardOpenOption => SO}
import java.util.concurrent.{CompletableFuture, Semaphore}

/** Bulk snapshot downloads. Two engines behind one seam, selected in config/download.yaml:
  *
  *   - `Aria2BulkDownloader` (default): ONE aria2c process for the whole snapshot — multi-connection range requests per
  *     file times concurrent files, resumable via aria2 control files. For plain HTTPS from stars.renci.org this
  *     saturates the link; per-connection server throttling is the ceiling and parallel segments are the only lever.
  *   - `JdkBulkDownloader` (fallback): the original async JDK HttpClient engine, for boxes without aria2c.
  */
trait BulkDownloader {

  /** Download every (relative path, URL) pair under `destDir`, resuming/skipping where possible. */
  def downloadAll(files: Seq[(os.SubPath, String)], destDir: os.Path): Unit
}

object BulkDownloader {

  /** Drop files already fully downloaded (local size == remote Content-Length). One batched round of async HEADs, so
    * reruns of an interrupted snapshot cost seconds, not a re-download.
    */
  def skipComplete(
      client: HttpClient,
      files: Seq[(os.SubPath, String)],
      destDir: os.Path
  ): Seq[(os.SubPath, String)] = {
    val checks = files.map { case pair @ (sub, url) =>
      val head = client.sendAsync(
        HttpRequest.newBuilder(URI.create(url)).method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
        HttpResponse.BodyHandlers.discarding()
      )
      head.thenApply { resp =>
        val remoteSize = resp.headers().firstValueAsLong("Content-Length").orElse(-1L)
        val target = (destDir / sub).toNIO
        val complete =
          remoteSize >= 0 && java.nio.file.Files.isRegularFile(target) &&
            java.nio.file.Files.size(target) == remoteSize
        pair -> complete
      }
    }
    CompletableFuture
      .allOf(checks: _*)
      .join()
    checks.map(_.join()).collect { case (pair, false) => pair }
  }
}

/** aria2c engine: one process, one input file. Resume is aria2's own (control files + `--continue`), plus the HEAD
  * pre-filter for completed files (aria2 alone would re-fetch those).
  */
class Aria2BulkDownloader(cfg: Aria2Config) extends BulkDownloader {

  /** aria2 `--input-file` body: URL line followed by an indented per-file `out=` preserving the snapshot's relative
    * layout. Pure for testing.
    */
  def inputFile(files: Seq[(os.SubPath, String)]): String =
    files
      .map { case (sub, url) => s"$url\n  out=$sub" }
      .mkString("\n") + "\n"

  override def downloadAll(files: Seq[(os.SubPath, String)], destDir: os.Path): Unit = {
    val client = HttpClient.newHttpClient()
    val remaining = BulkDownloader.skipComplete(client, files, destDir)
    if (remaining.isEmpty) return
    os.makeDir.all(destDir)
    val input = destDir / "aria2-input.txt"
    os.write.over(input, inputFile(remaining))
    val cmd = Seq(
      cfg.binary,
      s"--dir=$destDir",
      s"--input-file=$input",
      s"--max-connection-per-server=${cfg.connectionsPerFile}",
      s"--split=${cfg.split}",
      s"--min-split-size=${cfg.minSplitSize}",
      s"--max-concurrent-downloads=${cfg.concurrentFiles}",
      "--continue=true",
      "--file-allocation=none",
      "--auto-file-renaming=false",
      // Transient resilience: stars.renci.org intermittently answers 5xx during
      // multi-hour pulls, which aria2 classifies as file-not-found; the default
      // max-file-not-found=0 aborts the WHOLE run on a single blip (observed
      // 2026-10-22 at ~100GB: exit 29). Retry per file instead.
      "--max-file-not-found=20",
      "--max-tries=10",
      "--retry-wait=5",
      "--timeout=120",
      "--connect-timeout=30",
      "--console-log-level=warn",
      "--summary-interval=30"
    ) ++ cfg.extraArgs
    val proc = new ProcessBuilder(cmd: _*)
      .redirectErrorStream(true)
      .redirectOutput(ProcessBuilder.Redirect.INHERIT)
      .start()
    val exit = proc.waitFor()
    os.remove(input)
    if (exit != 0)
      throw new RuntimeException(s"aria2c failed with exit $exit for ${remaining.size} files into $destDir")
  }
}

/** JDK async HttpClient engine (fallback): bounded concurrency, `<file>.partial` + atomic rename, Range resume. */
class JdkBulkDownloader(maxConcurrent: Int = 8) extends BulkDownloader {

  private val client = HttpClient.newHttpClient()

  override def downloadAll(files: Seq[(os.SubPath, String)], destDir: os.Path): Unit = {
    val gate = new Semaphore(maxConcurrent)
    val futures = files.map { case (sub, url) =>
      CompletableFuture.runAsync(() => {
        gate.acquire()
        try download(url, destDir / sub)
        finally gate.release()
      })
    }
    CompletableFuture.allOf(futures: _*).join()
    ()
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
    ()
  }
}

/** JDK engine with per-file parallel HTTP Range chunks (aria2-style, no external binary). Chunks land in per-chunk part
  * files (streamed to disk, never held in memory) and are concatenated sequentially. Resume granularity is the whole
  * file (aria2's per-chunk resume is finer); completed files are skipped by the HEAD pre-filter either way. Exists to
  * be benchmarked against aria2 (`bench/download`), not as a default.
  */
class ChunkedJdkBulkDownloader(
    connectionsPerFile: Int = 8,
    maxConcurrentFiles: Int = 4
) extends BulkDownloader {

  private val client = HttpClient.newHttpClient()

  override def downloadAll(files: Seq[(os.SubPath, String)], destDir: os.Path): Unit = {
    val remaining = BulkDownloader.skipComplete(client, files, destDir)
    val gate = new Semaphore(maxConcurrentFiles)
    val futures = remaining.map { case (sub, url) =>
      CompletableFuture.runAsync(() => {
        gate.acquire()
        try downloadChunked(url, destDir / sub)
        finally gate.release()
      })
    }
    CompletableFuture.allOf(futures: _*).join()
    ()
  }

  /** One file in `connectionsPerFile` range chunks (single GET below the chunk threshold or when the server ignores
    * ranges), then concat + atomic rename.
    */
  def downloadChunked(url: String, dest: os.Path): Unit = {
    os.makeDir.all(dest / os.up)
    val uri = URI.create(url)
    val head = client.send(
      HttpRequest.newBuilder(uri).method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
      HttpResponse.BodyHandlers.discarding()
    )
    val size = head.headers().firstValueAsLong("Content-Length").orElse(-1L)
    if (size <= 0) { new JdkBulkDownloader().download(url, dest); return }

    val chunks = math.max(1, math.min(connectionsPerFile, size / (16 * 1024 * 1024)).toInt)
    if (chunks == 1) { new JdkBulkDownloader().download(url, dest); return }

    val bounds = (0 until chunks).map { i =>
      val from = size * i / chunks
      val to = if (i == chunks - 1) size - 1 else size * (i + 1) / chunks - 1
      (from, to)
    }
    val partFutures = bounds.zipWithIndex.map { case ((from, to), i) =>
      val part = dest.toNIO.resolveSibling(s"${dest.last}.chunk$i")
      val req = HttpRequest.newBuilder(uri).header("Range", s"bytes=$from-$to").GET().build()
      client.sendAsync(req, HttpResponse.BodyHandlers.ofFile(part)).thenApply { resp =>
        if (resp.statusCode() != 206)
          throw new RuntimeException(s"chunk $i of $url: expected 206, got HTTP ${resp.statusCode()}")
        part
      }
    }
    CompletableFuture.allOf(partFutures: _*).join()
    val parts = partFutures.map(_.join())

    val target = dest.toNIO
    val out = FileChannel.open(target, SO.CREATE, SO.WRITE, SO.TRUNCATE_EXISTING)
    try {
      parts.foreach { p =>
        val in = FileChannel.open(p, SO.READ)
        try in.transferTo(0, java.nio.file.Files.size(p), out)
        finally { in.close(); java.nio.file.Files.delete(p) }
      }
    } finally out.close()

    if (java.nio.file.Files.size(target) != size)
      throw new RuntimeException(
        s"chunked download size mismatch for $url: ${java.nio.file.Files.size(target)} != $size"
      )
  }
}
