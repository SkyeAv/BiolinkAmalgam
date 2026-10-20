package biolinkamalgam.ingest

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI

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

/** Downloads a BABEL snapshot from stars.renci.org: lists compendia/ and synonyms/ (never anything conflated), then
  * hands the whole file set to ONE bulk download (aria2c by default, see config/download.yaml) instead of streaming
  * files one by one. Interrupted runs resume, never restart.
  */
class RemoteSnapshotProvider(
    version: String,
    downloader: BulkDownloader,
    baseUrl: String = "https://stars.renci.org/var/babel_outputs"
) extends SnapshotProvider {

  private val client = HttpClient.newHttpClient()

  override def ensureSnapshot(destDir: os.Path): os.Path = {
    downloader.downloadAll(listRemote(version), destDir)
    destDir
  }

  /** Fetch the directory listings for compendia/ and synonyms/ (excluding anything conflated — drug/chemical and
    * gene/protein cliques stay distinct by design). Returns (relative path, URL) pairs.
    */
  def listRemote(version: String): Seq[(os.SubPath, String)] =
    Seq("compendia", "synonyms").flatMap { sub =>
      val listingUrl = s"$baseUrl/$version/$sub/"
      val resp = client.send(
        HttpRequest.newBuilder(URI.create(listingUrl)).GET().build(),
        HttpResponse.BodyHandlers.ofString()
      )
      if (resp.statusCode() != 200)
        throw new RuntimeException(s"listing failed: HTTP ${resp.statusCode()} for $listingUrl")
      RemoteSnapshotProvider
        .parseListing(resp.body(), sub)
        .map(name => os.sub / sub / name -> s"$listingUrl$name")
    }
}

object RemoteSnapshotProvider {

  private val HrefRe = "<a href=\"([^\"]+)\"".r

  /** Pure listing filter: file links only (no subdirs/parents), never conflated, synonyms limited to `.txt`/`.txt.gz`.
    */
  def parseListing(html: String, sub: String): Seq[String] =
    HrefRe
      .findAllMatchIn(html)
      .map(_.group(1))
      .filterNot(_.contains("/"))
      .filterNot(_.startsWith("?"))
      .filterNot(_.contains("Conflated"))
      .filter(name => sub != "synonyms" || name.endsWith(".txt.gz") || name.endsWith(".txt"))
      .toSeq
      .distinct
}
