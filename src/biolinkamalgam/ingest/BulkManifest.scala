package biolinkamalgam.ingest

import org.yaml.snakeyaml.Yaml

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.jdk.CollectionConverters._

/** Checked release inventory and projection receipts. Does not infer completeness from existing filenames. Download
  * orchestration owns one compressed shard at a time; source deletion is allowed only after a receipt.
  */
object BulkManifest {
  case class Entry(name: String, url: String, bytes: Long, md5: String, role: String) {
    require(name.matches("[A-Za-z0-9][A-Za-z0-9._-]*") && !name.contains(".."), s"unsafe bulk filename: $name")
    val uri = new java.net.URI(url)
    require(
      uri.getScheme == "https" && uri.getHost != null && uri.getUserInfo == null && uri.getFragment == null,
      s"bulk source must be a credential-free HTTPS URL: $name"
    )
    require(
      !uri.getPath.split('/').exists(p => p == "latest" || p.startsWith("latest-") || p == "latest_release"),
      s"bulk URL must pin a release: $name"
    )
    require(bytes > 0, s"invalid byte count: $name")
    require(md5.matches("[a-f0-9]{32}"), s"invalid MD5: $name")
    require(role.matches("[a-z][a-z0-9_-]*"), s"invalid bulk role: $role")
  }

  case class Manifest(release: String, scope: String, files: Seq[Entry]) {
    require(release.matches("[A-Za-z0-9][A-Za-z0-9._-]*"), "invalid release id")
    require(scope.trim.nonEmpty, "bulk scope is required")
    require(files.nonEmpty && files.map(_.name).distinct.size == files.size, "empty or duplicate bulk inventory")
    def fingerprint: String = {
      val canonical =
        Seq(release, scope) ++ files.sortBy(_.name).flatMap(e => Seq(e.name, e.url, e.bytes.toString, e.md5, e.role))
      // Length prefixes avoid ambiguous concatenation and schema-independent YAML ordering differences.
      val encoded = canonical.map(s => s"${s.getBytes(StandardCharsets.UTF_8).length}:$s").mkString
      MessageDigest.getInstance("SHA-256").digest(encoded.getBytes(StandardCharsets.UTF_8)).map(b => f"$b%02x").mkString
    }
  }

  private def mapping(value: Any, allowed: Set[String]): Map[String, Any] = {
    require(value.isInstanceOf[java.util.Map[_, _]], "bulk config must contain mappings")
    val fields = value.asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    require(fields.keySet == allowed, s"bulk config keys must be ${allowed.toSeq.sorted.mkString(",")}")
    fields
  }

  def parse(text: String): Manifest = {
    val loader = new org.yaml.snakeyaml.LoaderOptions()
    loader.setAllowDuplicateKeys(false)
    val yaml = new Yaml(new org.yaml.snakeyaml.constructor.SafeConstructor(loader))
    val root = mapping(yaml.load[Any](text), Set("release", "scope", "files"))
    def string(fields: Map[String, Any], key: String): String = {
      require(fields(key).isInstanceOf[String], s"bulk $key must be a string")
      fields(key).asInstanceOf[String]
    }
    require(root("files").isInstanceOf[java.util.List[_]], "bulk files must be a list")
    val entries = root("files").asInstanceOf[java.util.List[Any]].asScala.toSeq.map { raw =>
      val e = mapping(raw, Set("name", "url", "bytes", "md5", "role"))
      require(
        e("bytes").isInstanceOf[java.lang.Integer] || e("bytes").isInstanceOf[java.lang.Long],
        "bulk bytes must be an integer"
      )
      Entry(
        string(e, "name"),
        string(e, "url"),
        e("bytes").asInstanceOf[Number].longValue(),
        string(e, "md5"),
        string(e, "role")
      )
    }
    Manifest(string(root, "release"), string(root, "scope"), entries)
  }

  def verify(file: os.Path, entry: Entry): Unit = {
    require(os.isFile(file) && os.size(file) == entry.bytes, s"bulk file missing or truncated: ${entry.name}")
    val md5 = MessageDigest.getInstance("MD5")
    val stream = java.nio.file.Files.newInputStream(file.toNIO)
    val buffer = new Array[Byte](1024 * 1024)
    try {
      var read = stream.read(buffer)
      while (read >= 0) {
        if (read > 0) md5.update(buffer, 0, read)
        read = stream.read(buffer)
      }
    } finally stream.close()
    val actual = md5.digest().map(b => f"$b%02x").mkString
    require(actual == entry.md5, s"bulk checksum mismatch: ${entry.name}")
  }

  def requireSpace(freeBytes: Long, entry: Entry, reserveBytes: Long, outputAllowanceBytes: Long): Unit = {
    require(freeBytes >= 0 && reserveBytes >= 0 && outputAllowanceBytes >= 0, "negative bulk disk budget")
    require(
      BigInt(freeBytes) >= BigInt(entry.bytes) + reserveBytes + outputAllowanceBytes,
      s"insufficient disk space for ${entry.name} plus reserve and projected output"
    )
  }

  private def parts(output: os.Path): Seq[os.Path] =
    if (!os.isDir(output)) Seq.empty
    else
      os.walk(output)
        .filter(p => os.isFile(p) && p.last.startsWith("part-") && p.last.endsWith(".parquet"))
        .sortBy(_.toString)

  private def receipt(manifest: Manifest, entry: Entry, output: os.Path, rows: Long, projection: String): String = {
    require(
      projection.matches("[a-f0-9]{64}"),
      "projection fingerprint must be SHA-256 of code version and scope config"
    )
    val inventory = parts(output).map(p => s"${p.relativeTo(output)}\t${os.size(p)}").mkString("\n")
    s"bulk-receipt-v1\n${manifest.fingerprint}\n${entry.name}\n${entry.md5}\n$rows\n$projection\n$inventory\n"
  }

  def complete(
      manifest: Manifest,
      entry: Entry,
      input: os.Path,
      output: os.Path,
      rows: Long,
      projection: String
  ): Unit = {
    require(manifest.files.contains(entry), "entry does not belong to manifest")
    require(rows >= 0, "negative projection count")
    verify(input, entry)
    require(os.isFile(output / "_SUCCESS") && parts(output).nonEmpty, "projection has no committed parquet output")
    val target = output / "_bulk_receipt"
    val tmp = output / "_bulk_receipt.tmp"
    os.write.over(tmp, receipt(manifest, entry, output, rows, projection))
    os.move(tmp, target, replaceExisting = true)
  }

  def reusable(manifest: Manifest, entry: Entry, output: os.Path, projection: String): Boolean = {
    val target = output / "_bulk_receipt"
    if (
      !manifest.files.contains(entry) || !os.isFile(target) || !os.isFile(output / "_SUCCESS") || parts(output).isEmpty
    ) false
    else {
      val lines = os.read.lines(target)
      lines
        .lift(4)
        .flatMap(_.toLongOption)
        .exists(n => n >= 0 && os.read(target) == receipt(manifest, entry, output, n, projection))
    }
  }
}
