# Changelog

<a name="2.1.1+1.5.3></a>
## v2.1.1+1.5.3

* Upgrade to librdkafka v1.5.3.
* Enforce a minimum zstd-sys version of 1.4.19. This bumps the vendored version
  of libzstd to at least v1.4.8, which avoids a bug in libzstd v1.4.5 that could
  cause decompression failures ([edenhill/librdkafka#2672]).

<a name="2.1.0+1.5.0></a>
## v2.1.0+1.5.0

* Upgrade to librdkafka v1.5.0.

<a name="2.0.0+1.4.2"></a>
## v2.0.0+1.4.2 (2020-07-08)

* Start separate changelog for rdkafka-sys.

* Upgrade to librdkafka v1.4.2.

* Correct several references to `usize` in the generated bindings to `size_t`.

[edenhill/librdkafka#2672]: https://github.com/edenhill/librdkafka/issues/2672
