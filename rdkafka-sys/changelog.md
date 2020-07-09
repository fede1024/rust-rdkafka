# Changelog

<a name="2.0.0+1.4.2"></a>
## v2.0.0+1.4.2 (Unreleased)

* Start separate changelog for rdkafka-sys.

* Upgrade to librdkafka v1.4.2.

* Correct several references to `usize` in the generated bindings to `size_t`.

* Introduce the `sasl-plain` option to compile the vendored libsasl2 with
  support for the PLAIN authentication method. Also introduce the
  `sasl-vendored` option to compile the vendored libsasl2 with support for only
  the default authentication methods.
