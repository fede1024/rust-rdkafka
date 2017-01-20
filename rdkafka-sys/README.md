# rdkafka-sys

Sys wrapper around [librdkafka](https://github.com/edenhill/librdkafka).

To regenerate the bindings:

```
bindgen --builtins --convert-macros librdkafka/src/rdkafka.h > src/bindings.rs
```
