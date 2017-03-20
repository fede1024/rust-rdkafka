Native\* structs
================

`Native*` structs are thin wrappers around native resources that are often needed when
a native resource is required by other native resources. Examples:
 - `RDKafkaConfig` is requred by `RDKafka` for client creation: `NativeClientConfig` is used.
 - `RDKafkaClient` is requred by many methods to consume and produce data: `NativeClient`
   is used.

Specifically, `Native*` structs are used for:
 - Implementing unsafe traits according to the documentation of the resource being wrapped.
   - Example: `Send` and `Sync` for `NativeClient`
 - Implementing ownership helpers:
   - `ptr()` -> reference
   - `ptr_move()` -> ownership
   - `Drop`

Native structs shouldn't be used for:
 - Exporting methods of the resource being wrapped.
 - Creating the native struct itself.

Each `Native*` struct should be created from the higher level resource. Examples:
 - `ClientConfig` creates `NativeClientConfig`
 - `Client` creates `NativeClient`
