#[cfg(feature = "aws-lc")]
#[test]
fn initializes_aws_lc_ssl() {
    unsafe {
        let context = aws_lc_sys::SSL_CTX_new(aws_lc_sys::TLS_method());
        assert!(!context.is_null());
        aws_lc_sys::SSL_CTX_free(context);
    }
}
