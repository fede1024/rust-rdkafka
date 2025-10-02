use std::sync::Once;

static INIT: Once = Once::new();

pub fn init_test_logger() {
    INIT.call_once(|| {
        env_logger::try_init().expect("Failed to initialize env_logger");
    });
}
