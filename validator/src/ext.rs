use cpython::Python;

pub(crate) trait ResultExt<T> {
    fn expect_pyerr(self, msg: &str) -> T;
}

impl<T> ResultExt<T> for Result<T, cpython::PyErr> {
    #[track_caller]
    fn expect_pyerr(self, msg: &str) -> T {
        match self {
            Ok(value) => value,
            Err(e) => {
                let py = Python::acquire_gil();
                e.print(py.python());
                panic!("{}", msg);
            }
        }
    }
}
