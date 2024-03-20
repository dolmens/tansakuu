use std::{io, sync::Arc};

use crate::directory::error::{OpenReadError, OpenWriteError};

#[derive(Debug, Clone, Error)]
pub enum TansakuuError {
    /// IO Error.
    #[error("An IO error occurred: '{0}'")]
    IoError(Arc<io::Error>),
    /// Failed to open a file for read.
    #[error("Failed to open file for read: '{0:?}'")]
    OpenReadError(#[from] OpenReadError),
    /// Failed to open a file for write.
    #[error("Failed to open file for write: '{0:?}'")]
    OpenWriteError(#[from] OpenWriteError),
    /// Invalid argument was passed by the user.
    #[error("An invalid argument was passed: '{0}'")]
    InvalidArgument(String),
    /// System error. (e.g.: We failed spawning a new thread).
    #[error("System error.'{0}'")]
    SystemError(String),
    #[error("Internal error: '{0}'")]
    InternalError(String),
}

impl From<io::Error> for TansakuuError {
    fn from(io_err: io::Error) -> TansakuuError {
        TansakuuError::IoError(Arc::new(io_err))
    }
}
