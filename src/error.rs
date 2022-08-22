#[derive(Debug)]
pub enum Error {
    IoError(std::io::Error),
    Utf8Error(std::str::Utf8Error),
    FromUtf8Error(std::string::FromUtf8Error),
    PrometheusError(Box<prometheus::Error>),
    WarpError(warp::Error),
    LapinError(lapin::Error),
    JoinError(tokio::task::JoinError),
    SqlxError(sqlx::Error),
    VarError(std::env::VarError),
    // NoneError(std::option::NoneError),
    SerdeJsonError(serde_json::Error),
    SetLoggerError(tracing::log::SetLoggerError),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::IoError(e)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Error {
        Error::Utf8Error(e)
    }
}

impl From<prometheus::Error> for Error {
    fn from(e: prometheus::Error) -> Error {
        Error::PrometheusError(Box::new(e))
    }
}

impl From<warp::Error> for Error {
    fn from(e: warp::Error) -> Error {
        Error::WarpError(e)
    }
}

impl From<lapin::Error> for Error {
    fn from(e: lapin::Error) -> Error {
        Error::LapinError(e)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(e: tokio::task::JoinError) -> Error {
        Error::JoinError(e)
    }
}

impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Error {
        Error::SqlxError(e)
    }
}

impl From<std::env::VarError> for Error {
    fn from(e: std::env::VarError) -> Error {
        Error::VarError(e)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(e: std::string::FromUtf8Error) -> Error {
        Error::FromUtf8Error(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::SerdeJsonError(e)
    }
}

impl From<tracing::log::SetLoggerError> for Error {
    fn from(e: tracing::log::SetLoggerError) -> Error {
        Error::SetLoggerError(e)
    }
}
