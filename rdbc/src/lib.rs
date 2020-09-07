//! The RDBC (Rust DataBase Connectivity) API is loosely based on the ODBC and JDBC standards
//! and provides a database agnostic programming interface for executing queries and fetching
//! results.
//!
//! Reference implementation RDBC Drivers exist for Postgres, MySQL and SQLite.
//!
//! The following example demonstrates how RDBC can be used to run a trivial query against Postgres.
//!
//! ```rust,ignore
//! use rdbc::*;
//! use rdbc_postgres::PostgresDriver;
//!
//! let driver = PostgresDriver::new();
//! let mut conn = driver.connect("postgres://postgres:password@localhost:5433").unwrap();
//! let mut stmt = conn.prepare("SELECT a FROM b WHERE c = ?").unwrap();
//! let mut rs = stmt.execute_query(&[Value::Int32(123)]).unwrap();
//! while rs.next() {
//!   println!("{:?}", rs.get_string(1));
//! }
//! ```

use chrono::prelude::*;
use std::fmt;
/// RDBC Error
#[derive(Debug)]
pub enum Error {
    General(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::General(msg)=>{
                write!(f, "Error:{}",msg)
            }
        }

    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}



#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    UInt32(u32),
    String(String),
    DateTime(NaiveDateTime),
    Date(NaiveDate),
    //TODO add other types
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::Int32(n) => format!("{}", n),
            Value::UInt32(n) => format!("{}", n),
            Value::String(s) => format!("'{}'", s),
            Value::DateTime(s) => format!("'{}'", s.format("%Y-%m-%d %H:%M:%S").to_string()),
            Value::Date(d) => format!("'{}'",d.format("%Y-%m-%d").to_string()),
        }
    }
}

/// RDBC Result type
pub type Result<T> = std::result::Result<T, Error>;

/// Represents database driver that can be shared between threads, and can therefore implement
/// a connection pool
pub trait Driver: Sync + Send {
    /// Create a connection to the database. Note that connections are intended to be used
    /// in a single thread since most database connections are not thread-safe
    fn connect(&self, url: &str) -> Result<Box<dyn Connection>>;
}

/// Represents a connection to a database
pub trait Connection: Sync + Send {
    /// Create a statement for execution
    fn create(&mut self, sql: &str) -> Result<Box<dyn Statement + '_>>;

    /// Create a prepared statement for execution
    fn prepare(&mut self, sql: &str) -> Result<Box<dyn Statement + '_>>;

    fn start_transaction(
        &mut self,
    ) -> Result<()>;

    fn commit(&mut self) -> Result<()>;

    fn rollback(&mut self) -> Result<()>;

}

/// Represents an executable statement
pub trait Statement {
    /// Execute a query that is expected to return a result set, such as a `SELECT` statement
    fn execute_query(&mut self, params: &[Value]) -> Result<Box<dyn ResultSet + '_>>;

    /// Execute a query that is expected to update some rows.
    fn execute_update(&mut self, params: &[Value]) -> Result<u64>;
}

/// Result set from executing a query against a statement
pub trait ResultSet  {
    /// get meta data about this result set
    fn meta_data(&self) -> Result<Box<dyn ResultSetMetaData>>;

    /// Move the cursor to the next available row if one exists and return true if it does
    fn next(&mut self) -> bool;

    fn get_i8(&self, i: u64) -> Result<Option<i8>>;
    fn get_i16(&self, i: u64) -> Result<Option<i16>>;
    fn get_i32(&self, i: u64) -> Result<Option<i32>>;
    fn get_i64(&self, i: u64) -> Result<Option<i64>>;
    fn get_f32(&self, i: u64) -> Result<Option<f32>>;
    fn get_f64(&self, i: u64) -> Result<Option<f64>>;
    fn get_string(&self, i: u64) -> Result<Option<String>>;
    fn get_date_time(&self, i: u64) -> Result<Option<NaiveDateTime>>;
    fn get_date(&self, i: u64) -> Result<Option<NaiveDate>>;
    fn get_bytes(&self, i: u64) -> Result<Option<Vec<u8>>>;
}

/// Meta data for result set
pub trait ResultSetMetaData {
    fn num_columns(&self) -> u64;
    fn column_name(&self, i: u64) -> String;
    fn column_type(&self, i: u64) -> DataType;
}

/// RDBC Data Types
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Byte,
    Char,
    Short,
    Integer,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    Datetime,
    Utf8,
    Binary,
}

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Column {
            name: name.to_owned(),
            data_type,
        }
    }
}

impl ResultSetMetaData for Vec<Column> {
    fn num_columns(&self) -> u64 {
        self.len() as u64
    }

    fn column_name(&self, i: u64) -> String {
        self[i as usize].name.clone()
    }

    fn column_type(&self, i: u64) -> DataType {
        self[i as usize].data_type
    }
}
