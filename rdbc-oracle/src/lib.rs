//! Oracle RDBC Driver
//!
//! This crate implements an RDBC Driver for the `oracle` crate.
//!
//! The RDBC (Rust DataBase Connectivity) API is loosely based on the ODBC and JDBC standards.
//!
//! ```rust,no_run
//! use rdbc::*;
//! use rdbc_oracle::OracleDriver;
//! use rdbc_oracle::rewrite;
//!
//! let driver = OracleDriver::new("","");
//! let mut conn = driver.connect("//localhost:1521/xe").unwrap();
//! let sql = "SELECT name FROM ZZZZZ WHERE code = ?";
//! let sql = rewrite(sql,params).unwrap();
//! let mut stmt = conn.prepare(sql.as_str()).unwrap();
//! let mut rs = stmt.execute_query(&[Value::Int32(123)]).unwrap();
//! while rs.next() {
//!   println!("{:?}", rs.get_string(0));
//! }
//! ```

use oracle::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::tokenizer::{Token, Tokenizer, Word};
use oracle::sql_type::*;
use chrono::prelude::*;
use chrono::naive::{NaiveTime,NaiveDateTime,NaiveDate};


/// Convert a MySQL error into an RDBC error
fn to_rdbc_err(e: oracle::Error) -> rdbc::Error {
    rdbc::Error::General(e.to_string())
}

fn value_to_rdbc_err(e: oracle::Error) -> rdbc::Error {
    rdbc::Error::General(e.to_string())
}

pub struct OracleDriver {
    username: String,
    password: String,
}

impl OracleDriver {
    pub fn new(username:&str,password: &str) -> Self {
        OracleDriver {
            username: username.to_owned(),
            password:password.to_owned(),
        }
    }
}

impl rdbc::Driver for OracleDriver {
    fn connect(&self, url: &str) -> rdbc::Result<Box<dyn rdbc::Connection>> {
        let mut conn = oracle::Connection::connect(&self.username,&self.password,url).unwrap();
        &conn.set_autocommit(true);
        Ok(Box::new(OracleConnection { conn }))
    }
}



pub struct OracleConnection {
    conn: oracle::Connection,
}

impl rdbc::Connection for OracleConnection {
    fn create(&mut self, sql: &str) -> rdbc::Result<Box<dyn rdbc::Statement + '_>> {
        Ok(Box::new(OracleStatement {
            conn:&mut self.conn,
            sql: sql.to_owned(),
        }))
    }

    fn prepare(&mut self, sql: &str) -> rdbc::Result<Box<dyn rdbc::Statement+ '_>> {
        let stmt = self.conn.prepare(&sql,&Vec::new()).map_err(to_rdbc_err)?;
        // dbg!(&stmt);
        Ok(Box::new(OraclePreparedStatement { stmt }))
    }

    fn start_transaction(&mut self) -> rdbc::Result<()> {
        self.conn.set_autocommit(false);
        Ok(())
    }

    fn commit(&mut self) -> rdbc::Result<()> {
        self.conn.commit().map_err(to_rdbc_err)?;
        self.conn.set_autocommit(true);
        Ok(())
    }

    fn rollback(&mut self) -> rdbc::Result<()>{
        self.conn.rollback().map_err(to_rdbc_err)?;
        self.conn.set_autocommit(true);
        Ok(())
    }
}

struct OracleStatement<'a> {
    conn: &'a mut oracle::Connection,
    sql: String,
}

impl<'a> rdbc::Statement for OracleStatement<'a> {
    fn execute_query(&mut self, params: &[rdbc::Value]) -> rdbc::Result<Box<dyn rdbc::ResultSet + '_>> {
        let sql = rewrite(&self.sql, params)?;
        // dbg!(&sql);
        let mut ora_params:Vec<&dyn ToSql> = vec![];
        for v in params {
            match v {
                rdbc::Value::Int32(n) => &ora_params.push(n),
                rdbc::Value::UInt32(n) => &ora_params.push(n),
                rdbc::Value::String(s) => &ora_params.push(s),
                rdbc::Value::DateTime(dt) => &ora_params.push(dt),
                rdbc::Value::Date(d) => &ora_params.push(d),
            };
        }
        // rdbc::Result::Err(rdbc::Error::General("E:".to_owned()))
        let result = self.conn.query(&sql,&ora_params).map_err(to_rdbc_err)?;
        Ok(Box::new(OracleResultSet {result,row:None}))
    }

    fn execute_update(&mut self, params: &[rdbc::Value]) -> rdbc::Result<u64> {
        let sql = rewrite(&self.sql, params)?;
        let mut ora_params:Vec<&dyn ToSql> = vec![];
        for v in params {
            match v {
                rdbc::Value::Int32(n) => &ora_params.push(n),
                rdbc::Value::UInt32(n) => &ora_params.push(n),
                rdbc::Value::String(s) => &ora_params.push(s),
                rdbc::Value::DateTime(dt) => &ora_params.push(dt),
                rdbc::Value::Date(d) => &ora_params.push(d),
            };
        }
        let result = self.conn.execute(&sql,&ora_params).map_err(to_rdbc_err)?;
        Ok(result.row_count().unwrap())
    }
}

struct OraclePreparedStatement<'a> {
    stmt: oracle::Statement<'a>,
}

impl<'a> rdbc::Statement for OraclePreparedStatement<'a> {
    fn execute_query(&mut self, params: &[rdbc::Value]) -> rdbc::Result<Box<dyn rdbc::ResultSet+ '_>> {
        let mut ora_params:Vec<&dyn ToSql> = vec![];
        for v in params {
            match v {
                rdbc::Value::Int32(n) => &ora_params.push(n),
                rdbc::Value::UInt32(n) => &ora_params.push(n),
                rdbc::Value::String(s) => &ora_params.push(s),
                rdbc::Value::DateTime(dt) => &ora_params.push(dt),
                rdbc::Value::Date(d) => &ora_params.push(d),
            };
        }
        // dbg!(&params);
        // rdbc::Result::Err(rdbc::Error::General("E:".to_owned()))
        let result = self.stmt.query(&ora_params).map_err(to_rdbc_err)?;
        Ok(Box::new(OracleResultSet {result,row:None}))
    }
    fn execute_update(&mut self, params: &[rdbc::Value]) -> rdbc::Result<u64> {
        let mut ora_params:Vec<&dyn ToSql> = vec![];
        for v in params {
            match v {
                rdbc::Value::Int32(n) => &ora_params.push(n),
                rdbc::Value::UInt32(n) => &ora_params.push(n),
                rdbc::Value::String(s) => &ora_params.push(s),
                rdbc::Value::DateTime(dt) => &ora_params.push(dt),
                rdbc::Value::Date(d) => &ora_params.push(d),
            };
        }
        let _result = self.stmt.execute(&ora_params).map_err(to_rdbc_err)?;

        Ok(self.stmt.row_count().unwrap())
    }
}

pub struct OracleResultSet<'a> {
    result: oracle::ResultSet<'a,oracle::Row>,
    row: Option<oracle::Result<oracle::Row>>,
}


macro_rules! impl_result_set_fns {
    ($($fn: ident -> $ty: ty),*) => {
        $(
            fn $fn(&self, i: u64) -> rdbc::Result<Option<$ty>> {
                match &self.row {
                    Some(Ok(row)) => {
                           let o_val:oracle::Result<$ty> = row.get(i as usize);
                           o_val.map_or(Ok(None),|v| Ok(Some(v)))
                        },
                    _ => Ok(None),
                }
            }
        )*
    }
}

impl<'a> rdbc::ResultSet for OracleResultSet<'a> {
    fn meta_data(&self) -> rdbc::Result<Box<dyn rdbc::ResultSetMetaData>> {
        let meta: Vec<rdbc::Column> = self
            .result
            .column_info()
            .iter()
            .map(|info| rdbc::Column::new(&info.name(), to_rdbc_type(&info.oracle_type())))
            .collect();
        Ok(Box::new(meta))
    }

    fn next(&mut self) -> bool {
        self.row = self.result.next();
        // let val: String = self.row.get(0).unwrap();
        self.row.is_some()
    }

    impl_result_set_fns! {
        get_i8 -> i8,
        get_i16 -> i16,
        get_i32 -> i32,
        get_i64 -> i64,
        get_f32 -> f32,
        get_f64 -> f64,
        get_string -> String,
        get_date_time -> NaiveDateTime,
        get_date -> NaiveDate,
        get_bytes -> Vec<u8>
    }
}

fn to_rdbc_type(t: &OracleType) -> rdbc::DataType {
    match t {
        OracleType::BinaryFloat | OracleType::Float(_) => rdbc::DataType::Decimal,
        OracleType::BinaryDouble => rdbc::DataType::Decimal,
        OracleType::Rowid | OracleType::Long | OracleType::LongRaw => rdbc::DataType::Integer,
        OracleType::Number(_,_) => rdbc::DataType::Decimal,
        OracleType::Raw(_) => rdbc::DataType::Byte,
        OracleType::Timestamp(_)|OracleType::TimestampTZ(_)|OracleType::TimestampLTZ(_) => rdbc::DataType::Datetime,
        _=> rdbc::DataType::Char
    }
}
/*
fn to_rdbc_type(t: &OracleType) -> rdbc::DataType {
    match t {
        OracleType::BinaryFloat => rdbc::DataType::Float,
        OracleType::BinaryDouble => rdbc::DataType::Double,
        OracleType::Rowid => rdbc::DataType::Integer,
        OracleType::Long => rdbc::DataType::Integer,
        OracleType::LongRaw => rdbc::DataType::Integer,
        OracleType::Long => rdbc::DataType::Decimal,
        ColumnType::MYSQL_TYPE_NEWDECIMAL => rdbc::DataType::Decimal,
        ColumnType::MYSQL_TYPE_STRING => rdbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_VAR_STRING => rdbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_VARCHAR => rdbc::DataType::Utf8,
        ColumnType::MYSQL_TYPE_TINY_BLOB => rdbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB => rdbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_LONG_BLOB => rdbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_BLOB => rdbc::DataType::Binary,
        ColumnType::MYSQL_TYPE_BIT => rdbc::DataType::Bool,
        ColumnType::MYSQL_TYPE_DATE => rdbc::DataType::Date,
        ColumnType::MYSQL_TYPE_TIME => rdbc::DataType::Time,
        ColumnType::MYSQL_TYPE_TIMESTAMP => rdbc::DataType::Datetime, // TODO: Data type for timestamps in UTC?
        ColumnType::MYSQL_TYPE_DATETIME => rdbc::DataType::Datetime,
        mysql_datatype => todo!("Datatype not currently supported: {:?}", mysql_datatype),
    }
}

 */



pub fn rewrite(sql: &str, params: &[rdbc::Value]) -> rdbc::Result<String> {
    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    tokenizer
        .tokenize()
        .and_then(|tokens| {
            let mut i = 0;

            let tokens: Vec<Token> = tokens
                .iter()
                .map(|t| match t {
                    Token::Char(c) if *c == '?' => {
                        let param = &params[i];
                        i += 1;
                        Token::Word(Word {
                            value: format!(":{}",i),
                            quote_style: None,
                            keyword: sqlparser::dialect::keywords::Keyword::NONE,
                        })
                    }
                    _ => t.clone(),
                })
                .collect();

            let sql = tokens
                .iter()
                .map(|t| format!("{}", t))
                .collect::<Vec<String>>()
                .join("");

            Ok(sql)
        })
        .map_err(|e| rdbc::Error::General(format!("{:?}", e)))
}


#[cfg(test)]
mod tests {
    use oracle::{Connection, Error};
    use oracle::sql_type::{FromSql, ToSql};
    use chrono::{Date, Local, DateTime, Utc, NaiveDateTime};
    use crate::value_to_rdbc_err;
    use crate::to_rdbc_err;
    use crate::OracleDriver;
    use crate::rewrite;
    use rdbc::*;

    #[test]
    fn it_b_update() {
        let conn = Connection::connect("seadun_bm", "seadun_bm_123", "//192.168.2.214:1521/seadunbm").unwrap();
        let result = conn.execute("update ZZZZZ set STATUS = :1 where CODE = :2",&[&6_i64,&"26"]).map_err(to_rdbc_err).unwrap();
        let ret = result.row_count().unwrap();
        println!("ret:{}",ret);

        if !conn.autocommit() {
            conn.commit().unwrap();
            println!("commit:{}",ret);
        }
        conn.close().unwrap();

    }

    #[test]
    fn it_a_works() {

// Connect to a database.
        let conn = Connection::connect("seadun_bm", "seadun_bm_123", "//192.168.2.214:1521/seadunbm").unwrap();



        let sql = "select * from ZZZZZ where CODE = :1 AND STATUS = :2";

        let mut params:Vec<&dyn ToSql> = vec![];
        params.push(&"26");
        params.push(&6_i64);

// Select a table with a bind variable.
        println!("---------------|---------------|---------------|");
        let rows = conn.query(sql, &params).unwrap();
        /*
        for info in rows.column_info(){
            println!("col:{}={:?}",&info.name(),&info.oracle_type());
        }
         */
        println!("---------------|---------------|--------------|");
        for row_result in rows {
            let row = row_result.unwrap();
            /*
            let ename: oracle::Result<NaiveDateTime> = NaiveDateTime::from_sql(&row.sql_values()[4]);
            if let Ok(e) = ename {
                println!("Name:{:?}",e);
            }
             */
            // get a column value by position (0-based)
            //rdbc::Result<Option<$ty>>
            /*
            let ename:oracle::Result<Vec<u8>> =
            row.get(3);

            let rname:rdbc::Result<Option<Vec<u8>>> = ename.map(|v| Some(v)).map_err(value_to_rdbc_err);
            // .map_err(value_to_rdbc_err);
            println!("Name1:{:?}",String::from_utf8( rname.expect("5555").unwrap()));
             */

            let ename:oracle::Result<Vec<u8>> = row.get(3);

            let rname:rdbc::Result<Option<Vec<u8>>> = ename.map_or(Ok(None),|v| Ok(Some(v)));
            println!("rname1:{:?}",rname);
            let status:oracle::Result<i64> = row.get(4);

            // let rstatus:rdbc::Result<Option<i64>> = ename.map_or(Ok(None),|s| Ok(Some(s)));
            // .map_err(value_to_rdbc_err);
            println!("Status1:{:?}",status);
        }
        conn.close().unwrap();

// Another way to fetch rows.
// The rows iterator returns Result<(String, i32, Option<i32>)>.
        /*
        println!("---------------|---------------|---------------|");
        let rows = conn.query_as::<(String, i32, Option<i32>)>(sql, &[&10])?;
        for row_result in rows {
            let (ename, sal, comm) = row_result?;
            println!(" {:14}| {:>10}    | {:>10}    |",
                     ename,
                     sal,
                     comm.map_or("".to_string(), |v| v.to_string()));
        }

         */
    }

    #[test]
    fn it_rdbc_query() {
        let driver = OracleDriver::new("seadun_bm","seadun_bm_123");
        let mut conn = driver.connect("//192.168.2.214:1521/seadunbm").unwrap();
        let sql = "SELECT name FROM ZZZZZ WHERE code = ?";
        let params = &vec![Value::String("26".to_owned())];

        let sql = rewrite(sql,params).unwrap();

        let mut stmt = conn.prepare(sql.as_str()).unwrap();

        let mut rs = stmt.execute_query(&params).unwrap();
        while rs.next() {
          println!("zzzz_name{}", rs.get_string(0).unwrap().unwrap());
        }
    }
}
