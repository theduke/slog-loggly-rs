extern crate slog;
extern crate slog_async;
extern crate reqwest;
extern crate serde;
#[macro_use]
extern crate serde_json;

use std::{
    time::Duration,
    cell::RefCell,
};

use slog::{
    Drain,
    Record,
    OwnedKVList,
};
use slog_async::{Async};
use serde_json::{
    Map,
    Value,
};

struct InnerLogglyDrain {
    client: reqwest::Client,
    url: reqwest::Url,

    buffer_size: usize,
    // A buffer of slog Records waiting to be sent.
    // They are already transformed to JSON.
    buffer: RefCell<Vec<Vec<u8>>>,
}

impl InnerLogglyDrain {
    pub fn new(token: String, tag: String, buffer_size: usize) -> Self {
        let url = reqwest::Url::parse(&format!(
            "http://logs-01.loggly.com/bulk/{}/tag/{}", token, tag)).unwrap();

        InnerLogglyDrain{
            client: reqwest::Client::new(),
            url,
            buffer_size,
            buffer: RefCell::new(Vec::new()),
        }
    }

    fn record_to_json(record: &Record) -> Vec<u8> {
        let mut obj: Map<String, Value> = Map::new();
        obj["message"] = json!(record.msg().to_string());
        serde_json::to_vec(&obj).unwrap()
    }

    fn send_payload(&self, payload: Vec<u8>) -> Result<(), reqwest::Error> {
        self.client
            .post(self.url.clone())
            .body(payload)
            .send()?
            .error_for_status()?;
        Ok(())
    }
}

impl Drain for InnerLogglyDrain {
    type Ok = ();
    type Err = slog::Never;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<<Self as Drain>::Ok, <Self as Drain>::Err> {
        let is_flush_msg = record.tag() == "__slog_loggly_flush";

        let mut lock = self.buffer.borrow_mut();
        if is_flush_msg == false {
            if lock.len() < self.buffer_size {
                let json = Self::record_to_json(record);
                lock.push(json);
                return Ok(());
            }
        }
        if is_flush_msg && lock.len() < 1 {
            return Ok(());
        }

        let mut payload = lock
            .drain(0..)
            .fold(Vec::<u8>::new(), |mut payload, record| {
                payload.extend_from_slice(&record);
                payload.push(b'\n');
                payload
            });
        if is_flush_msg == false {
            if payload.len() > 0 {
                payload.push(b'\n');
                payload.extend_from_slice(&Self::record_to_json(record));
            }
        }

        Ok(())
    }

}

pub struct Config {
    pub token: String,
    pub tag: String,
    pub buffer_size: usize,
    pub flush_interval: Duration,
}

pub struct LogglyDrain {
    async: Async,
}

impl LogglyDrain {
    pub fn new(config: Config) -> Self {
        let drain = InnerLogglyDrain::new(config.token, config.tag, config.buffer_size);
        let async = Async::new(drain)
            .thread_name("loggly".into())
            .build();

        // Spawn the flusher thread that sends flush messages.
        {
            /*
            let async = async.clone();
            ::std::thread::Builder::new()
                .name("loggly_flusher".into())
                .spawn(|| {
                });
                */
        }

        LogglyDrain{
            async,
        }
    }
}

pub struct Error(slog_async::AsyncError);

impl Drain for LogglyDrain {
    type Ok = ();
    type Err = Error;

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<<Self as Drain>::Ok, <Self as Drain>::Err> {
        self.async.log(record, values)
            .map_err(|e| Error(e))
    }
}
