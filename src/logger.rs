//! Structured event logger: file-only, terminal, or null (benchmarks).

use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

enum LogMode {
    FileOnly(Mutex<BufWriter<File>>),
    Terminal(Mutex<BufWriter<File>>),
    Null,
}

pub struct Logger { mode: LogMode }

impl Logger {
    pub fn new() -> Arc<Self> {
        let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
        create_dir_all("logs").unwrap();
        let f = File::create(format!("logs/run_{}.log", ts)).unwrap();
        Arc::new(Self { mode: LogMode::FileOnly(Mutex::new(BufWriter::new(f))) })
    }

    pub fn with_terminal() -> Arc<Self> {
        let ts = chrono::Local::now().format("%Y%m%d_%H%M%S");
        create_dir_all("logs").unwrap();
        let f = File::create(format!("logs/run_{}.log", ts)).unwrap();
        Arc::new(Self { mode: LogMode::Terminal(Mutex::new(BufWriter::new(f))) })
    }

    pub fn null() -> Arc<Self> {
        Arc::new(Self { mode: LogMode::Null })
    }

    pub fn log(&self, msg: &str) {
        match &self.mode {
            LogMode::FileOnly(file) => {
                writeln!(file.lock().unwrap(), "{}", msg).ok();
            }
            LogMode::Terminal(file) => {
                println!("{}", msg);
                writeln!(file.lock().unwrap(), "{}", msg).ok();
            }
            LogMode::Null => {}
        }
    }

    pub fn flush(&self) {
        match &self.mode {
            LogMode::FileOnly(f) | LogMode::Terminal(f) => { f.lock().unwrap().flush().ok(); }
            LogMode::Null => {}
        }
    }
}

impl Drop for Logger {
    fn drop(&mut self) { self.flush(); }
}
