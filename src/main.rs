// get history of bitcoin price on binance
// https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=1000

const API_URL: &str = "https://api.binance.com/api/v3/klines";
const LIMIT: &str = "1000";
const PATH: &str = "data/";
const LAST_FILE: &str = "data/state.json";


use reqwest::blocking::get;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Deserialize, Serialize)]
pub struct Candle {
    pub open_time: i64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub open: f64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub high: f64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub low: f64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub close: f64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub volume: f64,

    pub close_time: i64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub quote_asset_volume: f64,

    pub number_of_trades: i64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub taker_buy_base_asset_volume: f64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub taker_buy_quote_asset_volume: f64,

    #[serde(deserialize_with = "de_float_from_str")]
    pub ignore: f64,
}

pub fn de_float_from_str<'a, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'a>,
{
    let str_val = String::deserialize(deserializer)?;
    str_val.parse::<f64>().map_err(de::Error::custom)
}

pub fn ser_float_to_str<S>(val: &f64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
{
    serializer.serialize_str(&val.to_string())
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ToSave {
    pub symbol: String,
    pub interval: String,
    pub start_time: i64,
}

impl ToSave {
    pub fn get_symbol(&self) -> String {
        self.symbol.clone()
    }
    pub fn get_interval(&self) -> String {
        self.interval.clone()
    }
    pub fn get_start_time(&self) -> i64 {
        self.start_time
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Backup {
    pub symbols: Vec<ToSave>,
}

impl Backup {
    pub fn load() -> Self {
        while let Ok(file) = std::fs::read_to_string(LAST_FILE) {
            if let Ok(backup) = serde_json::from_str(&file) {
                return backup;
            }
        }
        Self { symbols: vec![] }
    }

    pub fn save(&self) {
        if let Ok(json) = serde_json::to_string(&self) {
            if let Ok(_) = std::fs::write(LAST_FILE, json) {}
        }
    }

    pub fn create_folder(&self) {
        for symbol in &self.symbols {
            let path = format!("{}{}-{}", PATH, symbol.symbol, symbol.interval);
            std::fs::create_dir_all(path).unwrap();
        }
    }
}

impl Candle {
    pub fn format(api_url: &str, symbol: &str, interval: &str, start_time: i64, limit: &str) -> String {
        format!("{}?symbol={}&interval={}&startTime={}&limit={}", api_url, symbol, interval, start_time, limit)
    }
}

pub type Candles = Vec<Candle>;

fn save_json(candles: &Candles, symbol: &str, interval: &str) {
    // save in json
    let mut wtr = std::fs::File::create(
        format!("{}{}-{}/{}_{}-{}.json",
                PATH,
                symbol,
                interval,
                symbol,
                candles.first().unwrap().open_time,
                candles.last().unwrap().close_time)).unwrap();
    serde_json::to_writer(&mut wtr, &candles).unwrap();
}

fn main() {

    // init variables
    let backup = Backup::load();
    backup.create_folder();
    let backup = backup.clone();

    // init threads
    let mut threads = vec![];
    for to_save in backup.symbols.iter() {
        let to_save = to_save.clone();
        threads.push(std::thread::spawn(move || {

            let mut start_time = to_save.start_time;
            loop {
                let url = Candle::format(API_URL, &to_save.symbol, &to_save.interval, start_time, LIMIT);
                match get(&url) {
                    Ok(resp) => {
                        match resp.bytes() {
                            Ok(body) => {
                                match serde_json::from_slice::<Candles>(&body) {
                                    Ok(candles) => {
                                        if candles.is_empty() {
                                            let mut backup = Backup::load();
                                            for symbol in &mut backup.symbols {
                                                if symbol.symbol == to_save.symbol && symbol.interval == to_save.interval {
                                                    symbol.start_time = start_time;
                                                }
                                            }
                                            backup.save();
                                            break;
                                        }
                                        println!("{}_{}_{}_{}", to_save.symbol, to_save.interval, candles.first().unwrap().open_time, candles.last().unwrap().close_time);
                                        save_json(&candles, &to_save.symbol, &to_save.interval);
                                        start_time = candles.last().unwrap().close_time + 1;
                                    }
                                    Err(err) => {
                                        println!("error: {}", err);
                                    }
                                }
                            }
                            Err(err) => {
                                println!("error {}", err)
                            }
                        }
                    }
                    Err(err) => {
                        println!("error {}", err)
                    }
                }
            }
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
}



// test section 

mod test_api_binance {
    use super::*;

    #[test]
    fn check_connection() {
        let url = Candle::format(API_URL, "BTCUSDT", "1m", 0, LIMIT);
        let resp = get(&url).unwrap();
        assert_eq!(resp.status(), 200);
    }

    

    // #[test]
    // fn check_candle() {
    //     let url = Candle::format(API_URL, "BTCUSDT", "1m", 0, LIMIT);
    //     let resp = get(&url).unwrap();
    //     let body = resp.bytes().unwrap();
    //     let candles = serde_json::from_slice::<Candles>(&body).unwrap();
    //     assert_eq!(candles.len(), 500);
    // }
}

// send barem
// borie alex
// alves adam
