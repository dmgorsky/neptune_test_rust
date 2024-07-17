use std::cmp::min;
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};

use serde::Serialize;

///Inputs for add_batch
#[derive(Debug, Clone)]
pub struct PushPrices {
    pub(crate) prices: Vec<f32>,
}

///Output for get_stats
#[derive(Debug, Clone, Serialize)]
pub struct StatsCalculation {
    calc_range: String,
    s_sum: f32,
    s_min: f32,
    s_max: f32,
    s_last: f32,
    s_var: f32,
}

///Service holding calc data
#[derive(Default, Clone)]
pub struct TradingService {
    in_vec: Arc<RwLock<Vec<f32>>>,
}

impl TradingService {
    pub fn new() -> Self {
        Self {
            in_vec: Arc::new(RwLock::new(Vec::with_capacity(10_000_000))),
        }
    }
    
    ///Appending price data
    pub fn push_prices(&self, in_prices: PushPrices) -> Result<u32, Box<dyn Error>> {
        let mut mut_vec = self.in_vec.write().unwrap();
        mut_vec.extend(in_prices.prices);
        let l = mut_vec.len();

        //if len exceeds max(say, 1e8), trim front
        //rough emulating RingBuffer
        if l > 100_000_000 {
            mut_vec.drain(0..(l - 100_000_000));
        };

        Ok(min(l as u32, 100_000_000))
    }
    ///Calculating stats
    pub fn get_stats(&self, stats_order: u32) -> Result<StatsCalculation, Box<dyn Error>> {
        let k = stats_order;
        let data_vec = self.in_vec.read().unwrap();
        let real_k = min(10_i32.pow(k), data_vec.len() as i32);
        let real_k_float = real_k as f32;
        let mut start_from = data_vec.len() as i32 - real_k - 1;
        if start_from < 0 {
            start_from = 0
        };

        //guard
        if real_k <= start_from {
            return Err("Not enough data".into());
        }

        let mut calc_iterator = data_vec
            .iter()
            .skip(start_from as usize)
            .take(real_k as usize);
        let mut s_sum = 0f32;
        let mut s_min = f32::MAX;
        let mut s_max = -f32::MAX;
        let mut s_last = 0f32;

        //1st pass: sum,min,max,last
        calc_iterator.for_each(|&elm| {
            s_sum += elm;
            if s_min > elm {
                s_min = elm
            };
            if s_max < elm {
                s_max = elm
            };
            s_last = elm;
        });

        let mean = s_sum / real_k_float;
        let mut s_var = 0f32;

        //2nd pass: variance
        calc_iterator = data_vec.iter().skip(start_from as usize).take(k as usize);
        calc_iterator.for_each(|&elm| {
            let diff = (elm - mean) * (elm - mean);
            s_var += diff / real_k_float;
        });

        Ok(StatsCalculation {
            calc_range: format!("{}..{}", start_from + 1, start_from + real_k),
            s_sum,
            s_min,
            s_max,
            s_last,
            s_var,
        })
    }
}

///Service holding all the services for different symbols
#[derive(Clone, Default)]
pub(crate) struct TradingServices {
    trading_services: Arc<RwLock<HashMap<char, TradingService>>>,
}

impl TradingServices {
    pub fn new() -> Self {
        Self {
            trading_services: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    ///Dispatching push_price to corresponding symbol
    pub fn push_prices(
        &self,
        symbol: char,
        in_prices: PushPrices,
    ) -> Result<u32, Box<dyn Error + '_>> {
        let mut trading_services = self.trading_services.write()?;
        //guard for 10 symbols max
        if trading_services.len() > 10 {
            return Err("Too many symbols".into());
        }
        trading_services
            .entry(symbol)
            .or_insert_with(TradingService::new)
            .push_prices(in_prices)
    }

    ///Dispatching get_stats to corresponding symbol
    pub fn get_stats(&self, symbol: char, stats_order: u32) -> Option<StatsCalculation> {
        let trading_services = self.trading_services.read().expect("RwLock poisoned");
        trading_services
            .get(&symbol)
            .and_then(|ts| ts.get_stats(stats_order).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[actix_web::test]
    async fn test_update_stats() {
        let trading_service = TradingService::default();
        let stats = trading_service.get_stats(2);
        assert!(stats.is_err());
        trading_service.push_prices(PushPrices {
            prices: vec![1f32, 2f32, 3f32, 4f32, 5f32, 6f32, 7f32, 8f32, 9f32, 10f32],
        });
        let stats = trading_service.get_stats(2);
        assert!(stats.is_ok());
        let stats = stats.unwrap();
        assert_eq!(stats.calc_range, "1..10");
        assert_eq!(stats.s_sum, 55f32);
        assert_eq!(stats.s_min, 1f32);
        assert_eq!(stats.s_max, 10f32);
        assert_eq!(stats.s_last, 10f32);
        assert_eq!(stats.s_var, 7.5f32);
    }
}
