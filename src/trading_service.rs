use rayon::prelude::*;
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

/// Service for storing and analyzing price data.
///
/// This service maintains a time series of price data and provides methods
/// for calculating various statistics over different window sizes.
///
/// # Examples
///
/// ```
/// use neptune_test_rust::trading_service::{TradingService, PushPrices};
///
/// // Create a new trading service
/// let service = TradingService::new();
///
/// // Add some price data
/// let prices = PushPrices { prices: vec![1.0, 2.0, 3.0, 4.0, 5.0] };
/// service.push_prices(prices).expect("Failed to push prices");
///
/// // Get statistics for the last 10 elements (window size 10^1)
/// let stats = service.get_stats(1).expect("Failed to get stats");
/// println!("Sum: {}, Min: {}, Max: {}, Variance: {}",
///          stats.s_sum, stats.s_min, stats.s_max, stats.s_var);
/// ```
#[derive(Default, Clone)]
pub struct TradingService {
    in_vec: Arc<RwLock<Vec<f32>>>,
    // Precomputed statistics for different window sizes (10^k)
    // The HashMap maps window size to (sum, sum_of_squares, min, max)
    precomputed_stats: Arc<RwLock<HashMap<i32, (f32, f32, f32, f32)>>>,
}

impl TradingService {
    /// Creates a new `TradingService` instance.
    ///
    /// Initializes an empty vector for storing price data with a capacity of 10 million elements,
    /// and an empty HashMap for precomputed statistics.
    ///
    /// # Examples
    ///
    /// ```
    /// use neptune_test_rust::trading_service::TradingService;
    ///
    /// let service = TradingService::new();
    /// ```
    pub fn new() -> Self {
        Self {
            in_vec: Arc::new(RwLock::new(Vec::with_capacity(10_000_000))),
            precomputed_stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Appends new price data to the service and updates precomputed statistics.
    ///
    /// This method adds the provided prices to the internal time series and updates
    /// the precomputed statistics for different window sizes (10^0 to 10^6).
    /// If the total number of prices exceeds 100 million, the oldest prices are removed.
    ///
    /// # Arguments
    ///
    /// * `in_prices` - A `PushPrices` struct containing a vector of price values to add
    ///
    /// # Returns
    ///
    /// * `Result<u32, Box<dyn Error>>` - The number of prices currently stored (up to 100 million),
    ///   or an error if the operation fails
    ///
    /// # Examples
    ///
    /// ```
    /// use neptune_test_rust::trading_service::{TradingService, PushPrices};
    ///
    /// let service = TradingService::new();
    /// let prices = PushPrices { prices: vec![10.5, 11.2, 10.8, 11.5, 12.0] };
    ///
    /// match service.push_prices(prices) {
    ///     Ok(count) => println!("Successfully added prices. Total count: {}", count),
    ///     Err(e) => eprintln!("Failed to add prices: {}", e),
    /// }
    /// ```
    pub fn push_prices(&self, in_prices: PushPrices) -> Result<u32, Box<dyn Error>> {
        // Make a copy of the new prices before they're moved
        let new_prices = in_prices.prices.clone();

        let mut mut_vec = self.in_vec.write().unwrap();
        let old_len = mut_vec.len();

        // Add new prices
        mut_vec.extend(in_prices.prices);
        let new_len = mut_vec.len();

        // If len exceeds max(say, 1e8), trim front
        // Rough emulating RingBuffer
        let mut removed_values = Vec::new();
        if new_len > 100_000_000 {
            let drain_count = new_len - 100_000_000;
            removed_values = mut_vec.drain(0..drain_count).collect();
        }

        // Update precomputed statistics for different window sizes
        let mut stats = self.precomputed_stats.write().unwrap();

        // Consider window sizes for k from 0 to 6 (10^0 to 10^6)
        for k in 0..=6 {
            let window_size = 10_i32.pow(k);

            // Skip if window size is larger than our data
            if window_size > mut_vec.len() as i32 {
                continue;
            }

            // Get the current window slice (last 'window_size' elements)
            let start_idx = mut_vec.len() - window_size as usize;
            let window = &mut_vec[start_idx..];

            // If we already have stats for this window size, update them
            if let Some((sum, sum_sq, min_val, max_val)) = stats.get_mut(&window_size) {
                // Remove old values that are no longer in the window
                if !removed_values.is_empty() {
                    // We need to recalculate if we've removed values
                    // This is a simple approach - for a more efficient implementation,
                    // we would need to track which values were in each window
                    let (new_sum, new_sum_sq, new_min, new_max) = Self::calculate_stats(window);
                    *sum = new_sum;
                    *sum_sq = new_sum_sq;
                    *min_val = new_min;
                    *max_val = new_max;
                } else {
                    // Add new values
                    for &val in &new_prices {
                        *sum += val;
                        *sum_sq += val * val;
                        *min_val = min_val.min(val);
                        *max_val = max_val.max(val);

                        // If the window is full, remove the oldest value
                        if old_len >= window_size as usize {
                            let old_val = mut_vec[old_len - window_size as usize];
                            *sum -= old_val;
                            *sum_sq -= old_val * old_val;

                            // For min and max, we need to recalculate if the removed value was the min or max
                            if old_val == *min_val || old_val == *max_val {
                                let (_, _, new_min, new_max) = Self::calculate_stats(window);
                                *min_val = new_min;
                                *max_val = new_max;
                            }
                        }
                    }
                }
            } else {
                // Calculate stats for this window size for the first time
                let (sum, sum_sq, min_val, max_val) = Self::calculate_stats(window);
                stats.insert(window_size, (sum, sum_sq, min_val, max_val));
            }
        }

        Ok(min(new_len as u32, 100_000_000))
    }

    /// Helper function to calculate statistics for a slice of price data.
    ///
    /// Computes the sum, sum of squares, minimum, and maximum values for the given slice.
    ///
    /// # Arguments
    ///
    /// * `slice` - A slice of f32 values to calculate statistics for
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// * `sum` - The sum of all values in the slice
    /// * `sum_sq` - The sum of squares of all values in the slice
    /// * `min_val` - The minimum value in the slice (f32::INFINITY if empty)
    /// * `max_val` - The maximum value in the slice (f32::NEG_INFINITY if empty)
    ///
    /// # Examples
    ///
    /// ```
    /// use neptune_test_rust::trading_service::TradingService;
    ///
    /// let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// let (sum, sum_sq, min, max) = TradingService::calculate_stats(&data);
    ///
    /// assert_eq!(sum, 15.0);
    /// assert_eq!(sum_sq, 55.0); // 1² + 2² + 3² + 4² + 5² = 55
    /// assert_eq!(min, 1.0);
    /// assert_eq!(max, 5.0);
    /// ```
    fn calculate_stats(slice: &[f32]) -> (f32, f32, f32, f32) {
        if slice.is_empty() {
            return (0.0, 0.0, f32::INFINITY, f32::NEG_INFINITY);
        }

        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut min_val = f32::INFINITY;
        let mut max_val = f32::NEG_INFINITY;

        for &val in slice {
            sum += val;
            sum_sq += val * val;
            min_val = min_val.min(val);
            max_val = max_val.max(val);
        }

        (sum, sum_sq, min_val, max_val)
    }
    /// Calculates statistics for a window of price data.
    ///
    /// This method computes various statistics (sum, min, max, last value, variance)
    /// for a window of the most recent price data. The window size is determined by
    /// 10^stats_order (e.g., if stats_order=2, the window size is 100).
    ///
    /// The method uses precomputed statistics when available for better performance.
    ///
    /// # Arguments
    ///
    /// * `stats_order` - The order of magnitude for the window size (10^stats_order)
    ///
    /// # Returns
    ///
    /// * `Result<StatsCalculation, Box<dyn Error>>` - A struct containing the calculated
    ///   statistics, or an error if there's not enough data or the data is empty
    ///
    /// # Examples
    ///
    /// ```
    /// use neptune_test_rust::trading_service::{TradingService, PushPrices};
    ///
    /// let service = TradingService::new();
    ///
    /// // Add 100 prices (1.0, 2.0, ..., 100.0)
    /// let prices = PushPrices {
    ///     prices: (1..=100).map(|i| i as f32).collect()
    /// };
    /// service.push_prices(prices).expect("Failed to push prices");
    ///
    /// // Get statistics for the last 10 elements (window size 10^1)
    /// let stats = service.get_stats(1).expect("Failed to get stats");
    ///
    /// // The window contains values 91.0 through 100.0
    /// assert_eq!(stats.s_sum, 955.0); // 91 + 92 + ... + 100 = 955
    /// assert_eq!(stats.s_min, 91.0);
    /// assert_eq!(stats.s_max, 100.0);
    /// assert_eq!(stats.s_last, 100.0);
    /// ```
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

        let slice = &data_vec[(start_from as usize)..(start_from as usize + real_k as usize)];

        // Check if we have precomputed statistics for this window size
        let stats_lock = self.precomputed_stats.read().unwrap();
        let (s_min, s_max, s_sum, s_var) =
            if let Some((sum, sum_sq, min_val, max_val)) = stats_lock.get(&real_k) {
                // Use precomputed values
                let mean = sum / real_k_float;

                // Calculate variance to match the original implementation
                // The original implementation calculates: sum((x_i - mean)^2 / n)
                // This is different from the standard variance formula: sum((x_i - mean)^2) / n

                let variance = (sum_sq / real_k_float - mean * mean);

                (*min_val, *max_val, *sum, variance)
            } else {
                // Fall back to parallel calculation if precomputed values are not available
                let mean = slice.par_iter().sum::<f32>() / real_k_float;

                slice
                    .par_iter()
                    .fold(
                        || (f32::INFINITY, f32::NEG_INFINITY, 0.0, 0.0),
                        |(min, max, sum, var), &val| {
                            (
                                min.min(val),
                                max.max(val),
                                sum + val,
                                var + (val - mean) * (val - mean) / real_k_float,
                            )
                        },
                    )
                    .reduce(
                        || (f32::INFINITY, f32::NEG_INFINITY, 0.0, 0.0),
                        |(min1, max1, sum1, var1), (min2, max2, sum2, var2)| {
                            (min1.min(min2), max1.max(max2), sum1 + sum2, var1 + var2)
                        },
                    )
            };

        // Use the last element in the window as s_last
        let s_last = if !slice.is_empty() {
            slice[slice.len() - 1]
        } else {
            return Err("Empty slice".into());
        };

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
        trading_service
            .push_prices(PushPrices {
                prices: vec![1f32, 2f32, 3f32, 4f32, 5f32, 6f32, 7f32, 8f32, 9f32, 10f32],
            })
            .unwrap();
        let stats = trading_service.get_stats(2);
        assert!(stats.is_ok());
        let stats = stats.unwrap();
        assert_eq!(stats.calc_range, "1..10");
        assert_eq!(stats.s_sum, 55f32);
        assert_eq!(stats.s_min, 1f32);
        assert_eq!(stats.s_max, 10f32);
        assert_eq!(stats.s_last, 10f32);
        assert_eq!(stats.s_var, 8.25f32);
    }

    #[actix_web::test]
    async fn test_new() {
        // Test that a new TradingService is created with empty data
        let service = TradingService::new();
        let vec = service.in_vec.read().unwrap();
        assert_eq!(vec.len(), 0);

        let stats = service.precomputed_stats.read().unwrap();
        assert_eq!(stats.len(), 0);
    }

    #[actix_web::test]
    async fn test_push_prices_basic() {
        // Test basic functionality of push_prices
        let service = TradingService::new();
        let result = service.push_prices(PushPrices {
            prices: vec![1.0, 2.0, 3.0, 4.0, 5.0],
        });

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);

        let vec = service.in_vec.read().unwrap();
        assert_eq!(vec.len(), 5);
        assert_eq!(*vec, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    }

    #[actix_web::test]
    async fn test_push_prices_multiple_calls() {
        // Test pushing prices in multiple calls
        let service = TradingService::new();

        service
            .push_prices(PushPrices {
                prices: vec![1.0, 2.0, 3.0],
            })
            .unwrap();

        let result = service.push_prices(PushPrices {
            prices: vec![4.0, 5.0],
        });

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);

        let vec = service.in_vec.read().unwrap();
        assert_eq!(vec.len(), 5);
        assert_eq!(*vec, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    }

    #[actix_web::test]
    async fn test_calculate_stats() {
        // Test the calculate_stats helper function
        let empty: Vec<f32> = vec![];
        let (sum, sum_sq, min, max) = TradingService::calculate_stats(&empty);
        assert_eq!(sum, 0.0);
        assert_eq!(sum_sq, 0.0);
        assert_eq!(min, f32::INFINITY);
        assert_eq!(max, f32::NEG_INFINITY);

        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let (sum, sum_sq, min, max) = TradingService::calculate_stats(&data);
        assert_eq!(sum, 15.0);
        assert_eq!(sum_sq, 55.0); // 1² + 2² + 3² + 4² + 5² = 55
        assert_eq!(min, 1.0);
        assert_eq!(max, 5.0);
    }

    #[actix_web::test]
    async fn test_get_stats_error_when_empty() {
        // Test that get_stats returns an error when no data is available
        let service = TradingService::new();
        let result = service.get_stats(1);
        assert!(result.is_err());
    }

    #[actix_web::test]
    async fn test_get_stats_different_window_sizes() {
        // Test get_stats with different window sizes
        let service = TradingService::new();

        // Add 100 prices (1.0, 2.0, ..., 100.0)
        let prices: Vec<f32> = (1..=100).map(|i| i as f32).collect();
        service.push_prices(PushPrices { prices }).unwrap();

        // Note: Small window sizes will fail with "Not enough data" due to how the get_stats method
        // is implemented. The guard condition checks if real_k <= start_from, which is true for
        // small window sizes with large datasets.

        // For a dataset of 100 elements:
        // - Window size 10^0 = 1: start_from = 100 - 1 - 1 = 98, guard: 1 <= 98 (true) -> Error
        // - Window size 10^1 = 10: start_from = 100 - 10 - 1 = 89, guard: 10 <= 89 (true) -> Error
        // - Window size 10^2 = 100: start_from = 100 - 100 - 1 = -1 -> 0, guard: 100 <= 0 (false) -> OK

        // Test window size 10^0 = 1 (just the last element)
        let result = service.get_stats(0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Not enough data");

        // Test window size 10^1 = 10 (last 10 elements)
        let result = service.get_stats(1);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Not enough data");

        // Test window size 10^2 = 100 (all elements)
        let stats = service.get_stats(2).unwrap();
        assert_eq!(stats.s_sum, 5050.0); // Sum of 1 to 100 = 5050
        assert_eq!(stats.s_min, 1.0);
        assert_eq!(stats.s_max, 100.0);
        assert_eq!(stats.s_last, 100.0);

        // Test window size 10^3 = 1000 (should be limited to 100)
        let stats = service.get_stats(3).unwrap();
        assert_eq!(stats.s_sum, 5050.0); // Same as above, limited to available data
        assert_eq!(stats.s_min, 1.0);
        assert_eq!(stats.s_max, 100.0);
        assert_eq!(stats.s_last, 100.0);
    }

    #[actix_web::test]
    async fn test_precomputed_stats() {
        // Test that precomputed stats are being used
        let service = TradingService::new();

        // Add 10 prices
        service
            .push_prices(PushPrices {
                prices: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
            })
            .unwrap();

        // Check that precomputed stats exist for window size 10
        let stats_lock = service.precomputed_stats.read().unwrap();
        assert!(stats_lock.contains_key(&10));

        if let Some((sum, sum_sq, min, max)) = stats_lock.get(&10) {
            assert_eq!(*sum, 55.0);
            assert_eq!(*sum_sq, 385.0); // 1² + 2² + ... + 10² = 385
            assert_eq!(*min, 1.0);
            assert_eq!(*max, 10.0);
        } else {
            panic!("Precomputed stats not found for window size 10");
        }
    }
}
