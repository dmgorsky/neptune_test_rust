use std::sync::{Arc, Mutex, RwLock};

use actix_web::{App, get, HttpResponse, HttpServer, post, Responder, web};
use actix_web::middleware::Logger;
use actix_web::web::Data;
use env_logger::Env;
use serde::Deserialize;

use crate::trading_service::{PushPrices, TradingServices};

mod trading_service;

#[derive(Clone)]
struct AppState {
    trading_services: Arc<RwLock<TradingServices>>,
}
impl AppState {
    pub fn new() -> Self {
        Self {
            trading_services: Arc::new(RwLock::new(TradingServices::new())),
        }
    }
}

#[derive(Deserialize)]
struct GetStatsParams {
    symbol: char,
    k: u32,
}

///get_stats handler, using trading_services as shared context
#[get("/stats/")]
async fn get_stats(
    params: web::Query<GetStatsParams>,
    data: web::Data<Mutex<AppState>>,
) -> impl Responder {
    let context = data.lock().unwrap();
    let trading_service = context.trading_services.read().unwrap();
    let stats = trading_service.get_stats(params.symbol, params.k);
    match stats {
        Some(statistics) => HttpResponse::Ok().json(statistics),
        None => HttpResponse::NotFound().body("Not found"),
    }
}

#[derive(Deserialize)]
struct AddBatchParams {
    symbol: char,
    values: Vec<f32>,
}

///add_batch handler, using trading_services as shared context
#[post("/add_batch/")]
async fn add_batch(
    params: web::Json<AddBatchParams>,
    data: web::Data<Mutex<AppState>>,
) -> impl Responder {
    let context = data.lock().unwrap();
    let trading_service = context.trading_services.write().unwrap();
    let _added_prices = trading_service.push_prices(
        params.symbol,
        PushPrices {
            prices: params.values.clone(),
        },
    ); //todo check result
    HttpResponse::Ok()
}

#[get("/")]
async fn root() -> impl Responder {
    HttpResponse::Ok().body("POST /add_batch/ , GET /stats/?symbol={symbol}&k={k}")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    //shared context (AppState) holds trading_services
    let app_context = Data::new(Mutex::new(AppState::new()));
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(Data::clone(&app_context))
            .service(root)
            .service(get_stats)
            .service(add_batch)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
