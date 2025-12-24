use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use chrono_tz::America::New_York;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;

const KALSHI_FEE_RATE: f64 = 0.07;
const ARBITRAGE_LOG_FILE: &str = "arbitrage_opportunities.txt";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolymarketEvent {
    markets: Vec<PolymarketMarket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolymarketMarket {
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
    outcomes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClobBook {
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PriceLevel {
    price: String,
    size: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BinancePrice {
    price: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KalshiResponse {
    markets: Vec<KalshiMarket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KalshiMarket {
    subtitle: String,
    yes_bid: Option<u32>,
    yes_ask: Option<u32>,
    no_bid: Option<u32>,
    no_ask: Option<u32>,
}

#[derive(Debug, Clone)]
struct PolymarketData {
    price_to_beat: Option<f64>,
    current_price: Option<f64>,
    prices: HashMap<String, f64>,
    slug: String,
    coin: String,
}

#[derive(Debug, Clone)]
struct KalshiData {
    event_ticker: String,
    current_price: Option<f64>,
    markets: Vec<KalshiMarketData>,
    coin: String,
}

#[derive(Debug, Clone)]
struct KalshiMarketData {
    strike: f64,
    yes_ask: f64,
    no_ask: f64,
}

#[derive(Debug, Clone)]
struct ArbitrageCheck {
    kalshi_strike: f64,
    kalshi_yes: f64,
    kalshi_no: f64,
    check_type: String,
    poly_leg: String,
    kalshi_leg: String,
    poly_cost: f64,
    kalshi_cost: f64,
    kalshi_fee: f64,
    total_cost: f64,
    total_cost_with_fees: f64,
    is_arbitrage: bool,
    margin: f64,
}

#[derive(Debug, Clone)]
struct ArbitrageResult {
    coin: String,
    polymarket: Option<PolymarketData>,
    kalshi: Option<KalshiData>,
    checks: Vec<ArbitrageCheck>,
    opportunities: Vec<ArbitrageCheck>,
    errors: Vec<String>,
}

fn generate_polymarket_slug(target_time: DateTime<chrono_tz::Tz>, coin: &str) -> String {
    let month = target_time.format("%B").to_string().to_lowercase();
    let day = target_time.day();
    let hour = target_time.format("%I").to_string().parse::<u32>().unwrap();
    let am_pm = target_time.format("%p").to_string().to_lowercase();
    let coin_name = if coin == "BTC" { "bitcoin" } else { "ethereum" };

    format!("{}-up-or-down-{}-{}-{}{}-et", coin_name, month, day, hour, am_pm)
}

fn generate_kalshi_slug(target_time: DateTime<chrono_tz::Tz>, coin: &str) -> String {
    let year = target_time.format("%y");
    let month = target_time.format("%b").to_string().to_lowercase();
    let day = target_time.format("%d");
    let hour = target_time.format("%H");
    let ticker = if coin == "BTC" { "kxbtcd" } else { "kxethd" };

    format!("{}-{}{}{}{}", ticker, year, month, day, hour)
}

fn get_current_market_info(coin: &str) -> (String, String, DateTime<Utc>) {
    let now = Utc::now();
    let target_time = now
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();

    let et_time = target_time.with_timezone(&New_York);
    let polymarket_slug = generate_polymarket_slug(et_time, coin);

    let kalshi_target_time = (target_time + Duration::hours(1)).with_timezone(&New_York);
    let kalshi_slug = generate_kalshi_slug(kalshi_target_time, coin);

    (polymarket_slug, kalshi_slug, target_time)
}

async fn get_clob_price(client: &reqwest::Client, token_id: &str) -> anyhow::Result<f64> {
    let url = format!("https://clob.polymarket.com/book?token_id={}", token_id);
    let response = client.get(&url).send().await?;
    let data: ClobBook = response.json().await?;

    let best_ask = data
        .asks
        .iter()
        .map(|a| a.price.parse::<f64>().unwrap_or(1.0))
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap_or(1.0);

    Ok(if best_ask > 0.0 { best_ask } else { 1.0 })
}

async fn get_polymarket_data(
    client: &reqwest::Client,
    slug: &str,
) -> anyhow::Result<HashMap<String, f64>> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);
    let response = client.get(&url).send().await?;
    let data: Vec<PolymarketEvent> = response.json().await?;

    if data.is_empty() {
        return Err(anyhow::anyhow!("Event not found"));
    }

    let event = &data[0];
    let markets = &event.markets;

    if markets.is_empty() {
        return Err(anyhow::anyhow!("Markets not found in event"));
    }

    let market = &markets[0];

    // Parse JSON strings
    let clob_token_ids: Vec<String> = serde_json::from_str(&market.clob_token_ids)?;
    let outcomes: Vec<String> = serde_json::from_str(&market.outcomes)?;

    if clob_token_ids.len() != 2 {
        return Err(anyhow::anyhow!("Unexpected number of tokens"));
    }

    let mut prices = HashMap::new();
    for (outcome, token_id) in outcomes.iter().zip(clob_token_ids.iter()) {
        let price = get_clob_price(client, token_id).await.unwrap_or(0.0);
        prices.insert(outcome.clone(), price);
    }

    Ok(prices)
}

async fn get_binance_current_price(client: &reqwest::Client, coin: &str) -> anyhow::Result<f64> {
    let symbol = if coin == "BTC" { "BTCUSDT" } else { "ETHUSDT" };
    let url = format!("https://api.binance.com/api/v3/ticker/price?symbol={}", symbol);
    let response = client.get(&url).send().await?;
    let data: BinancePrice = response.json().await?;
    Ok(data.price.parse()?)
}

async fn get_binance_open_price(
    client: &reqwest::Client,
    target_time: DateTime<Utc>,
    coin: &str,
) -> anyhow::Result<f64> {
    let symbol = if coin == "BTC" { "BTCUSDT" } else { "ETHUSDT" };
    let timestamp_ms = target_time.timestamp_millis();

    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval=1h&startTime={}&limit=1",
        symbol, timestamp_ms
    );

    let response = client.get(&url).send().await?;
    let data: Vec<serde_json::Value> = response.json().await?;

    if data.is_empty() {
        return Err(anyhow::anyhow!("Candle not found yet"));
    }

    let open_price = data[0][1].as_str().unwrap().parse()?;
    Ok(open_price)
}

async fn fetch_polymarket_data_struct(
    client: &reqwest::Client,
    coin: &str,
) -> anyhow::Result<PolymarketData> {
    let (slug, _, target_time) = get_current_market_info(coin);

    let prices = get_polymarket_data(client, &slug).await?;
    let current_price = get_binance_current_price(client, coin).await.ok();
    let price_to_beat = get_binance_open_price(client, target_time, coin).await.ok();

    Ok(PolymarketData {
        price_to_beat,
        current_price,
        prices,
        slug,
        coin: coin.to_string(),
    })
}

fn parse_strike(subtitle: &str) -> f64 {
    let re = Regex::new(r"\$([\d,]+)").unwrap();
    if let Some(caps) = re.captures(subtitle) {
        if let Some(matched) = caps.get(1) {
            return matched.as_str().replace(",", "").parse().unwrap_or(0.0);
        }
    }
    0.0
}

async fn get_kalshi_markets(
    client: &reqwest::Client,
    event_ticker: &str,
) -> anyhow::Result<Vec<KalshiMarket>> {
    let url = format!(
        "https://api.elections.kalshi.com/trade-api/v2/markets?limit=100&event_ticker={}",
        event_ticker
    );

    let response = client.get(&url).send().await?;
    let data: KalshiResponse = response.json().await?;
    Ok(data.markets)
}

async fn fetch_kalshi_data_struct(
    client: &reqwest::Client,
    coin: &str,
) -> anyhow::Result<KalshiData> {
    let (_, kalshi_slug, _) = get_current_market_info(coin);
    let event_ticker = kalshi_slug.to_uppercase();

    let current_price = get_binance_current_price(client, coin).await.ok();
    let markets = get_kalshi_markets(client, &event_ticker).await?;

    let mut market_data: Vec<KalshiMarketData> = markets
        .iter()
        .filter_map(|m| {
            let strike = parse_strike(&m.subtitle);
            if strike > 0.0 {
                Some(KalshiMarketData {
                    strike,
                    yes_ask: if m.yes_ask.unwrap_or(0) > 0 {
                        m.yes_ask.unwrap() as f64 / 100.0
                    } else {
                        1.0
                    },
                    no_ask: if m.no_ask.unwrap_or(0) > 0 {
                        m.no_ask.unwrap() as f64 / 100.0
                    } else {
                        1.0
                    },
                })
            } else {
                None
            }
        })
        .collect();

    market_data.sort_by(|a, b| a.strike.partial_cmp(&b.strike).unwrap());

    Ok(KalshiData {
        event_ticker,
        current_price,
        markets: market_data,
        coin: coin.to_string(),
    })
}

fn get_arbitrage_for_coin(
    poly_data: PolymarketData,
    kalshi_data: KalshiData,
) -> ArbitrageResult {
    let mut result = ArbitrageResult {
        coin: poly_data.coin.clone(),
        polymarket: Some(poly_data.clone()),
        kalshi: Some(kalshi_data.clone()),
        checks: Vec::new(),
        opportunities: Vec::new(),
        errors: Vec::new(),
    };

    let poly_strike = match poly_data.price_to_beat {
        Some(s) => s,
        None => {
            result.errors.push("Polymarket Strike is None".to_string());
            return result;
        }
    };

    let poly_up_cost = *poly_data.prices.get("Up").unwrap_or(&0.0);
    let poly_down_cost = *poly_data.prices.get("Down").unwrap_or(&0.0);

    let kalshi_markets = &kalshi_data.markets;

    // Find closest market to poly_strike
    let closest_idx = kalshi_markets
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            let diff_a = (a.strike - poly_strike).abs();
            let diff_b = (b.strike - poly_strike).abs();
            diff_a.partial_cmp(&diff_b).unwrap()
        })
        .map(|(idx, _)| idx)
        .unwrap_or(0);

    let start_idx = closest_idx.saturating_sub(4);
    let end_idx = (closest_idx + 5).min(kalshi_markets.len());

    for km in &kalshi_markets[start_idx..end_idx] {
        let kalshi_strike = km.strike;
        let kalshi_yes_cost = km.yes_ask;
        let kalshi_no_cost = km.no_ask;

        if (poly_strike - kalshi_strike).abs() < 0.01 {
            // Equal strikes - check both combinations
            let check1 = create_check(
                kalshi_strike,
                kalshi_yes_cost,
                kalshi_no_cost,
                "Equal",
                "Down",
                "Yes",
                poly_down_cost,
                kalshi_yes_cost,
            );

            if check1.is_arbitrage {
                result.opportunities.push(check1.clone());
            }
            result.checks.push(check1);

            let check2 = create_check(
                kalshi_strike,
                kalshi_yes_cost,
                kalshi_no_cost,
                "Equal",
                "Up",
                "No",
                poly_up_cost,
                kalshi_no_cost,
            );

            if check2.is_arbitrage {
                result.opportunities.push(check2.clone());
            }
            result.checks.push(check2);
        } else if poly_strike > kalshi_strike {
            let check = create_check(
                kalshi_strike,
                kalshi_yes_cost,
                kalshi_no_cost,
                "Poly > Kalshi",
                "Down",
                "Yes",
                poly_down_cost,
                kalshi_yes_cost,
            );

            if check.is_arbitrage {
                result.opportunities.push(check.clone());
            }
            result.checks.push(check);
        } else {
            let check = create_check(
                kalshi_strike,
                kalshi_yes_cost,
                kalshi_no_cost,
                "Poly < Kalshi",
                "Up",
                "No",
                poly_up_cost,
                kalshi_no_cost,
            );

            if check.is_arbitrage {
                result.opportunities.push(check.clone());
            }
            result.checks.push(check);
        }
    }

    result
}

fn create_check(
    kalshi_strike: f64,
    kalshi_yes: f64,
    kalshi_no: f64,
    check_type: &str,
    poly_leg: &str,
    kalshi_leg: &str,
    poly_cost: f64,
    kalshi_cost: f64,
) -> ArbitrageCheck {
    let kalshi_fee = KALSHI_FEE_RATE * (1.0 - kalshi_cost);
    let total_cost = poly_cost + kalshi_cost;
    let total_cost_with_fees = total_cost + kalshi_fee;
    let is_arbitrage = total_cost_with_fees < 1.0;
    let margin = if is_arbitrage {
        1.0 - total_cost_with_fees
    } else {
        0.0
    };

    ArbitrageCheck {
        kalshi_strike,
        kalshi_yes,
        kalshi_no,
        check_type: check_type.to_string(),
        poly_leg: poly_leg.to_string(),
        kalshi_leg: kalshi_leg.to_string(),
        poly_cost,
        kalshi_cost,
        kalshi_fee,
        total_cost,
        total_cost_with_fees,
        is_arbitrage,
        margin,
    }
}

fn log_arbitrage_opportunity(opportunity: &ArbitrageCheck, coin: &str, timestamp: &str) {
    if opportunity.margin < 0.02 {
        return;
    }
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(ARBITRAGE_LOG_FILE)
    {
        let log_entry = format!(
            "\n{}\nARBITRAGE OPPORTUNITY DETECTED\nTimestamp: {}\nCoin: {}\nProfit Margin: ${:.4}\n{}\nStrategy:\n  Polymarket {}: ${:.4}\n  Kalshi {} (${:.0}): ${:.4}\n  Kalshi Fee (7%): ${:.4}\n{}\nTotal Cost (with fees): ${:.4}\nGuaranteed Payout: $1.00\nNet Profit: ${:.4}\n{}\n",
            "=".repeat(80),
            timestamp,
            coin,
            opportunity.margin,
            "-".repeat(80),
            opportunity.poly_leg,
            opportunity.poly_cost,
            opportunity.kalshi_leg,
            opportunity.kalshi_strike,
            opportunity.kalshi_cost,
            opportunity.kalshi_fee,
            "-".repeat(80),
            opportunity.total_cost_with_fees,
            opportunity.margin,
            "=".repeat(80)
        );

        let _ = file.write_all(log_entry.as_bytes());
    }
}

async fn check_arbitrage(client: &reqwest::Client, last_hour: &mut i32) -> anyhow::Result<()> {
    let now = Utc::now();
    let current_hour = now.hour() as i32;
    let timestamp = now.format("%Y-%m-%d %H:%M:%S UTC").to_string();

    // Detect hour change
    if *last_hour != current_hour {
        println!("\n{}", "=".repeat(80));
        println!("HOUR CHANGED: Switching to new markets");
        println!("Previous hour: {}, Current hour: {}", last_hour, current_hour);

        // Show the new market slugs
        let (btc_poly_slug, btc_kalshi_slug, _) = get_current_market_info("BTC");
        let (eth_poly_slug, eth_kalshi_slug, _) = get_current_market_info("ETH");
        println!("BTC Polymarket: {}", btc_poly_slug);
        println!("BTC Kalshi: {}", btc_kalshi_slug);
        println!("ETH Polymarket: {}", eth_poly_slug);
        println!("ETH Kalshi: {}", eth_kalshi_slug);
        println!("{}\n", "=".repeat(80));
        *last_hour = current_hour;
    }

    println!("[{}] Scanning for arbitrage...", now.format("%H:%M:%S"));

    // Fetch BTC data
    let btc_poly = fetch_polymarket_data_struct(client, "BTC").await;
    let btc_kalshi = fetch_kalshi_data_struct(client, "BTC").await;

    // Fetch ETH data
    let eth_poly = fetch_polymarket_data_struct(client, "ETH").await;
    let eth_kalshi = fetch_kalshi_data_struct(client, "ETH").await;

    let mut all_opportunities = Vec::new();

    // Process BTC
    if let (Ok(poly), Ok(kalshi)) = (btc_poly, btc_kalshi) {
        println!("\n=== BTC ===");
        println!("Polymarket Strike: ${:.2}", poly.price_to_beat.unwrap_or(0.0));

        let result = get_arbitrage_for_coin(poly, kalshi);

        for opp in &result.opportunities {
            if opp.margin < 0.02 {
                continue;
            }
            println!("!!! ARBITRAGE FOUND !!!");
            println!("  Type: {}", opp.check_type);
            println!("  Strategy: Poly {} (${:.4}) + Kalshi {} (${:.4})",
                opp.poly_leg, opp.poly_cost, opp.kalshi_leg, opp.kalshi_cost);
            println!("  Kalshi Strike: ${:.0}", opp.kalshi_strike);
            println!("  Total Cost (with fees): ${:.4}", opp.total_cost_with_fees);
            println!("  Profit Margin: ${:.4}", opp.margin);

            log_arbitrage_opportunity(opp, "BTC", &timestamp);
            all_opportunities.push((opp.clone(), "BTC"));
        }

        if result.opportunities.is_empty() {
            println!("No BTC arbitrage opportunities found.");
        }
    }

    // Process ETH
    if let (Ok(poly), Ok(kalshi)) = (eth_poly, eth_kalshi) {
        println!("\n=== ETH ===");
        println!("Polymarket Strike: ${:.2}", poly.price_to_beat.unwrap_or(0.0));

        let result = get_arbitrage_for_coin(poly, kalshi);

        for opp in &result.opportunities {
            if opp.margin < 0.02 {
                continue;
            }
            println!("!!! ARBITRAGE FOUND !!!");
            println!("  Type: {}", opp.check_type);
            println!("  Strategy: Poly {} (${:.4}) + Kalshi {} (${:.4})",
                opp.poly_leg, opp.poly_cost, opp.kalshi_leg, opp.kalshi_cost);
            println!("  Kalshi Strike: ${:.0}", opp.kalshi_strike);
            println!("  Total Cost (with fees): ${:.4}", opp.total_cost_with_fees);
            println!("  Profit Margin: ${:.4}", opp.margin);

            log_arbitrage_opportunity(opp, "ETH", &timestamp);
            all_opportunities.push((opp.clone(), "ETH"));
        }

        if result.opportunities.is_empty() {
            println!("No ETH arbitrage opportunities found.");
        }
    }

    if all_opportunities.is_empty() {
        println!("\nNo arbitrage opportunities found for BTC or ETH.");
    } else {
        println!("\n{} total opportunities logged.", all_opportunities.len());
    }

    println!("{}", "-".repeat(50));

    Ok(())
}

#[tokio::main]
async fn main() {
    println!("Starting Arbitrage Bot...");
    println!("Monitoring BTC and ETH markets");
    println!("Press Ctrl+C to stop.\n");

    let client = reqwest::Client::new();
    let mut last_hour = Utc::now().hour() as i32;

    // Display initial markets
    let (btc_poly_slug, btc_kalshi_slug, _) = get_current_market_info("BTC");
    let (eth_poly_slug, eth_kalshi_slug, _) = get_current_market_info("ETH");
    println!("Initial Markets:");
    println!("  BTC Polymarket: {}", btc_poly_slug);
    println!("  BTC Kalshi: {}", btc_kalshi_slug);
    println!("  ETH Polymarket: {}", eth_poly_slug);
    println!("  ETH Kalshi: {}", eth_kalshi_slug);
    println!();

    loop {
        if let Err(e) = check_arbitrage(&client, &mut last_hour).await {
            eprintln!("Error: {}", e);
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
