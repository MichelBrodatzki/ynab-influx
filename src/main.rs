use chrono::{DateTime, Utc};
use clap::Parser;
use influxdb::InfluxDbWriteable;

#[derive(InfluxDbWriteable)]
struct BudgetReading {
    time: DateTime<Utc>,
    #[influxdb(tag)] category: String,
    budgeted: i64,
    activity: i64,
    balance: i64,
    #[influxdb(tag)] hidden: bool,
    #[influxdb(tag)] deleted: bool
}

/// YNAB Category Exporter to InfluxDB (v2)
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// YNAB API token
    #[arg(long)]
    ynab_token: String,

    /// YNAB Budget ID
    #[arg(long)]
    ynab_budget: String,

    /// InfluxDB URL
    #[arg(long)]
    influx_url: String,

    /// InfluxDB Token
    #[arg(long)]
    influx_token: String,

    /// InfluxDB Bucket
    #[arg(long)]
    influx_bucket: String
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let influx_client = influxdb::Client::new(args.influx_url, args.influx_bucket).with_token(args.influx_token);
    let ynab_client = ynab_rs::client::Client::new(args.ynab_token);

    let mut last_knowledge: Option<i64> = None;
    let mut budget_readings: Vec<influxdb::WriteQuery> = vec![];

    let budget = ynab_client.get_budget(args.ynab_budget.as_str(), None).await.expect_left("Budget ID or API token invalid");

    println!("Starting fetch loop ...");

    loop {
        let categories = ynab_client.get_category_list(&budget.data.budget.id, last_knowledge).await;

        if categories.is_left() {
            let categories = categories.unwrap_left();
            last_knowledge = Some(categories.data.server_knowledge);

            for category_group in categories.data.category_groups {
                if category_group.name == "Internal Master Category" {
                    // TODO: Add flag, if internal master category should also be pushed.
                    continue;
                }

                for category in category_group.categories {

                    budget_readings.push(BudgetReading {
                        time: Utc::now(),
                        category: category.name,
                        budgeted: category.budgeted,
                        activity: category.activity,
                        balance: category.balance,
                        hidden: category.hidden,
                        deleted: category.deleted
                    }.into_query(&budget.data.budget.name))
                }
            }

            if budget_readings.len() > 0 {
                println!("[{} -> '{}'] Writing {} readings ...", &last_knowledge.unwrap(), &budget.data.budget.name, &budget_readings.len());

                let write_result = influx_client.query(&budget_readings).await;
                assert!(write_result.is_ok(), "Write result was not ok");

                budget_readings.clear();
                
                println!("[{} -> '{}'] Successfully written ...", &last_knowledge.unwrap(), &budget.data.budget.name);
            } else {
                println!("[{} -> '{}'] Nothing to write ...", &last_knowledge.unwrap(), &budget.data.budget.name);
            }
        } else {
            dbg!(categories.unwrap_right());
            return;
        }
        
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}
