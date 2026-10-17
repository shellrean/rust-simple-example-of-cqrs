use async_trait::async_trait;
use cqrs_es::{Aggregate, DomainEvent, EventStore};
use cqrs_es::persist::PersistedEventStore;
use postgres_es::{default_postgress_pool, PostgresCqrs, postgres_cqrs, PostgresEventRepository};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Deserialize)]
pub enum BankAccountCommand {
    OpenAccount { account_id: String },
    DepositMoney { amount: f64 }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BankAccountEvent {
    AccountOpened { account_id: String },
    CustomerDepositedMoney { amount: f64 },
}

impl DomainEvent for BankAccountEvent {
    fn event_type(&self) -> String {
        let event_type: &str = match self {
            BankAccountEvent::AccountOpened { .. } => "AccountOpened",
            BankAccountEvent::CustomerDepositedMoney {..} => "CustomerDepositedMoney",
        };
        event_type.to_string()
    }

    fn event_version(&self) -> String {
        "2.0".to_string()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BankAccount {
    pub account_id: String,
    pub balance: f64,
}

#[derive(Debug, Error)]
pub enum BankAccountError {
    #[error("account already opened")]
    AccountAlreadyOpen,

    #[error("invalid amount")]
    InvalidAmount
}

#[async_trait]
impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Event = BankAccountEvent;
    type Error = BankAccountError;
    type Services = ();

    fn aggregate_type() -> String {
        "Account".to_string()
    }

    async fn handle(&self, command: Self::Command, service: &Self::Services) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountCommand::OpenAccount {account_id,} => {
                if !self.account_id.is_empty() {
                    return Err(BankAccountError::AccountAlreadyOpen);
                }
                Ok(vec![BankAccountEvent::AccountOpened {account_id}])
            }
            BankAccountCommand::DepositMoney {amount} => {
                if amount < 0.0 {
                    return Err(BankAccountError::InvalidAmount);
                }
                Ok(vec![BankAccountEvent::CustomerDepositedMoney {amount}])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::CustomerDepositedMoney {amount} => {
                self.balance += amount;
            }
            BankAccountEvent::AccountOpened { account_id } => {
                self.account_id = account_id;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let db_url = "postgres://postgres:postgres@localhost:5432/learn_es";

    let pool = default_postgress_pool(db_url).await;


    //
    // let cqrs: PostgresCqrs<BankAccount> = postgres_cqrs(
    //     pool.clone(),
    //     vec![],
    //     (),
    // );
    //

    // cqrs.execute(
    //     "123",
    //     BankAccountCommand::OpenAccount {
    //         account_id: "1".to_string()
    //     }
    // ).await.unwrap();

    // cqrs.execute(
    //     "123",
    //     BankAccountCommand::DepositMoney {
    //         amount: 6.0,
    //     }
    // ).await.unwrap();

    let event_repo = PostgresEventRepository::new(pool.clone());

    let event_store = PersistedEventStore::<PostgresEventRepository, BankAccount>::new_event_store(
        event_repo,
    );

    let context = event_store
        .load_aggregate("123")
        .await
        .unwrap();

    let aggregate = context.aggregate;

    println!("Account ID: {}", aggregate.account_id);
    println!("Balance: {}", aggregate.balance);
}
