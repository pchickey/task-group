use anyhow::{anyhow, Error};
use task_group::{RuntimeError, TaskGroup, TaskManager};
use tokio::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let (tg, tm): (_, TaskManager<Error>) = TaskGroup::new();
    println!("hello dogs!");

    tg.spawn("gussie", async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        println!("Gussie goes and sucks on a blanket");
        Ok(())
    })
    .await?;
    tg.spawn("willa", async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("Willa wants to play outside");
        tokio::time::sleep(Duration::from_secs(1)).await;
        if true {
            println!("Willa is upset and about to do something naughty");
            Err(anyhow!("willa is chewing on the blinds"))
        } else {
            Ok(())
        }
    })
    .await?;
    tg.spawn("sparky", async move {
        for _ in 1..4usize {
            tokio::time::sleep(Duration::from_millis(500)).await;
            println!("Sparky wants to go out too");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("Sparky is taking a nap");
        Ok(())
    })
    .await?;

    drop(tg);

    match tokio::time::timeout_at(deadline, tm).await {
        Ok(Ok(())) => {
            println!("dogs have not defeated me");
            Ok(())
        }
        Ok(Err(RuntimeError::Application { name, error })) => {
            Err(error.context(format!("task `{}` died", name)))
        }
        Ok(Err(RuntimeError::Panic { name, panic })) => {
            Err(anyhow!("Panic: {:?}", panic).context(name))
        }
        Err(_) => Err(anyhow!("timeout")),
    }
}
