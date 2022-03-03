use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// A TaskGroup is used to spawn a collection of tasks. The collection has two properties:
/// * if any task returns an error or panicks, all tasks are terminated.
/// * if the `TaskManager` returned by `TaskGroup::new` is dropped, all tasks are terminated.
pub struct TaskGroup<E> {
    new_task: mpsc::Sender<ChildHandle<E>>,
}
// not the derived impl: E does not need to be Clone
impl<E> Clone for TaskGroup<E> {
    fn clone(&self) -> Self {
        Self {
            new_task: self.new_task.clone(),
        }
    }
}

impl<E: Send + 'static> TaskGroup<E> {
    pub fn new() -> (Self, TaskManager<E>) {
        let (new_task, reciever) = mpsc::channel(64);
        let group = TaskGroup { new_task };
        let manager = TaskManager::new(reciever);
        (group, manager)
    }

    pub fn spawn(
        &self,
        name: impl AsRef<str>,
        f: impl Future<Output = Result<(), E>> + Send + 'static,
    ) -> impl Future<Output = Result<(), SpawnError>> {
        let name = name.as_ref().to_string();
        let join = tokio::task::spawn(f);
        let new_task = self.new_task.clone();
        async move {
            match new_task.send(ChildHandle { name, join }).await {
                Ok(()) => Ok(()),
                // If there is no receiver alive to manage the new task, drop the child in error to
                // cancel it:
                Err(_child) => Err(SpawnError::GroupDied),
            }
        }
    }

    pub async fn spawn_on(
        &self,
        name: impl AsRef<str>,
        runtime: tokio::runtime::Handle,
        f: impl Future<Output = Result<(), E>> + Send + 'static,
    ) -> Result<(), SpawnError> {
        let name = name.as_ref().to_string();
        let join = runtime.spawn(f);
        match self.new_task.send(ChildHandle { name, join }).await {
            Ok(()) => Ok(()),
            // If there is no receiver alive to manage the new task, drop the child in error to
            // cancel it:
            Err(_child) => Err(SpawnError::GroupDied),
        }
    }

    pub async fn spawn_local(
        &self,
        name: impl AsRef<str>,
        f: impl Future<Output = Result<(), E>> + 'static,
    ) -> Result<(), SpawnError> {
        let name = name.as_ref().to_string();
        let join = tokio::task::spawn_local(f);
        match self.new_task.send(ChildHandle { name, join }).await {
            Ok(()) => Ok(()),
            // If there is no receiver alive to manage the new task, drop the child in error to
            // cancel it:
            Err(_child) => Err(SpawnError::GroupDied),
        }
    }

    /// Returns `true` if the task group has been shut down.
    pub fn is_closed(&self) -> bool {
        self.new_task.is_closed()
    }
}

struct ChildHandle<E> {
    name: String,
    join: JoinHandle<Result<(), E>>,
}

impl<E> ChildHandle<E> {
    // Pin projection. Since there is only this one required, avoid pulling in the proc macro.
    pub fn pin_join(self: Pin<&mut Self>) -> Pin<&mut JoinHandle<Result<(), E>>> {
        unsafe { self.map_unchecked_mut(|s| &mut s.join) }
    }
    fn cancel(&mut self) {
        self.join.abort();
    }
}

// As a consequence of this Drop impl, when a TaskManager is dropped, all of its children will be
// canceled.
impl<E> Drop for ChildHandle<E> {
    fn drop(&mut self) {
        self.cancel()
    }
}

/// A TaskManager is used to manage a collection of tasks. There are two
/// things you can do with it:
/// * TaskManager impls Future, so you can poll or await on it. It will be
/// Ready when all tasks return Ok(()) and the associated `TaskGroup` is
/// dropped (so no more tasks can be created), or when any task panicks or
/// returns an Err(E).
/// * When a TaskManager is dropped, all tasks it contains are canceled
/// (terminated). So, if you use a combinator like
/// `tokio::time::timeout(duration, task_manager).await`, all tasks will be
/// terminated if the timeout occurs.
pub struct TaskManager<E> {
    channel: Option<mpsc::Receiver<ChildHandle<E>>>,
    children: Vec<Pin<Box<ChildHandle<E>>>>,
}

impl<E> TaskManager<E> {
    fn new(channel: mpsc::Receiver<ChildHandle<E>>) -> Self {
        Self {
            channel: Some(channel),
            children: Vec::new(),
        }
    }
}

impl<E> Future for TaskManager<E> {
    type Output = Result<(), RuntimeError<E>>;
    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut s = self.as_mut();

        // If the channel is still open, take it out of s to satisfy the borrow checker.
        // We'll put it right back once we're done polling it.
        if let Some(mut channel) = s.channel.take() {
            // This loop processes each message in the channel until it is either empty
            // or closed.
            s.channel = loop {
                match channel.poll_recv(ctx) {
                    Poll::Pending => {
                        // No more messages, but channel still open
                        break Some(channel);
                    }
                    Poll::Ready(Some(new_child)) => {
                        // Put element from channel into the children
                        s.children.push(Box::pin(new_child));
                    }
                    Poll::Ready(None) => {
                        // Channel has closed and all messages have been recieved. No
                        // longer need channel.
                        break None;
                    }
                }
            };
        }

        // Need to mutate s after discovering error: store here temporarily
        let mut err = None;
        // Need to iterate through vec, possibly removing via swap_remove, so we cant use
        // a normal iterator:
        let mut child_ix = 0;
        while s.children.get(child_ix).is_some() {
            let child = s
                .children
                .get_mut(child_ix)
                .expect("precondition: child exists at index");
            match child.as_mut().pin_join().poll(ctx) {
                // Pending children get retained - move to next
                Poll::Pending => child_ix += 1,
                // Child returns successfully: remove it from children.
                // Then execute the loop body again with ix unchanged, because
                // last element was swapped into child_ix.
                Poll::Ready(Ok(Ok(()))) => {
                    let _ = s.children.swap_remove(child_ix);
                }
                // Child returns with error: yield the error
                Poll::Ready(Ok(Err(error))) => {
                    err = Some(RuntimeError::Application {
                        name: child.name.clone(),
                        error,
                    });
                    break;
                }
                // Child join error: it either panicked or was canceled
                Poll::Ready(Err(e)) => {
                    err = Some(match e.try_into_panic() {
                        Ok(panic) => RuntimeError::Panic {
                            name: child.name.clone(),
                            panic,
                        },
                        Err(_) => unreachable!("impossible to cancel tasks in TaskGroup"),
                    });
                    break;
                }
            }
        }

        if let Some(err) = err {
            // Drop all children, and the channel reciever, current tasks are destroyed
            // and new tasks cannot be created:
            s.children.truncate(0);
            s.channel.take();
            // Return the error:
            Poll::Ready(Err(err))
        } else if s.children.is_empty() {
            if s.channel.is_none() {
                // Task manager is complete when there are no more children, and
                // no more channel to get more children:
                Poll::Ready(Ok(()))
            } else {
                // Channel is still pending, so we are not done:
                Poll::Pending
            }
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug)]
pub enum RuntimeError<E> {
    Panic {
        name: String,
        panic: Box<dyn Any + Send + 'static>,
    },
    Application {
        name: String,
        error: E,
    },
}
impl<E: std::fmt::Display + std::error::Error> std::error::Error for RuntimeError<E> {}
impl<E: std::fmt::Display> std::fmt::Display for RuntimeError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RuntimeError::Panic { name, .. } => {
                write!(f, "Task `{}` panicked", name)
            }
            RuntimeError::Application { name, error } => {
                write!(f, "Task `{}` errored: {}", name, error)
            }
        }
    }
}

#[derive(Debug)]
pub enum SpawnError {
    GroupDied,
}
impl std::error::Error for SpawnError {}
impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SpawnError::GroupDied => write!(f, "Task group died"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::{anyhow, Error};
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn no_task() {
        let (tg, tm): (TaskGroup<Error>, TaskManager<Error>) = TaskGroup::new();
        drop(tg); // Must drop the ability to spawn for the taskmanager to be finished
        assert!(tm.await.is_ok());
    }

    #[tokio::test]
    async fn one_empty_task() {
        let (tg, tm): (TaskGroup<Error>, TaskManager<Error>) = TaskGroup::new();
        tg.spawn("empty", async move { Ok(()) }).await.unwrap();
        drop(tg); // Must drop the ability to spawn for the taskmanager to be finished
        assert!(tm.await.is_ok());
    }

    #[tokio::test]
    async fn empty_child() {
        let (tg, tm): (TaskGroup<Error>, TaskManager<Error>) = TaskGroup::new();
        tg.clone()
            .spawn("parent", async move {
                tg.spawn("child", async move { Ok(()) }).await.unwrap();
                Ok(())
            })
            .await
            .unwrap();
        assert!(tm.await.is_ok());
    }

    #[tokio::test]
    async fn many_nested_children() {
        // Record a side-effect to demonstate that all of these children executed
        let log = Arc::new(Mutex::new(vec![0usize]));
        let l = log.clone();
        let (tg, tm): (TaskGroup<Error>, TaskManager<_>) = TaskGroup::new();
        tg.clone()
            .spawn("root", async move {
                let log = log.clone();
                let tg2 = tg.clone();
                log.lock().await.push(1);
                tg.spawn("child", async move {
                    let tg3 = tg2.clone();
                    log.lock().await.push(2);
                    tg2.spawn("grandchild", async move {
                        log.lock().await.push(3);
                        tg3.spawn("great grandchild", async move {
                            log.lock().await.push(4);
                            Ok(())
                        })
                        .await
                        .unwrap();
                        Ok(())
                    })
                    .await
                    .unwrap();
                    Ok(())
                })
                .await
                .unwrap();
                Ok(())
            })
            .await
            .unwrap();
        assert!(tm.await.is_ok());
        assert_eq!(*l.lock().await, vec![0usize, 1, 2, 3, 4]);
    }
    #[tokio::test]
    async fn many_nested_children_error() {
        // Record a side-effect to demonstate that all of these children executed
        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(vec![]));
        let l = log.clone();

        let (tg, tm): (TaskGroup<Error>, TaskManager<_>) = TaskGroup::new();
        let tg2 = tg.clone();
        tg.spawn("root", async move {
            log.lock().await.push("in root");
            let tg3 = tg2.clone();
            tg2.spawn("child", async move {
                log.lock().await.push("in child");
                let tg4 = tg3.clone();
                tg3.spawn("grandchild", async move {
                    log.lock().await.push("in grandchild");
                    tg4.spawn("great grandchild", async move {
                        log.lock().await.push("in great grandchild");
                        Err(anyhow!("sooner or later you get a failson"))
                    })
                    .await
                    .unwrap();
                    sleep(Duration::from_secs(1)).await;
                    // The great-grandchild returning error should terminate this task.
                    unreachable!("sleepy grandchild should never wake");
                })
                .await
                .unwrap();
                Ok(())
            })
            .await
            .unwrap();
            Ok(())
        })
        .await
        .unwrap();
        drop(tg);
        assert_eq!(format!("{:?}", tm.await),
            "Err(Application { name: \"great grandchild\", error: sooner or later you get a failson })");
        assert_eq!(
            *l.lock().await,
            vec![
                "in root",
                "in child",
                "in grandchild",
                "in great grandchild"
            ]
        );
    }
    #[tokio::test]
    async fn root_task_errors() {
        let (tg, tm): (TaskGroup<Error>, TaskManager<_>) = TaskGroup::new();
        tg.spawn("root", async move { Err(anyhow!("idk!")) })
            .await
            .unwrap();
        let res = tm.await;
        assert!(res.is_err());
        assert_eq!(
            format!("{:?}", res),
            "Err(Application { name: \"root\", error: idk! })"
        );
    }

    #[tokio::test]
    async fn child_task_errors() {
        let (tg, tm): (TaskGroup<Error>, TaskManager<_>) = TaskGroup::new();
        tg.clone()
            .spawn("parent", async move {
                tg.spawn("child", async move { Err(anyhow!("whelp")) })
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
        let res = tm.await;
        assert!(res.is_err());
        assert_eq!(
            format!("{:?}", res),
            "Err(Application { name: \"child\", error: whelp })"
        );
    }

    #[tokio::test]
    async fn root_task_panics() {
        let (tg, tm): (TaskGroup<Error>, TaskManager<_>) = TaskGroup::new();
        tg.spawn("root", async move { panic!("idk!") })
            .await
            .unwrap();

        let res = tm.await;
        assert!(res.is_err());
        match res.err().unwrap() {
            RuntimeError::Panic { name, panic } => {
                assert_eq!(name, "root");
                assert_eq!(*panic.downcast_ref::<&'static str>().unwrap(), "idk!");
            }
            e => panic!("wrong error variant! {:?}", e),
        }
    }

    #[tokio::test]
    async fn child_task_panics() {
        let (tg, tm): (TaskGroup<Error>, TaskManager<_>) = TaskGroup::new();
        let tg2 = tg.clone();
        tg.spawn("root", async move {
            tg2.spawn("child", async move { panic!("whelp") }).await?;
            Ok(())
        })
        .await
        .unwrap();

        let res = tm.await;
        assert!(res.is_err());
        match res.err().unwrap() {
            RuntimeError::Panic { name, panic } => {
                assert_eq!(name, "child");
                assert_eq!(*panic.downcast_ref::<&'static str>().unwrap(), "whelp");
            }
            e => panic!("wrong error variant! {:?}", e),
        }
    }

    #[tokio::test]
    async fn child_sleep_no_timeout() {
        // Record a side-effect to demonstate that all of these children executed
        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(vec![]));
        let l = log.clone();
        let (tg, tm): (TaskGroup<Error>, TaskManager<_>) = TaskGroup::new();
        let tg2 = tg.clone();
        tg.spawn("parent", async move {
            tg2.spawn("child", async move {
                log.lock().await.push("child gonna nap");
                sleep(Duration::from_secs(1)).await; // 1 sec sleep, 2 sec timeout
                log.lock().await.push("child woke up happy");
                Ok(())
            })
            .await?;
            Ok(())
        })
        .await
        .unwrap();

        drop(tg); // Not going to launch anymore tasks
        let res = tokio::time::timeout(Duration::from_secs(2), tm).await;
        assert!(res.is_ok(), "no timeout");
        assert!(res.unwrap().is_ok(), "returned successfully");
        assert_eq!(
            *l.lock().await,
            vec!["child gonna nap", "child woke up happy"]
        );
    }

    #[tokio::test]
    async fn child_sleep_timeout() {
        let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(vec![]));
        let l = log.clone();

        let (tg, tm): (TaskGroup<Error>, TaskManager<_>) = TaskGroup::new();
        let tg2 = tg.clone();
        tg.spawn("parent", async move {
            tg2.spawn("child", async move {
                log.lock().await.push("child gonna nap");
                sleep(Duration::from_secs(2)).await; // 2 sec sleep, 1 sec timeout
                unreachable!("child should not wake from this nap");
            })
            .await?;
            Ok(())
        })
        .await
        .unwrap();

        let res = tokio::time::timeout(Duration::from_secs(1), tm).await;
        assert!(res.is_err(), "timed out");
        assert_eq!(*l.lock().await, vec!["child gonna nap"]);
    }
}
