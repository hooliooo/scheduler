use std::time::Duration;

use async_trait::async_trait;

/// Represents a task that should be executed constantly in an interval
#[async_trait]
pub trait Task: Sync + Send {
    /// The name of the Task
    fn name(&self) -> &'static str;

    /// The interval at which the Task should be executed. For example, every 20 seconds, 40
    /// seconds or 60 seconds, etc.
    fn duration(&self) -> Duration;

    /// The code to be executed
    async fn execute(&self);

    /// The cleanup code to be executed when the shutdown signal is received
    async fn cleanup(&self);
}

#[cfg(feature = "tokio")]
mod tokio_extras {

    use std::sync::Arc;

    use axum::{Router, routing::get};
    use futures::future::join_all;
    use tokio::{
        net::TcpListener,
        signal,
        sync::broadcast::{Receiver, Sender},
        task, time,
    };
    use tracing::{info, warn};

    use crate::Task;

    /// Sets up the Tasks into tokio tasks and sets up the broadcast channel and SIGINT logic
    /// for shutdown
    /// # Arguments
    /// * `tasks` - The Tasks to be transformed into tokio tasks
    pub async fn setup(tasks: Vec<Arc<dyn Task>>, http_port: &'static str) {
        // Create broadcast channel which allows one sender to send messages to multiple receivers
        // Used to inform background jobs when to shutdown
        // Channel capacity is set to one since only one shutdown signal is needed
        let (shutdown_tx, _): (Sender<()>, Receiver<()>) = tokio::sync::broadcast::channel(1);

        let health_task = {
            let mut shutdown_rx = shutdown_tx.subscribe();
            task::spawn(async move {
                info!("Starting health check HTTP server on port {}", http_port);
                tokio::select! {
                    result = configure_health_endpoint(http_port) => {
                        if let Err(e) = result {
                            warn!("Health check server error: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Health check server shutting down");
                    }
                }
            })
        };

        // Transform tasks into join handles
        let tasks: Vec<task::JoinHandle<()>> = {
            let mut tasks: Vec<task::JoinHandle<()>> = tasks
                .into_iter()
                .map(|task| create_interval_task(task, shutdown_tx.clone()))
                .collect();
            tasks.push(health_task);
            tasks
        };

        // Blocks the main thread until Ctrl+C (SIGINT) is received
        signal::ctrl_c().await.expect("Failed to listen to Ctrl+C");
        warn!("Ctrl+C signal received. Shutting down...");

        // Broadcast the shutdown signal to the tasks
        let _ = shutdown_tx.send(());
        join_all(tasks).await;

        info!("Tasks have stopped. Shutdown complete");
    }

    /// Creates a tokio Task based on the Task. Spawns a tokio asynchronouse green thread with
    /// a tokio select statement enclosed in a loop
    ///
    /// When the shutdown signal is received from the sender the Task's cleanup method is
    /// called. Otherwise, the Task's execute method is called when the interval, defined by
    /// the Task's duration method, is ticked
    ///
    /// # Arguments
    /// * `task` - The code to be executed within the tokio task
    /// * `shutdown_tx` - The broadcast channel's Sender which signals the tokio task when to shutdown
    ///
    pub fn create_interval_task(
        task: Arc<dyn Task>,
        shutdown_tx: Sender<()>,
    ) -> task::JoinHandle<()> {
        let mut shutdown_rx = shutdown_tx.subscribe();
        let duration = task.duration();

        tokio::spawn(async move {
            info!(job = task.name(), ?duration, "Starting job");

            let mut interval = time::interval(duration);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

            // First tick completes immediately. See tokio docs on tick
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        task.cleanup().await;
                        warn!(job = task.name(), "Received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        task.execute().await;
                    }
                }
            }
        })
    }

    /// Configures a health endpoint for k8s readiness probes
    /// # Arguments
    /// * `port` - The port the axum server should listen in
    ///
    pub(crate) async fn configure_health_endpoint(port: &str) -> Result<(), std::io::Error> {
        let app: Router = {
            async fn health() -> &'static str {
                "ok"
            }

            async fn ready() -> &'static str {
                "ok"
            }

            Router::new()
                .route("/healthz", get(health))
                .route("/readyz", get(ready))
        };

        let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .expect("Could not create TcpListener");
        let address = listener
            .local_addr()
            .expect("Cannot get address from TcpListener");
        info!("Health endpoint listening on {}", address);
        axum::serve(listener, app).await
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tokio_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use tokio::{sync::broadcast, time};

    use crate::tokio_extras::{configure_health_endpoint, create_interval_task};

    use super::*;

    /// A mock task that tracks execution count
    struct MockTask {
        name: &'static str,
        duration: Duration,
        execute_count: Arc<AtomicU32>,
        cleanup_count: Arc<AtomicU32>,
    }

    impl MockTask {
        fn new(name: &'static str, duration: Duration) -> Self {
            Self {
                name,
                duration,
                execute_count: Arc::new(AtomicU32::new(0)),
                cleanup_count: Arc::new(AtomicU32::new(0)),
            }
        }

        fn execute_count(&self) -> u32 {
            self.execute_count.load(Ordering::SeqCst)
        }

        fn cleanup_count(&self) -> u32 {
            self.cleanup_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Task for MockTask {
        fn name(&self) -> &'static str {
            self.name
        }

        fn duration(&self) -> Duration {
            self.duration
        }

        async fn execute(&self) {
            self.execute_count.fetch_add(1, Ordering::SeqCst);
        }

        async fn cleanup(&self) {
            self.cleanup_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_task_executes_at_interval() {
        let task = Arc::new(MockTask::new("test_op", Duration::from_millis(100)));
        let (shutdown_tx, _) = broadcast::channel(1);

        let join_handle = create_interval_task(task.clone(), shutdown_tx.clone());

        // Wait for multiple intervals
        time::sleep(Duration::from_millis(350)).await;

        // Send shutdown signal
        let _ = shutdown_tx.send(());
        let _ = join_handle.await;

        // Should have executed approximately 3 times (at 100ms, 200ms, 300ms)
        let execute_count = task.execute_count();
        assert!(
            (2..=4).contains(&execute_count),
            "Expected 2-4 executions, got {}",
            execute_count
        );

        let cleanup_count = task.cleanup_count();
        assert_eq!(
            cleanup_count, 1,
            "Expected a count of 1, got {}",
            cleanup_count
        );
    }

    #[tokio::test]
    async fn test_task_cleanup_on_shutdowwn() {
        let task = Arc::new(MockTask::new("test_op", Duration::from_millis(100)));
        let (shutdown_tx, _) = broadcast::channel(1);

        let join_handle = create_interval_task(task.clone(), shutdown_tx.clone());

        // Wait for multiple intervals
        time::sleep(Duration::from_millis(10)).await;

        // Send shutdown signal
        let _ = shutdown_tx.send(());
        let _ = join_handle.await;

        let execute_count = task.execute_count();
        assert_eq!(
            execute_count, 0,
            "Expected a count of 0, get {}",
            execute_count
        );

        let cleanup_count = task.cleanup_count();
        assert_eq!(
            cleanup_count, 1,
            "Expected a count of 1, got {}",
            cleanup_count
        );
    }

    #[tokio::test]
    async fn test_tasks_run_independently() {
        let task_one = Arc::new(MockTask::new("op1", Duration::from_millis(50)));
        let task_two = Arc::new(MockTask::new("op2", Duration::from_millis(100)));
        let (shutdown_tx, _) = broadcast::channel(1);

        let join_handle_one = create_interval_task(task_one.clone(), shutdown_tx.clone());
        let join_handle_two = create_interval_task(task_two.clone(), shutdown_tx.clone());

        time::sleep(Duration::from_millis(250)).await;

        let _ = shutdown_tx.send(());
        let _ = tokio::join!(join_handle_one, join_handle_two);

        let execute_count_one = task_one.execute_count();
        let execute_count_two = task_two.execute_count();

        let cleanup_count_one = task_one.cleanup_count();
        let cleanup_count_two = task_two.cleanup_count();

        assert!(
            execute_count_one > execute_count_two,
            "Task one should have higher count '{}' than Task two's count '{}'",
            execute_count_one,
            execute_count_two
        );

        assert_eq!(
            cleanup_count_one, 1,
            "Cleanup count should be 1 for task one. Count = {}",
            cleanup_count_one
        );

        assert_eq!(
            cleanup_count_two, 1,
            "Cleanup count should be 1 for task two. Count = {}",
            cleanup_count_two
        );
    }

    #[tokio::test]
    async fn test_immediate_shutdown() {
        let task = Arc::new(MockTask::new("test_op", Duration::from_millis(100)));
        let (shutdown_tx, _) = broadcast::channel(1);

        let join_handle = create_interval_task(task.clone(), shutdown_tx.clone());

        // Send shutdown signal
        let _ = shutdown_tx.send(());
        let _ = join_handle.await;

        let execute_count = task.execute_count();
        assert_eq!(
            execute_count, 0,
            "Expected a count of 0, get {}",
            execute_count
        );

        let cleanup_count = task.cleanup_count();
        assert_eq!(
            cleanup_count, 1,
            "Expected a count of 1, got {}",
            cleanup_count
        );
    }

    #[tokio::test]
    async fn test_health_endpoints() {
        // Start health endpoint in background
        let port = "38080"; // Use non-standard port for testing
        let handle = tokio::spawn(async move {
            let _ = configure_health_endpoint(port).await;
        });

        // Give server time to start
        time::sleep(Duration::from_millis(100)).await;

        // Test health endpoint
        let health_response = reqwest::get(format!("http://127.0.0.1:{}/healthz", port))
            .await
            .expect("Failed to call /healthz");
        assert_eq!(health_response.status(), 200);
        assert_eq!(health_response.text().await.unwrap(), "ok");

        // Test ready endpoint
        let ready_response = reqwest::get(format!("http://127.0.0.1:{}/readyz", port))
            .await
            .expect("Failed to call /readyz");
        assert_eq!(ready_response.status(), 200);
        assert_eq!(ready_response.text().await.unwrap(), "ok");

        // Clean up
        handle.abort();
    }
}
