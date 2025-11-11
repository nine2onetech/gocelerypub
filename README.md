# Go Celery Publisher

A lightweight, goroutine-safe Go package for publishing Celery tasks to AMQP brokers (RabbitMQ) with a fire-and-forget mechanism.

## Features

- **Simple API** - Easy-to-use interface for publishing Celery tasks
- **Dual Publishing Modes** - Choose between DirectMode (simple) or ChannelMode (goroutine-safe)
- **Thread-Safe** - ChannelMode handles concurrent publishing from multiple goroutines safely
- **Automatic Reconnection** - Built-in connection recovery and retry logic
- **No Worker Required** - Pure publisher implementation, no consumer/worker needed
- **Celery Compatible** - Generates standard Celery protocol v1 messages

## Why This Package?

AMQP connections and channels are **not goroutine-safe**. In high-performance applications using goroutines, concurrent access can cause frame mixing issues or require creating new connections for each publish operation, leading to poor performance.

This package solves the problem by offering two modes:
- **DirectMode**: Simple, single-threaded publishing (default)
- **ChannelMode**: Uses a dedicated goroutine with an internal task channel to handle concurrent publishing safely

## Installation

```bash
go get github.com/nine2onetech/gocelerypub
```

## Quick Start

### DirectMode (Simple)

```go
package main

import (
    "log"
    publisher "github.com/nine2onetech/gocelerypub"
)

func main() {
    // Create a publisher in DirectMode (default)
    pub, err := publisher.New(publisher.Config{
        BrokerType: publisher.AMQP,
        HostURL:    "amqp://guest:guest@localhost:5672/",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer pub.Close()

    // Publish a task
    err = pub.Publish(&publisher.PublishRequest{
        Queue:  "tasks",
        Task:   "myapp.tasks.add",
        Args:   []interface{}{1, 2},
        Kwargs: map[string]interface{}{"debug": true},
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### ChannelMode (Goroutine-Safe)

```go
package main

import (
    "log"
    "sync"
    publisher "github.com/nine2onetech/gocelerypub"
)

func main() {
    // Create a publisher in ChannelMode
    pub, err := publisher.New(publisher.Config{
        BrokerType:  publisher.AMQP,
        HostURL:     "amqp://guest:guest@localhost:5672/",
        PublishMode: publisher.ChannelMode,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer pub.Close()

    // Publish from multiple goroutines safely
    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()

            err := pub.Send(&publisher.PublishRequest{
                Queue:  "tasks",
                Task:   "myapp.tasks.process",
                Args:   []interface{}{id},
                Kwargs: map[string]interface{}{"worker_id": id},
            })
            if err != nil {
                log.Printf("Failed to publish task %d: %v", id, err)
            }
        }(i)
    }
    wg.Wait()
}
```

## Configuration

```go
type Config struct {
    BrokerType  BrokerType  // Type of broker (currently only AMQP supported)
    HostURL     string      // Broker connection URL
    PublishMode PublishMode // DirectMode or ChannelMode (default: DirectMode)
}
```

### Broker Types

| Type | Description |
|------|-------------|
| `AMQP` | RabbitMQ/AMQP broker |

### Publishing Modes

| Mode | Description | Use Case | Goroutine-Safe |
|------|-------------|----------|----------------|
| `DirectMode` | Publishes messages directly | Simple, single-threaded applications | ❌ No |
| `ChannelMode` | Uses internal task channel with dedicated goroutine | High-performance concurrent applications | ✅ Yes |

## API Reference

### Creating a Publisher

```go
pub, err := publisher.New(publisher.Config{
    BrokerType:  publisher.AMQP,
    HostURL:     "amqp://guest:guest@localhost:5672/",
    PublishMode: publisher.ChannelMode, // or publisher.DirectMode
})
```

### Publishing Methods

#### `Publish(req *PublishRequest) error`

Publishes a task directly. **Not goroutine-safe in DirectMode**.

```go
err := pub.Publish(&publisher.PublishRequest{
    Queue:  "my_queue",
    Task:   "myapp.tasks.process",
    Args:   []interface{}{1, 2, 3},
    Kwargs: map[string]interface{}{"key": "value"},
})
```

#### `Send(req *PublishRequest) error`

Publishes a task through internal channel. **Only available in ChannelMode**. Goroutine-safe.

```go
err := pub.Send(&publisher.PublishRequest{
    Queue:  "my_queue",
    Task:   "myapp.tasks.process",
    Args:   []interface{}{1, 2, 3},
    Kwargs: map[string]interface{}{"key": "value"},
})
```

#### `Close() error`

Gracefully shuts down the publisher, closing channels and broker connections.

```go
defer pub.Close()
```

### PublishRequest

```go
type PublishRequest struct {
    Queue  string                 // Target queue name
    Task   string                 // Task name (e.g., "tasks.add")
    Args   []interface{}          // Positional arguments
    Kwargs map[string]interface{} // Keyword arguments
}
```

## Architecture

### DirectMode

```
┌─────────┐
│ Your    │
│ Code    │──Publish()──► Publisher ──► AMQP Broker
└─────────┘
```

Simple and straightforward, but not safe for concurrent goroutines.

### ChannelMode

```
┌─────────┐
│Goroutine│──┐
└─────────┘  │
             │
┌─────────┐  │    ┌──────────┐    ┌────────────┐    ┌────────────┐
│Goroutine│──┼───►│Task Chan │───►│ Dedicated  │───►│    AMQP    │
└─────────┘  │    └──────────┘    │ Goroutine  │    │   Broker   │
             │                     └────────────┘    └────────────┘
┌─────────┐  │
│Goroutine│──┘
└─────────┘
```

All goroutines send requests to an internal channel. A single dedicated goroutine processes all AMQP operations sequentially, avoiding concurrency issues.

## Error Handling

The package includes built-in error handling:

- **Automatic Reconnection**: If the broker connection is lost, it automatically attempts to reconnect
- **PRECONDITION_FAILED Retry**: On queue mismatch errors, it reconnects and retries once
- **Error Propagation**: All errors are properly wrapped and returned to the caller

## Testing

Run the test suite:

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run with verbose output
go test -v ./...
```

The package includes comprehensive unit tests using mocks for the broker interface.

## Examples

### Publishing with Custom Task Options

```go
req := &publisher.PublishRequest{
    Queue:  "priority_tasks",
    Task:   "myapp.tasks.urgent_process",
    Args:   []interface{}{"data1", "data2"},
    Kwargs: map[string]interface{}{
        "priority": "high",
        "timeout":  300,
    },
}

err := pub.Send(req)
if err != nil {
    log.Printf("Failed to publish: %v", err)
}
```

### Handling Errors Gracefully

```go
err := pub.Send(req)
if err != nil {
    if strings.Contains(err.Error(), "connection") {
        // Handle connection errors
        log.Println("Connection lost, will retry...")
    } else {
        // Handle other errors
        log.Printf("Publish failed: %v", err)
    }
}
```

### Bulk Publishing

```go
tasks := []string{"task1", "task2", "task3", "task4", "task5"}

var wg sync.WaitGroup
for _, task := range tasks {
    wg.Add(1)
    go func(t string) {
        defer wg.Done()

        err := pub.Send(&publisher.PublishRequest{
            Queue: "bulk_queue",
            Task:  t,
            Args:  []interface{}{},
        })
        if err != nil {
            log.Printf("Failed to publish %s: %v", t, err)
        }
    }(task)
}
wg.Wait()
```

## Performance Considerations

- **ChannelMode** adds minimal overhead (single channel operation) while providing full goroutine safety
- The dedicated goroutine in ChannelMode processes requests sequentially, so throughput is limited by AMQP network latency
- For maximum throughput, consider running multiple Publisher instances (each with its own dedicated goroutine)
- Connection pooling is intentionally avoided due to AMQP's lack of thread-safety

## Requirements

- Go 1.24.0 or higher
- RabbitMQ or another AMQP 0.9.1 compatible broker
- Dependencies:
  - `github.com/rabbitmq/amqp091-go`
  - `github.com/google/uuid`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

[MIT License](LICENSE) - see the LICENSE file for details

## Acknowledgments

- Celery Project for the message protocol specification
- RabbitMQ team for the excellent AMQP Go client

## Support

If you encounter any issues or have questions, please file an issue on the GitHub issue tracker.
