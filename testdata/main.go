package main

import (
	"fmt"
	"log"
	"time"

	publisher "github.com/nine2onetech/gocelerypub"
)

func main() {
	fmt.Println("=== Go Celery Publisher Integration Test ===")
	fmt.Println()

	// Create publisher
	cfg := publisher.Config{
		BrokerType: publisher.AMQP,
		HostURL:    "amqp://guest:guest@localhost:5672/",
	}

	pub, err := publisher.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}

	fmt.Println("✅ Publisher created successfully")
	fmt.Println()

	// Test 1: tasks.add with args
	fmt.Println("Test 1: Publishing tasks.add with args [10, 20, 30]")
	err = pub.Publish(&publisher.PublishRequest{
		Queue: "celery",
		Task:  "tasks.add",
		Args:  []interface{}{10, 20, 30},
	})
	if err != nil {
		log.Printf("❌ Failed to publish add task: %v", err)
	} else {
		fmt.Println("✅ Published tasks.add")
	}
	time.Sleep(1 * time.Second)

	// Test 2: tasks.process_data with kwargs
	fmt.Println()
	fmt.Println("Test 2: Publishing tasks.process_data with kwargs")
	err = pub.Publish(&publisher.PublishRequest{
		Queue: "celery",
		Task:  "tasks.process_data",
		Kwargs: map[string]interface{}{
			"user_id": 123,
			"action":  "update",
			"data":    "sample data",
		},
	})
	if err != nil {
		log.Printf("❌ Failed to publish process_data task: %v", err)
	} else {
		fmt.Println("✅ Published tasks.process_data")
	}
	time.Sleep(1 * time.Second)

	// Test 3: tasks.complex_operation with both args and kwargs
	fmt.Println()
	fmt.Println("Test 3: Publishing tasks.complex_operation with args and kwargs")
	err = pub.Publish(&publisher.PublishRequest{
		Queue: "celery",
		Task:  "tasks.complex_operation",
		Args:  []interface{}{"data", 42},
		Kwargs: map[string]interface{}{
			"priority": 5,
			"retry":    false,
		},
	})
	if err != nil {
		log.Printf("❌ Failed to publish complex_operation task: %v", err)
	} else {
		fmt.Println("✅ Published tasks.complex_operation")
	}
	time.Sleep(1 * time.Second)

	// Test 4: tasks.batch_process
	fmt.Println()
	fmt.Println("Test 4: Publishing tasks.batch_process")
	err = pub.Publish(&publisher.PublishRequest{
		Queue: "celery",
		Task:  "tasks.batch_process",
		Args:  []interface{}{"item1", "item2", "item3"},
		Kwargs: map[string]interface{}{
			"batch_id": "batch-001",
		},
	})
	if err != nil {
		log.Printf("❌ Failed to publish batch_process task: %v", err)
	} else {
		fmt.Println("✅ Published tasks.batch_process")
	}

	fmt.Println()
	fmt.Println("=== All tasks published ===")
	fmt.Println("Check the Celery worker logs to verify task execution")
}
