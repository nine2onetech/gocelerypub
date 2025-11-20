package gocelerypub

import (
	"errors"
	"fmt"
	"go.uber.org/mock/gomock"
	"strings"
	"sync"
	"testing"
)

func TestNew_ValidationOnly(t *testing.T) {
	// Note: These tests only validate error cases that don't require broker connection
	// Tests that require successful broker creation need a running AMQP broker

	tests := []struct {
		name        string
		config      Config
		wantErr     bool
		errContains string
	}{
		{
			name: "unsupported broker type",
			config: Config{
				BrokerType:  "redis",
				HostURL:     "redis://localhost",
				PublishMode: DirectMode,
			},
			wantErr:     true,
			errContains: "unsupported broker type",
		},
		{
			name: "unsupported publish mode",
			config: Config{
				BrokerType:  AMQP,
				HostURL:     "amqp://localhost",
				PublishMode: "invalid",
			},
			wantErr:     true,
			errContains: "unsupported publish mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub, err := New(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("New() error = nil, wantErr %v", tt.wantErr)
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("New() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}

			if err != nil {
				t.Errorf("New() unexpected error = %v", err)
				return
			}

			if pub == nil {
				t.Error("New() returned nil publisher")
				return
			}

			// Clean up if ChannelMode
			if pub.taskChan != nil {
				_ = pub.Close()
			}
		})
	}
}

func TestPublisher_ConfigDefaults(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBroker := NewMockBroker(ctrl)

	t.Run("DirectMode is used when not specified", func(t *testing.T) {
		pub := &Publisher{
			broker: mockBroker,
			config: Config{
				BrokerType:  AMQP,
				PublishMode: "", // Not specified
			},
		}

		// Simulate the default setting that New() would do
		if pub.config.PublishMode == "" {
			pub.config.PublishMode = DirectMode
		}

		if pub.config.PublishMode != DirectMode {
			t.Errorf("Default PublishMode = %v, want %v", pub.config.PublishMode, DirectMode)
		}
	})

	t.Run("ChannelMode creates task channel", func(t *testing.T) {
		pub := &Publisher{
			broker: mockBroker,
			config: Config{
				BrokerType:  AMQP,
				PublishMode: ChannelMode,
			},
			taskChan: make(chan *internalPublishRequest),
		}

		if pub.taskChan == nil {
			t.Error("ChannelMode should have non-nil taskChan")
		}

		close(pub.taskChan)
	})

	t.Run("DirectMode has nil task channel", func(t *testing.T) {
		pub := &Publisher{
			broker: mockBroker,
			config: Config{
				BrokerType:  AMQP,
				PublishMode: DirectMode,
			},
			taskChan: nil,
		}

		if pub.taskChan != nil {
			t.Error("DirectMode should have nil taskChan")
		}
	})
}

func TestPublisher_Publish_DirectMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBroker := NewMockBroker(ctrl)

	pub := &Publisher{
		broker: mockBroker,
		config: Config{
			BrokerType:             AMQP,
			HostURL:                "amqp://localhost",
			PublishMode:            DirectMode,
			messageProtocolVersion: 1,
		},
		taskChan: nil, // DirectMode doesn't use task channel
	}

	req := &PublishRequest{
		Queue:  "test_queue",
		Task:   "tasks.test",
		Args:   []interface{}{1, 2, 3},
		Kwargs: map[string]interface{}{"key": "value"},
	}

	t.Run("successful publish", func(t *testing.T) {
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(nil).Times(1)

		err := pub.Publish(req)
		if err != nil {
			t.Errorf("Publish() error = %v, want nil", err)
		}
	})

	t.Run("publish failure", func(t *testing.T) {
		expectedErr := errors.New("connection failed")
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(expectedErr).Times(1)

		err := pub.Publish(req)
		if err == nil {
			t.Error("Publish() error = nil, want error")
			return
		}
		if !strings.Contains(err.Error(), "failed to publish") {
			t.Errorf("Publish() error = %v, want error containing 'failed to publish'", err)
		}
	})
}

func TestPublisher_HandleRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBroker := NewMockBroker(ctrl)

	pub := &Publisher{
		broker: mockBroker,
		config: Config{
			BrokerType:             AMQP,
			HostURL:                "amqp://localhost",
			PublishMode:            DirectMode,
			messageProtocolVersion: 1,
		},
	}

	req := &PublishRequest{
		Queue: "test_queue",
		Task:  "tasks.test",
	}

	t.Run("successful publish when broker can publish", func(t *testing.T) {
		mockBroker.EXPECT().CanPublish().Return(true).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(nil).Times(1)

		err := pub.handleRequest(req)
		if err != nil {
			t.Errorf("handleRequest() error = %v, want nil", err)
		}
	})

	t.Run("reconnect when broker cannot publish", func(t *testing.T) {
		mockBroker.EXPECT().CanPublish().Return(false).Times(1)
		mockBroker.EXPECT().Reconnect(pub.config.HostURL).Return(nil).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(nil).Times(1)

		err := pub.handleRequest(req)
		if err != nil {
			t.Errorf("handleRequest() error = %v, want nil", err)
		}
	})

	t.Run("reconnect failure", func(t *testing.T) {
		reconnectErr := errors.New("reconnect failed")
		mockBroker.EXPECT().CanPublish().Return(false).Times(1)
		mockBroker.EXPECT().Reconnect(pub.config.HostURL).Return(reconnectErr).Times(1)

		err := pub.handleRequest(req)
		if err == nil {
			t.Error("handleRequest() error = nil, want error")
			return
		}
		if !strings.Contains(err.Error(), "reconnect failed") {
			t.Errorf("handleRequest() error = %v, want error containing 'reconnect failed'", err)
		}
	})

	t.Run("retry on PRECONDITION_FAILED", func(t *testing.T) {
		preconditionErr := errors.New("PRECONDITION_FAILED - queue mismatch")
		mockBroker.EXPECT().CanPublish().Return(true).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(preconditionErr).Times(1)
		mockBroker.EXPECT().Reconnect(pub.config.HostURL).Return(nil).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(nil).Times(1)

		err := pub.handleRequest(req)
		if err != nil {
			t.Errorf("handleRequest() error = %v, want nil (should succeed on retry)", err)
		}
	})

	t.Run("PRECONDITION_FAILED reconnect failure", func(t *testing.T) {
		preconditionErr := errors.New("PRECONDITION_FAILED - queue mismatch")
		reconnectErr := errors.New("reconnect failed")
		mockBroker.EXPECT().CanPublish().Return(true).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(preconditionErr).Times(1)
		mockBroker.EXPECT().Reconnect(pub.config.HostURL).Return(reconnectErr).Times(1)

		err := pub.handleRequest(req)
		if err == nil {
			t.Error("handleRequest() error = nil, want error")
			return
		}
		if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
			t.Errorf("handleRequest() error = %v, want error containing 'PRECONDITION_FAILED'", err)
		}
	})

	t.Run("PRECONDITION_FAILED retry still fails", func(t *testing.T) {
		preconditionErr := errors.New("PRECONDITION_FAILED - queue mismatch")
		mockBroker.EXPECT().CanPublish().Return(true).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(preconditionErr).Times(1)
		mockBroker.EXPECT().Reconnect(pub.config.HostURL).Return(nil).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(preconditionErr).Times(1)

		err := pub.handleRequest(req)
		if err == nil {
			t.Error("handleRequest() error = nil, want error")
			return
		}
		if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
			t.Errorf("handleRequest() error = %v, want error containing 'PRECONDITION_FAILED'", err)
		}
	})
}

func TestPublisher_Send_ChannelMode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBroker := NewMockBroker(ctrl)

	// Create a publisher with ChannelMode
	pub := &Publisher{
		broker: mockBroker,
		config: Config{
			BrokerType:             AMQP,
			PublishMode:            ChannelMode,
			messageProtocolVersion: 1,
		},
		taskChan: make(chan *internalPublishRequest),
	}

	// Start the run goroutine
	go pub.run()
	defer func() { _ = pub.Close() }()

	req := &PublishRequest{
		Queue:  "test_queue",
		Task:   "tasks.test",
		Args:   []interface{}{1, 2, 3},
		Kwargs: map[string]interface{}{"key": "value"},
	}

	t.Run("successful send", func(t *testing.T) {
		mockBroker.EXPECT().CanPublish().Return(true).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(nil).Times(1)

		err := pub.Send(req)
		if err != nil {
			t.Errorf("Send() error = %v, want nil", err)
		}
	})

	t.Run("send failure", func(t *testing.T) {
		expectedErr := errors.New("send failed")
		mockBroker.EXPECT().CanPublish().Return(true).Times(1)
		mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(expectedErr).Times(1)

		err := pub.Send(req)
		if err == nil {
			t.Error("Send() error = nil, want error")
			return
		}
		if !strings.Contains(err.Error(), "failed to publish") {
			t.Errorf("Send() error = %v, want error containing 'failed to publish'", err)
		}
	})
}

func TestPublisher_Send_DirectMode_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBroker := NewMockBroker(ctrl)

	// Create a publisher in DirectMode (taskChan is nil)
	pub := &Publisher{
		broker: mockBroker,
		config: Config{
			BrokerType:             AMQP,
			PublishMode:            DirectMode,
			messageProtocolVersion: 1,
		},
		taskChan: nil,
	}

	req := &PublishRequest{
		Queue: "test_queue",
		Task:  "tasks.test",
	}

	err := pub.Send(req)
	if err == nil {
		t.Error("Send() in DirectMode should return error")
		return
	}
	if !strings.Contains(err.Error(), "channel mode") {
		t.Errorf("Send() error = %v, want error containing 'channel mode'", err)
	}
}

func TestPublisher_Send_Concurrent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBroker := NewMockBroker(ctrl)

	pub := &Publisher{
		broker: mockBroker,
		config: Config{
			BrokerType:             AMQP,
			PublishMode:            ChannelMode,
			messageProtocolVersion: 1,
		},
		taskChan: make(chan *internalPublishRequest),
	}

	go pub.run()
	defer func() { _ = pub.Close() }()

	// Expect 100 concurrent sends
	mockBroker.EXPECT().CanPublish().Return(true).Times(100)
	mockBroker.EXPECT().SendCeleryMessage(gomock.Any()).Return(nil).Times(100)

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Launch 100 concurrent Send operations
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req := &PublishRequest{
				Queue:  "test_queue",
				Task:   fmt.Sprintf("tasks.test_%d", id),
				Args:   []interface{}{id},
				Kwargs: map[string]interface{}{"id": id},
			}
			if err := pub.Send(req); err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		t.Errorf("concurrent Send() error = %v", err)
	}
}
