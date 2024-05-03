package google

import (
	"context"
	"os"
	"testing"

	gcPubSub "cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"gofr.dev/pkg/gofr/testutil"
)

func getGoogleClient(t *testing.T) *gcPubSub.Client {
	srv := pstest.NewServer()

	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Errorf("could not initialize a connection to dummy server")
	}

	client, err := gcPubSub.NewClient(context.Background(), "project", option.WithGRPCConn(conn))
	if err != nil {
		t.Errorf("could not initialize a test client")
	}

	return client
}

func TestGoogleClient_New(t *testing.T) {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8681")
	defer os.Unsetenv("PUBSUB_EMULATOR_HOST")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := testutil.NewMockLogger(testutil.ERRORLOG)
	var g *googleClient

	g = New(Config{ProjectID: "test123", SubscriptionName: "test"}, logger, NewMockMetrics(ctrl))

	assert.NotNil(t, g, "TestGoogleClient_New Failed!")
}

func TestGoogleClient_NewError(t *testing.T) {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8681")
	defer os.Unsetenv("PUBSUB_EMULATOR_HOST")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := testutil.NewMockLogger(testutil.ERRORLOG)
	var g *googleClient

	tests := []struct {
		desc      string
		config    Config
		outputLog string
	}{
		{desc: "Invalid Config", config: Config{ProjectID: "invalid", SubscriptionName: "invalid"},
			outputLog: ""},
		{desc: "Empty Config", config: Config{}, outputLog: "google pubsub could not be configured, err:"},
	}

	for i, tc := range tests {
		g = New(tc.config, logger, NewMockMetrics(ctrl))

		assert.Nil(t, g, "TEST[%d] Failed!\n", i)
	}
}

func TestGoogleClient_Publish_Success(t *testing.T) {
	client := getGoogleClient(t)
	defer client.Close()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetrics := NewMockMetrics(ctrl)

	topic := "test-topic"
	message := []byte("test message")

	out := testutil.StdoutOutputForFunc(func() {
		g := &googleClient{
			logger: testutil.NewMockLogger(testutil.DEBUGLOG),
			client: client,
			Config: Config{
				ProjectID:        "test",
				SubscriptionName: "sub",
			},
			metrics: mockMetrics,
		}

		mockMetrics.EXPECT().IncrementCounter(gomock.Any(), "app_pubsub_publish_total_count", "topic", topic)
		mockMetrics.EXPECT().IncrementCounter(gomock.Any(), "app_pubsub_publish_success_count", "topic", topic)

		err := g.Publish(context.Background(), topic, message)

		assert.Nil(t, err)
	})

	assert.Contains(t, out, "PUB")
	assert.Contains(t, out, "test message")
	assert.Contains(t, out, "test-topic")
	assert.Contains(t, out, "test")
	assert.Contains(t, out, "GCP")
}

func TestGoogleClient_PublishTopic_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMetrics := NewMockMetrics(ctrl)

	g := &googleClient{client: getGoogleClient(t), Config: Config{
		ProjectID:        "test",
		SubscriptionName: "sub",
	}, metrics: mockMetrics, logger: testutil.NewMockLogger(testutil.DEBUGLOG)}
	defer g.client.Close()

	ctx, cancel := context.WithCancel(context.Background())

	cancel()

	mockMetrics.EXPECT().IncrementCounter(gomock.Any(), "app_pubsub_publish_total_count", "topic", "test-topic")

	err := g.Publish(ctx, "test-topic", []byte(""))
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "context canceled")
	}
}

func TestGoogleClient_getTopic_Success(t *testing.T) {
	g := &googleClient{client: getGoogleClient(t), Config: Config{
		ProjectID:        "test",
		SubscriptionName: "sub",
	}}
	defer g.client.Close()

	topic, err := g.getTopic(context.Background(), "test-topic")

	assert.Nil(t, err)
	assert.Equal(t, topic.ID(), "test-topic")
}

func TestGoogleClient_getTopic_Error(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	g := &googleClient{client: getGoogleClient(t), Config: Config{
		ProjectID:        "test",
		SubscriptionName: "sub",
	}}
	defer g.client.Close()

	topic, err := g.getTopic(ctx, "test-topic")

	assert.Nil(t, topic)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestGoogleClient_getSubscription(t *testing.T) {
	g := &googleClient{client: getGoogleClient(t), Config: Config{
		ProjectID:        "test",
		SubscriptionName: "sub",
	}}
	defer g.client.Close()

	topic, _ := g.client.CreateTopic(context.Background(), "test-topic")

	sub, err := g.getSubscription(context.Background(), topic)

	assert.Nil(t, err)
	assert.NotNil(t, sub)
}

func Test_validateConfigs(t *testing.T) {
	testCases := []struct {
		desc   string
		input  *Config
		expErr error
	}{
		{desc: "project id not provided", input: &Config{}, expErr: errProjectIDNotProvided},
		{desc: "subscription not provided", input: &Config{ProjectID: "test"}, expErr: errSubscriptionNotProvided},
		{desc: "success", input: &Config{ProjectID: "test", SubscriptionName: "subs"}, expErr: nil},
	}

	for _, tc := range testCases {
		err := validateConfigs(tc.input)

		assert.Equal(t, tc.expErr, err)
	}
}

func TestGoogleClient_CreateAndDeleteTopic(t *testing.T) {
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8681")
	defer os.Unsetenv("PUBSUB_EMULATOR_HOST")

	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := testutil.NewMockLogger(testutil.ERRORLOG)
	mockMetrics := NewMockMetrics(ctrl)

	g := New(Config{
		ProjectID:        "test123",
		SubscriptionName: "test",
	}, logger, mockMetrics)

	err := g.CreateTopic(ctx, "test-topic")
	assert.Nil(t, err)

	err = g.DeleteTopic(ctx, "test-topic")
	assert.Nil(t, err)
}
