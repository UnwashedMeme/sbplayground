package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	"github.com/gofrs/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/tomb.v2"
)

var (
	sessionCount       = 10
	receiverCount      = 3
	msgdelaystddev     = 30.0
	stopIfNothingFor   = 15
	msgCountUpperBound = 8
)

// Create a new session with uuid identifier, random count and delay
func sendSession(ctx context.Context, q *servicebus.Queue) error {
	sessionuuid, err := uuid.NewV4()
	if err != nil {
		return err
	}
	sessionid := sessionuuid.String()
	log := log.With().Str("SID", sessionid).Logger()
	msgcount := rand.Intn(msgCountUpperBound) + 1

	d := time.Duration(math.Abs(rand.NormFloat64()*msgdelaystddev)) * time.Second
	for i := 1; i <= msgcount; i++ {

		t := time.NewTimer(d)
		select {
		case <-t.C:
			var msg *servicebus.Message
			if i < msgcount {
				msg = servicebus.NewMessageFromString(fmt.Sprintf("Message %v/%v", i, msgcount))
			} else {
				msg = servicebus.NewMessageFromString("Shutdown")
			}
			msg.SessionID = &sessionid
			if err := q.Send(ctx, msg); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Err(err).Msg("q.Send()")
				}
				return err
			}
		case <-ctx.Done():
			if !t.Stop() {
				log.Debug().Msg("Draining sendTimer")
				<-t.C
			}
			return ctx.Err()
		}

	}
	log.Info().Msgf("Finished sending %v", msgcount)
	return nil
}


type SimpleSessionHandler struct {
	messageSession *servicebus.MessageSession
	SessionID      *string
	log            zerolog.Logger
}

// Start is called when a new session is started
func (ssh *SimpleSessionHandler) Start(ms *servicebus.MessageSession) error {
	ssh.messageSession = ms
	ssh.SessionID = ms.SessionID() // This is always nil right now unless we patch session.go
	ssh.log.Info().Msg("Begin SimpleSessionHandler")
	return nil
}

// Handle is called when a new session message is received
func (ssh *SimpleSessionHandler) Handle(ctx context.Context, msg *servicebus.Message) error {

	log := ssh.log
	if ssh.SessionID == nil {
		ssh.SessionID = msg.SessionID
	}
	remaininglock := time.Until(ssh.messageSession.LockedUntil())
	logd := log.With().Str("remaininglock", remaininglock.String()).Logger()
	logd.Info().Msgf("msg=\"%s\"", string(msg.Data))

	//if we don't hold the lock renew it before handling the message
	if ssh.messageSession.LockedUntil().Before(time.Now()) {
		if err := ssh.messageSession.RenewLock(ctx); err != nil {
			log.Err(err).Msgf("Renewlock() failed")
			return err
		}
	}

	// if ssh.messageSession.LockedUntil().Before(time.Now()) {
	// 	log.Info().Msgf("Closing expired session")
	// 	ssh.messageSession.Close()
	// }
	if strings.Contains(string(msg.Data), "Shutdown") {
		ssh.messageSession.Close()
	}

	return msg.Complete(ctx)
}

func (ssh *SimpleSessionHandler) End() {
	ssh.log.Info().Msg("End SimpleSessionHandler")
}

type MonitoredSessionHandler struct {
	SimpleSessionHandler
	// cancel           context.CancelFunc
	// ctx              context.Context
	lasthandled      time.Time
	stopIfNothingFor time.Duration
}

func (msh *MonitoredSessionHandler) Start(ms *servicebus.MessageSession) error {
	if ms.SessionID() != nil {
		//will be nil unless you patch session.go
		msh.log = msh.log.With().Str("SID", *ms.SessionID()).Logger()
	} else {
		msh.log.Warn().Msg("Start MSH with no SessionID")
	}

	return msh.SimpleSessionHandler.Start(ms)
}

func (msh *MonitoredSessionHandler) Handle(ctx context.Context, msg *servicebus.Message) error {
	if err := msh.SimpleSessionHandler.Handle(ctx, msg); err != nil {
		return err
	}
	msh.lasthandled = time.Now()
	return nil
}

func (msh *MonitoredSessionHandler) AutoRenew(ctx context.Context, cancel context.CancelFunc) error {
	log := msh.log
	lockRenewTimer := time.NewTicker(5 * time.Second)
	noNewMessagesTimer := time.NewTimer(msh.stopIfNothingFor)
	rounder := time.Second
	for {
		select {
		case <-lockRenewTimer.C:
			if msh.messageSession != nil && msh.messageSession.SessionID() != nil {
				if err := msh.messageSession.RenewLock(ctx); err != nil {
					log.Err(err).Msgf("Renewlock() failed")
					msh.messageSession.Close()
					return err
				} else {
					remaininglock := time.Until(msh.messageSession.LockedUntil())
					logd := log.With().Str("remaininglock", remaininglock.String()).Logger()
					logd.Debug().Msg("lock renewed")
				}
				lockduration := time.Until(msh.messageSession.LockedUntil())
				//log.Debug().Msgf("%v renewed lock, good for another %v", msh, lockduration)
				lockRenewTimer.Reset(lockduration.Truncate(rounder))
			}
		case <-noNewMessagesTimer.C:
			d := time.Until(msh.lasthandled.Add(msh.stopIfNothingFor))
			//d := msh.lasthandled.Add(msh.stopIfNothingFor).Sub(time.Now())
			if d.Seconds() < 0 {
				log.Info().Msg("No new messages, closing MonitoredSessionHandler")
				cancel()
			} else {
				noNewMessagesTimer.Reset(d)
			}
		case <-ctx.Done():
			log.Info().Msg("stopping autorenew loop")
			lockRenewTimer.Stop()
			if !noNewMessagesTimer.Stop() {
				<-noNewMessagesTimer.C
			}
			return ctx.Err()
		}
	}
}

func receiveWorker(ctx context.Context, queue *servicebus.Queue) func() error {
	return func() error {
		log := zerolog.Ctx(ctx)
		for ctx.Err() != nil {
			queueSession := queue.NewSession(nil)
			ssh := &SimpleSessionHandler{
				log: *log,
			}
			log.Debug().Msg("queueSession.ReceiveOne()")
			if err := queueSession.ReceiveOne(ctx, ssh); err != nil && !errors.Is(err, context.Canceled) {
				log.Err(err).Msg("queueSession.ReceiveOne() failed")
				return err
			}
			if err := queueSession.Close(ctx); err != nil {
				log.Err(err).Msg("queueSession.Close() failed")
				return err
			}
		}
		return ctx.Err()
	}
}

// A worker that reuses the same queueSession object
func reuseSessionWorker(ctx context.Context, queue *servicebus.Queue) func() error {
	return func() error {
		log := ctx.Value("log").(zerolog.Logger)
		queueSession := queue.NewSession(nil)
		defer func() {
			if err := queueSession.Close(context.TODO()); err != nil {
				log.Err(err).Msg("queueSession.Close() failed")
			}
		}()

		for {
			log.Debug().Msgf("queueSession.ReceiveOne()")
			ssh := &SimpleSessionHandler{}
			if err := queueSession.ReceiveOne(ctx, ssh); err != nil {
				log.Err(err).Msgf("queueSession.ReceiveOne()")
				return err
			}
		}
		return ctx.Err()
	}
}

// Handle an individual queueSession by calling ReceiveOne, with autorenew monitoring
func newMonitoredSessionHandler(ctx context.Context, queueSession *servicebus.QueueSession) error {
	log := zerolog.Ctx(ctx)

	log.Debug().Msgf("queueSession.ReceiveOne()")
	mshctx, cancel := context.WithCancel(ctx)
	defer cancel()
	msh := &MonitoredSessionHandler{
		SimpleSessionHandler: SimpleSessionHandler{
			log: log.With().Logger(),
		},
		stopIfNothingFor: time.Duration(stopIfNothingFor) * time.Second,
		lasthandled:      time.Now(),
	}
	go msh.AutoRenew(mshctx, cancel)
	err := queueSession.ReceiveOne(mshctx, msh)
	if err != nil {
		log.Err(err).Msgf("queueSession.ReceiveOne() failed")
	}
	return err
}

// a worker that is a MonitoredSessionHandler with lock autorenew
func monitoredWorker(ctx context.Context, queue *servicebus.Queue) func() error {
	return func() error {
		log := zerolog.Ctx(ctx)
		for ctx.Err() == nil {
			queueSession := queue.NewSession(nil)
			err := newMonitoredSessionHandler(ctx, queueSession)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Err(err).Msg("Failed")
			}
			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			defer cancel()
			if err := queueSession.Close(ctx); err != nil {
				log.Err(err).Msg("queueSession.Close() failed")
			}
		}
		return ctx.Err()
	}
}

// Reuse the queueSession object; this doesn't seem to work. the 2nd
// time ReceiveOne is called it returns immediately with context already
// cancelled.
func monReuseWorker(ctx context.Context, queue *servicebus.Queue) func() error {
	return func() error {
		log := zerolog.Ctx(ctx)
		queueSession := queue.NewSession(nil)
		defer func() {
			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			defer cancel()
			if err := queueSession.Close(ctx); err != nil {
				log.Err(err).Msg("queueSession.Close() failed")
			}
		}()
		for ctx.Err() == nil {
			err := newMonitoredSessionHandler(ctx, queueSession)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Err(err).Msg("Failed")
				return err
			}
		}
		return ctx.Err()
	}
}


func example_sessions(ctx context.Context) error {
	t, ctx := tomb.WithContext(ctx)

	connStr := os.Getenv("SERVICEBUS_CONNECTION_STRING")
	if connStr == "" {
		return fmt.Errorf("FATAL: expected environment variable SERVICEBUS_CONNECTION_STRING not set")
	}

	// Create a client to communicate with a Service Bus Namespace.
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connStr))
	if err != nil {
		return err
	}

	q, err := ns.NewQueue("fooshort", servicebus.QueueWithPrefetchCount(4))
	//q, err := ns.NewQueue("fooshort")
	if err != nil {
		return err
	}
	log.Debug().Msgf("Starting %v receivers", receiverCount)
	for i := 1; i <= receiverCount; i++ {
		log := log.With().Int("Worker", i).Logger()
		ctx := log.WithContext(ctx)
		t.Go(monitoredWorker(ctx, q))
		//t.Go(monReuseWorker(ctx, q))
		//t.Go(receiveWorker(ctx, q))
	}
	log.Debug().Msgf("Starting %v sessions", sessionCount)
	for i := 0; i < sessionCount; i++ {
		t.Go(func() error {
			return sendSession(ctx, q)
		})
	}
	log.Info().Msg("Waiting for everything to finish")
	return t.Wait()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("%-32s", i)
	}

	log.Logger = zerolog.New(output).With().Timestamp().Caller().Int("PID", os.Getpid()).Logger()
	ctx = log.Logger.WithContext(ctx)

	go func() {
		<-ctx.Done()
		log.Error().Msg("Received done from signal context")
	}()

	if err := example_sessions(ctx); err != nil {
		log.Err(err).Msg("Exiting Main")
	}
}
