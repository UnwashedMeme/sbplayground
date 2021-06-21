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

const (
	// all times in in seconds
	sessionCount         = 3
	receiverCount        = 2
	msgdelaystddev       = 3.0
	initialAutoRenewPoll = 10
	noNewSessionsTimeout = 20
	msgCountUpperBound   = 8
	qPrefetchCount       = 5
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

func (ssh *SimpleSessionHandler) End() {
	ssh.log.Info().Msg("End SimpleSessionHandler")
}

// Handle is called when a new session message is received
func (ssh *SimpleSessionHandler) Handle(ctx context.Context, msg *servicebus.Message) error {

	log := ssh.log
	if ssh.SessionID == nil {
		ssh.SessionID = msg.SessionID
	}
	//if we don't hold the lock renew it before handling the message
	lockdate := ssh.messageSession.LockedUntil()
	if lockdate.Before(time.Now()) {
		log.Debug().Str("lockremaining", time.Until(lockdate).String()).Msg("manual renew on message")
		if err := ssh.messageSession.RenewLock(ctx); err != nil {
			log.Err(err).Msgf("Renewlock() failed")
			return err
		}
	}

	remaininglock := time.Until(ssh.messageSession.LockedUntil())
	log = log.With().Str("remaininglock", remaininglock.String()).Logger()
	log.Info().Msgf("msg=\"%s\"", string(msg.Data))

	if strings.Contains(string(msg.Data), "Shutdown") {
		defer ssh.messageSession.Close()
	}
	return msg.Complete(ctx)
}

type MonitoredSessionHandler struct {
	SimpleSessionHandler
	// cancel           context.CancelFunc
	// ctx              context.Context
	lasthandled time.Time
}

func (msh *MonitoredSessionHandler) Start(ms *servicebus.MessageSession) error {
	if ms.SessionID() != nil {
		msh.log.UpdateContext(func(c zerolog.Context) zerolog.Context {
			return c.Str("SID", *ms.SessionID())
		})
	} else {
		return fmt.Errorf("nil sessionid")
	}
	msh.lasthandled = time.Now()
	return msh.SimpleSessionHandler.Start(ms)
}

func (msh *MonitoredSessionHandler) End() {
	msh.log.Info().Msg("MonitoredSessionHandler shutdown")
}

func (msh *MonitoredSessionHandler) Handle(ctx context.Context, msg *servicebus.Message) error {
	if err := msh.SimpleSessionHandler.Handle(ctx, msg); err != nil {
		return err
	}
	msh.lasthandled = time.Now()
	return nil
}

func (msh *MonitoredSessionHandler) AutoRenew(ctx context.Context) error {
	lockRenewTimer := time.NewTimer(time.Second * initialAutoRenewPoll)
	noNewMessagesTimer := time.NewTimer(time.Second * noNewSessionsTimeout)
	for {
		select {
		case <-lockRenewTimer.C:
			msh.log.Debug().Msg("Renewing lock")
			if msh.messageSession != nil {
				if err := msh.messageSession.RenewLock(ctx); err != nil {
					msh.log.Err(err).Msgf("Renewlock() failed")
					msh.messageSession.Close()
					return err
				}
				lockRenewTimer.Reset(time.Until(msh.messageSession.LockedUntil()))
			} else {
				lockRenewTimer.Reset(time.Second * initialAutoRenewPoll)
			}

		case <-noNewMessagesTimer.C:
			if msh.messageSession == nil {
				msh.log.Warn().Msg("No session started")
				return fmt.Errorf("no new session")
			}
			// In case the `Shutdown` message never got sent
			d := time.Until(msh.lasthandled.Add(noNewSessionsTimeout * time.Second))
			if d.Seconds() < 0 {
				msh.log.Info().Msg("No new messages, closing MonitoredSessionHandler")
				msh.messageSession.Close()

			} else {
				noNewMessagesTimer.Reset(d)
			}
		case <-ctx.Done():
			if !noNewMessagesTimer.Stop() {
				<-noNewMessagesTimer.C
			}
			if !lockRenewTimer.Stop() {
				<-lockRenewTimer.C
			}
			return ctx.Err()
		}
	}
}

// A basic worker
//lint:ignore U1000 I swap out which functions I want to use
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
//lint:ignore U1000 I swap out which functions I want to use
func reuseSessionWorker(ctx context.Context, queue *servicebus.Queue) func() error {
	return func() error {
		log := ctx.Value("log").(zerolog.Logger)
		queueSession := queue.NewSession(nil)
		defer func() {
			if err := queueSession.Close(context.TODO()); err != nil {
				log.Err(err).Msg("queueSession.Close() failed")
			}
		}()

		for ctx.Err() != nil {
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

	tomb, mshctx := tomb.WithContext(ctx)
	defer tomb.Kill(context.Canceled)
	msh := &MonitoredSessionHandler{
		SimpleSessionHandler: SimpleSessionHandler{
			log: log.With().Logger(),
		},
	}
	tomb.Go(func() error { return msh.AutoRenew(mshctx) })
	log.Debug().Msgf("queueSession.ReceiveOne()")
	return queueSession.ReceiveOne(mshctx, msh)

}

// a worker that is a MonitoredSessionHandler with lock autorenew
//lint:ignore U1000 I swap out which functions I want to use
func monitoredWorker(ctx context.Context, queue *servicebus.Queue) func() error {
	return func() error {
		log := zerolog.Ctx(ctx)
		for ctx.Err() == nil {
			//log.Debug().Msg("monitoredWorker top of loop")
			queueSession := queue.NewSession(nil)
			err := newMonitoredSessionHandler(ctx, queueSession)

			ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
			defer cancel()
			if err := queueSession.Close(ctx); err != nil {
				log.Err(err).Msg("queueSession.Close() failed")
			}

			if err != nil {
				log.Err(err).Msg("Failed")
				return err
			}

		}
		return ctx.Err()
	}
}

//lint:ignore U1000 I swap out which functions I want to use
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
			if err != nil {
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

	q, err := ns.NewQueue("fooshort", servicebus.QueueWithPrefetchCount(qPrefetchCount))
	//q, err := ns.NewQueue("fooshort")
	if err != nil {
		return err
	}
	log.Debug().Msgf("Starting %v receivers", receiverCount)
	for i := 1; i <= receiverCount; i++ {
		log := log.With().Int("Worker", i).Logger()
		ctx := log.WithContext(ctx)
		//t.Go(monitoredWorker(ctx, q))
		t.Go(monReuseWorker(ctx, q))
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
