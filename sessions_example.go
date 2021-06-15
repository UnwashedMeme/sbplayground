package main

import (
	"context"
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
	sessionCount     = 100
	receiverCount    = 2
	msgdelaystddev   = 120.0
	stopIfNothingFor = 30
)

func sendSession(ctx context.Context, q *servicebus.Queue) error {
	sessionuuid, err := uuid.NewV4()
	if err != nil {
		return err
	}
	sessionid := sessionuuid.String()
	log := log.With().Str("id", sessionid).Logger()
	msgcount := rand.Intn(10) + 1

	d := time.Duration(math.Abs(rand.NormFloat64()*msgdelaystddev)) * time.Second
	t := time.NewTimer(d)

	for i := 1; i <= msgcount; i++ {
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
				log.Err(err).Msg("q.Send()")
				return err
			}
			t.Reset(d)
		case <-ctx.Done():
			return ctx.Err()
		}

	}
	log.Info().Msgf("Finished sending %v", msgcount)
	return nil
}

type SimpleSessionHandler struct {
	messageSession *servicebus.MessageSession
	SessionID      *string
	WorkerId       int
}

func (ssh *SimpleSessionHandler) String() string {
	var s string
	if ssh.SessionID == nil {
		s = "nil"
	} else {
		s = *ssh.SessionID
	}
	return fmt.Sprintf("%v/%v/%v", os.Getpid(), ssh.WorkerId, s)
}

// Start is called when a new session is started
func (ssh *SimpleSessionHandler) Start(ms *servicebus.MessageSession) error {
	ssh.messageSession = ms
	ssh.SessionID = ms.SessionID() // This is always nil

	log.Info().Msgf("%v: Begin SimpleSessionHandler", ssh)
	return nil
}

// Handle is called when a new session message is received
func (ssh *SimpleSessionHandler) Handle(ctx context.Context, msg *servicebus.Message) error {
	log := ctx.Value("log").(zerolog.Logger)
	if ssh.SessionID == nil {
		ssh.SessionID = msg.SessionID
	}
	log.Info().Msgf("%v: \"%s\", %v", ssh, msg.Data, ssh.messageSession.LockedUntil().Sub(time.Now()))
	// if we don't hold the lock renew it before handling the message
	if ssh.messageSession.LockedUntil().Before(time.Now()) {
		if err := ssh.messageSession.RenewLock(ctx); err != nil {
			log.Err(err).Msgf("%v: Renewlock() failed", ssh)
			return err
		}
	}

	if ssh.messageSession.LockedUntil().Before(time.Now()) {
		log.Info().Msgf("%v: Closing expired session", ssh)
		ssh.messageSession.Close()
	}
	if strings.Contains(string(msg.Data), "Shutdown") {
		ssh.messageSession.Close()
	}

	return msg.Complete(ctx)
}

func (ssh *SimpleSessionHandler) End() {
	log.Info().Msgf("%v: End SimpleSessionHandler", ssh)
}

type MonitoredSessionHandler struct {
	SimpleSessionHandler
	cancel           context.CancelFunc
	ctx              context.Context
	lasthandled      time.Time
	stopIfNothingFor time.Duration
}

func (msh *MonitoredSessionHandler) Start(ms *servicebus.MessageSession) error {
	if err := msh.SimpleSessionHandler.Start(ms); err != nil {
		return err
	}
	go msh.AutoRenew()
	return nil
}

func (msh *MonitoredSessionHandler) Handle(ctx context.Context, msg *servicebus.Message) error {
	if err := msh.SimpleSessionHandler.Handle(ctx, msg); err != nil {
		return err
	}
	msh.lasthandled = time.Now()
	return nil
}
func (msh *MonitoredSessionHandler) End() {
	msh.SimpleSessionHandler.End()
	msh.cancel()
}

func (msh *MonitoredSessionHandler) AutoRenew() error {
	log := msh.ctx.Value("log").(zerolog.Logger)
	lockRenewTimer := time.NewTicker(5 * time.Second)
	noNewMessagesTimer := time.NewTimer(msh.stopIfNothingFor)
	rounder := time.Second
	for {
		select {
		case <-lockRenewTimer.C:
			if msh.messageSession.SessionID() != nil {
				if err := msh.messageSession.RenewLock(msh.ctx); err != nil {
					log.Err(err).Msgf("%v: Renewlock() failed", msh)
					msh.messageSession.Close()
					return err
				}
				lockduration := msh.messageSession.LockedUntil().Sub(time.Now())
				//log.Debug().Msgf("%v renewed lock, good for another %v", msh, lockduration)
				lockRenewTimer.Reset(lockduration.Truncate(rounder))
			}
		case <-noNewMessagesTimer.C:
			d := msh.lasthandled.Add(msh.stopIfNothingFor).Sub(time.Now())
			if d.Seconds() < 0 {
				log.Info().Msgf("%v no new messages, closing session handler", msh)
				msh.messageSession.Close()
			} else {
				noNewMessagesTimer.Reset(d)
			}
		case <-msh.ctx.Done():
			log.Info().Msgf("%v stopping autorenew loop", msh)
			lockRenewTimer.Stop()
			return msh.ctx.Err()
		}
	}
}

func receiveWorker(ctx context.Context, workerNumber int, queue *servicebus.Queue) func() error {
	return func() error {
		log := ctx.Value("log").(zerolog.Logger)
		for {
			queueSession := queue.NewSession(nil)
			fmt.Printf("%v/%v: queueSession.ReceiveOne(%v)\n", os.Getpid(), workerNumber, queueSession.SessionID())
			ssh := SimpleSessionHandler{
				WorkerId: workerNumber,
			}
			if err := queueSession.ReceiveOne(ctx, &ssh); err != nil {
				log.Err(err).Msg("Whups")
				return err
			}
			fmt.Printf("%v/%v: queueSession.Close(%v)\n", os.Getpid(), workerNumber, queueSession.SessionID())
			if err := queueSession.Close(ctx); err != nil {
				log.Err(err).Msg("Whups")
				return err
			}
		}
		return nil
	}
}
func monitoredWorker(ctx context.Context, workerNumber int, queue *servicebus.Queue) func() error {
	return func() error {
		log := ctx.Value("log").(zerolog.Logger)

		for ctx.Err() == nil {
			queueSession := queue.NewSession(nil)
			log.Debug().Msgf("%v/%v: queueSession.ReceiveOne(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
			ctx, cancel := context.WithCancel(ctx)
			ssh := MonitoredSessionHandler{
				SimpleSessionHandler: SimpleSessionHandler{
					WorkerId: workerNumber,
				},
				ctx:              ctx,
				cancel:           cancel,
				stopIfNothingFor: time.Duration(stopIfNothingFor) * time.Second,
				lasthandled:      time.Now(),
			}
			if err := queueSession.ReceiveOne(ctx, &ssh); err != nil {
				log.Err(err).Msgf("%v/%v: queueSession.ReceiveOne(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
			}
			log.Debug().Msgf("%v/%v: queueSession.Close(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
			if err := queueSession.Close(ctx); err != nil {
				log.Err(err).Msgf("%v/%v: queueSession.Close(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
			}
		}
		return ctx.Err()
	}
}

func monReuseWorker(ctx context.Context, workerNumber int, queue *servicebus.Queue) func() error {
	return func() error {
		log := ctx.Value("log").(zerolog.Logger)
		queueSession := queue.NewSession(nil)
		for ctx.Err() == nil {
			log.Debug().Msgf("%v/%v: queueSession.ReceiveOne(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
			ctx, cancel := context.WithCancel(ctx)
			ssh := MonitoredSessionHandler{
				SimpleSessionHandler: SimpleSessionHandler{
					WorkerId: workerNumber,
				},
				ctx:              ctx,
				cancel:           cancel,
				stopIfNothingFor: 30 * time.Second,
			}
			if err := queueSession.ReceiveOne(ctx, &ssh); err != nil {
				log.Err(err).Msgf("%v/%v: queueSession.ReceiveOne(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
			}
		}
		log.Debug().Msgf("%v/%v: queueSession.Close(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
		if err := queueSession.Close(ctx); err != nil {
			log.Err(err).Msg("Error closing queue")
			return err
		}
		return ctx.Err()
	}
}

func reuseSessionWorker(ctx context.Context, workerNumber int, queue *servicebus.Queue) func() error {
	return func() error {
		log := ctx.Value("log").(zerolog.Logger)
		queueSession := queue.NewSession(nil)
		for {
			log.Debug().Msgf("%v/%v: queueSession.ReceiveOne(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
			ssh := SimpleSessionHandler{WorkerId: workerNumber}
			if err := queueSession.ReceiveOne(ctx, &ssh); err != nil {
				log.Err(err).Msgf("%v/%v: queueSession.ReceiveOne(%v)", os.Getpid(), workerNumber, queueSession.SessionID())
				return err
			}
		}
		fmt.Printf("%v/%v: queueSession.Close(%v)\n", os.Getpid(), workerNumber, queueSession.SessionID())
		if err := queueSession.Close(ctx); err != nil {
			log.Err(err).Msg("Whups")
			return err
		}
		return nil
	}
}

func peekingWorker(ctx context.Context, workerNumber int, queue *servicebus.Queue) func() error {
	return func() error {
		for {
			msg, err := queue.PeekOne(ctx)
			if err != nil {
				log.Error().AnErr("Problem peeking", err)
				continue
			}
			if msg == nil {
				time.Sleep(4 * time.Second)
				continue
			}
			log.Info().Msgf("Found msg: %s, %+v", msg.Data, msg)

			queueSession := queue.NewSession(msg.SessionID)

			fmt.Printf("%v/%v: queueSession.ReceiveOne(%v)\n", os.Getpid(), workerNumber, queueSession.SessionID())
			ssh := SimpleSessionHandler{WorkerId: workerNumber}
			if err := queueSession.ReceiveOne(ctx, &ssh); err != nil {
				log.Err(err).Msg("Whups")
				return err
			}
			fmt.Printf("%v/%v: queueSession.Close(%v)\n", os.Getpid(), workerNumber, queueSession.SessionID())
			if err := queueSession.Close(ctx); err != nil {
				log.Err(err).Msg("Whups")
				return err
			}
		}
		return nil
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

	for i := 1; i <= receiverCount; i++ {
		t.Go(monitoredWorker(ctx, i, q))
	}
	for i := 0; i < sessionCount; i++ {
		t.Go(func() error {
			return sendSession(ctx, q)
		})
	}
	log.Info().Msg("Waiting for everything to finish")
	return t.Wait()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	log.Logger = zerolog.New(output).With().Timestamp().Caller().Int("PID", os.Getpid()).Logger()
	ctx = context.WithValue(ctx, "log", log.Logger)
	log.Debug().Msg("Starting")
	if err := example_sessions(ctx); err != nil {
		log.Err(err).Msg("Exiting Main")
	}
}
