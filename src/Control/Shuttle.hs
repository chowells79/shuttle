{-# Language RankNTypes, ExistentialQuantification #-}
module Control.Shuttle
  ( Shuttle
  , shuttle
  , startShuttle
  , startShuttle'
  , stopShuttle
  , ShuttleStopped(..)
  ) where

-- base
import Control.Concurrent (forkIO, mkWeakThreadId)
import Control.Monad.IO.Class (MonadIO, liftIO)
import System.Mem.Weak (deRefWeak)

import GHC.Conc(ThreadStatus(..), threadStatus)

-- exceptions
import Control.Monad.Catch
    ( SomeException
    , Exception
    , throwM
    , MonadCatch
    , try
    )

-- stm
import Control.Concurrent.STM
    ( atomically
    , TVar
    , newTVar
    , readTVar
    , writeTVar
    , mkWeakTVar
    , TMVar
    , newEmptyTMVar
    , putTMVar
    , takeTMVar
    )


-- Holds the results of startShuttle. The second argument is an action
-- that will gracefully end the forkIO'd thread and put the Shuttle
-- into an invalid state for future calls to shuttle.
--
-- The first argument is weirder. It is a TVar to enable the garbage
-- collector to kill an unreachable Shuttle, as odd as that
-- sounds. There needs to be a place to hook a finalizer that will
-- stop the background thread when the Shuttle is no longer
-- reachable. Due to GHC's very aggressive optimizations, regular data
-- types may be collected if all remaining locations project out the
-- same field, with only that field preserved. This could result in
-- premature finalization if the finalizer is attached to a data type
-- like Shuttle. TVars, though, won't be collected even if nothing
-- in the code will ever update their contents. Putting the core
-- function of the Shuttle inside a TVar gives a hook to attach a
-- finalizer to, to ensure that when the core function is no longer
-- accessible the finalizer is eligible to run. (Note the standard
-- disclaimers that the finalizer may not run promptly or at all, but
-- that seems acceptable here. Background threads don't prevent
-- program termination with GHC.) This does necessarily add some
-- indirection in calls to shuttle. Given the expectation that this
-- will be used to investigate APIs interactively, I'm willing to pay
-- a (hopefully-relatively-small) performance overhead to prevent
-- accidental runaway memory use.
--
-- | Can be used to run actions in the 'm' type in a background thread
-- maintaining a single coherent context, shuttling their results back
-- to the foreground when complete.
data Shuttle m = Shuttle (TVar (SF m)) (IO ())

-- non-impredicate wapper for older GHC compatibility
newtype SF m = SF { runSF :: forall a. m a -> IO a }

-- Pack together an action and a TMVar that can hold the result of
-- executing that action. It's unimportant what the type of that
-- result is, only that the TMVar can hold it. In fact, it's important
-- that the type of the action not leak into type of the Pack, so that
-- all Packs for the same environment can be sent via the same
-- channel.
data Pack m = forall a. Pack (m a) (TMVar (Either SomeException a))

-- | A minimal exception raised when calling shuttle after the
-- Shuttle has been stopped.
data ShuttleStopped = ShuttleStopped deriving (Eq, Ord, Show)
instance Exception ShuttleStopped


-- | Run an action in the background thread for this Shuttle. Any
-- exceptions raised during the execution of the action are caught and
-- re-raised by this. Does best-effort detection of the background
-- thread having stopped, but there is a window where this can be left
-- hanging. Hitting that case should require an async exception being
-- thrown to the background thread, or the background thread being
-- started without a proper try-like action. There's nothing that can
-- be done to prevent those cases and this is mostly intended for
-- interactive use, so I'm not too worried about them.
shuttle :: Shuttle m -> m a -> IO a
shuttle (Shuttle ref _) act = do
    sFunc <- atomically $ readTVar ref
    runSF sFunc act


-- | Calls startShuttle' with MonadCatch's try function. This is a
-- convenient default for instances of MonadCatch
startShuttle
    :: (MonadCatch m, MonadIO m)
    => (m () -> IO ())
    -> IO (Shuttle m)
startShuttle = startShuttle' try


-- | Launches a background thread to execute actions in another
-- context. Actions are shuttled from the user the caller of shuttle
-- to the background thread, and results are shuttled back.
startShuttle'
    :: MonadIO m
    => (forall a. m a -> m (Either SomeException a))
    -- ^ A function to handle exceptions thrown when executing the
    -- action passed to it. This should act like 'try'. If the
    -- produced action throws an exception instead of returning it,
    -- there is a risk of killing the background thread unexpectedly
    -- and without feedback.
    -> (m () -> IO ())
    -- ^ run the m-environment block. This is called only once, at the
    -- start of the background thread.
    -> IO (Shuttle m)
startShuttle' tryLike runner = do
    (shutdown, request) <- atomically $ (,) <$> newTVar False <*> newEmptyTMVar

    let execLoop = do
            next <- liftIO . atomically $ do
                done <- readTVar shutdown
                if done then pure Nothing else Just <$> takeTMVar request
            case next of
                Nothing -> pure () -- graceful shutdown
                Just (Pack act send) -> do
                    result <- tryLike act
                    liftIO . atomically $ putTMVar send result
                    execLoop

    -- Only store a weak reference to the ThreadId. This aids the
    -- runtime in determining when it can collect the thread and helps
    -- it detect when the thread is deadlocked in an STM action.
    wtid <- mkWeakThreadId =<< forkIO (runner execLoop)

    let remote act = do
            -- best-effort detection of the background thread not running
            mtid <- deRefWeak wtid
            tid <- maybe (throwM ShuttleStopped) pure mtid
            status <- threadStatus tid
            case status of
                ThreadFinished -> throwM ShuttleStopped
                ThreadDied -> throwM ShuttleStopped
                _ -> pure ()

            -- Check if the background thread is in the process of a
            -- graceful sthutdown, and send it a request if not.
            mbox <- atomically $ do
                done <- readTVar shutdown
                if done then throwM ShuttleStopped else pure ()
                receiver <- newEmptyTMVar
                putTMVar request $ Pack act receiver
                pure receiver

            response <- atomically $ takeTMVar mbox
            either throwM pure response

        stop = atomically $ writeTVar shutdown True

    -- Install the stop action as a finalizer on the TVar holding the
    -- remote call action.
    ref <- atomically $ newTVar (SF remote)
    _ <- mkWeakTVar ref stop

    return $ Shuttle ref stop


-- | Signal to a Shuttle that it may be gracefully stopped. This stop
-- should happen promptly, either immediately or right after finishing
-- processing the current action. Idempotent; no signalling if called
-- multiple times.
stopShuttle :: Shuttle m -> IO ()
stopShuttle (Shuttle _ stop) = stop
