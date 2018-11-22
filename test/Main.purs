module Test.Main where

import Prelude

import Control.Monad.Reader.Class (ask)
import Control.Monad.Reader.Trans (runReaderT)
import Control.Monad.Trans.Class (lift)
import Data.Query (Query)
import Effect (Effect)
import Effect.Console (log)

main :: Effect Unit
main = do
  log "You should add some tests."
