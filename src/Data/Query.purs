module Data.Query where

import Prelude

import Control.Monad.Reader.Class (asks)
import Control.Monad.Reader.Trans (ReaderT)
import Data.DataFrame (DataFrame(..))
import Data.Filterable (filter) as Filterable
import Data.Foldable (foldr)
import Data.List (List, (:))
import Data.Map (alter, empty, toUnfoldable) as Map
import Data.Maybe (Maybe(..), maybe)
import Data.Newtype (over)
import Data.Tuple (snd)
import Data.Tuple.Nested (type (/\))

type Query m a b = ReaderT (DataFrame a) m (DataFrame b)

filter :: ∀ m a. Monad m => (a -> Boolean) -> Query m a a
filter f = asks (Filterable.filter f)

groupBy
  :: ∀ m a b
  . Monad m
  => Ord b
  => (a -> b)
  -> Query m a (b /\ (List a))
groupBy f = asks go
  where
  go =
    over DataFrame $
      foldr
        (\a -> Map.alter (Just <<< maybe (pure a) (a : _)) (f a))
        Map.empty
      >>> Map.toUnfoldable

summarise
  :: ∀ m a b c
  . Monad m
  => (List a -> c)
  -> Query m (b /\ (List a)) (b /\ c)
summarise f = asks (over DataFrame (map (map f)))

ungroup
  :: ∀ m a b
  . Monad m
  => Query m (b /\ (List a)) a
ungroup = asks (over DataFrame (map snd >>> join))
