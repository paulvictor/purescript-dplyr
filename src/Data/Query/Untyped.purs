module Data.Query.Untyped where

import Prelude

import Control.Monad.Reader.Trans (asks)
import Data.Bifunctor (lmap)
import Data.DataFrame (DataFrame(..))
import Data.Foldable (elem)
import Data.Function (on)
import Data.List (sortBy)
import Data.List.NonEmpty (NonEmptyList)
import Data.Map (Map)
import Data.Map (filterKeys, insert, lookup, pop) as Map
import Data.Maybe (Maybe, fromMaybe, maybe)
import Data.Newtype (over)
import Data.Ordering (invert)
import Data.Query (Query)
import Data.String (Pattern(..))
import Data.String.NonEmpty (NonEmptyString, stripPrefix)
import Data.Tuple (uncurry)
import Types (Column, Row)

arrange
  :: ∀ m a
  . Ord a
  => Monad m
  => Column
  -> Query m (Row a) (Row a)
arrange column = asks (over DataFrame (sortBy (comp `on` Map.lookup columnName)))
  where
    withoutMinusPrefix = stripPrefix (Pattern "-") column
    columnName = fromMaybe column withoutMinusPrefix
    asc = maybe true (const false) withoutMinusPrefix
    comp a = if asc then compare a else invert <<< compare a

pull
  :: ∀ m a
  . Monad m
  => Column
  -> Query m (Row a) (Maybe a)
pull column = asks (map (Map.lookup column))

select
  :: ∀ m a
  . Monad m
  => NonEmptyList Column
  -> Query m (Row a) (Row a)
select ks = asks (map (Map.filterKeys (flip elem ks)))

rename
  :: ∀ m a
  . Monad m
  => Column
  -> Column
  -> Query m (Row a) (Row a)
rename from to = transmute from to identity

mutate
  :: ∀ m a
  . Monad m
  => Column
  -> (Row a -> a)
  -> Query m (Row a) (Row a)
mutate column f = asks (map (\r -> Map.insert column (f r) r))

transmute
  :: ∀ m a
  . Monad m
  => Column
  -> Column
  -> (a -> a)
  -> Query m (Row a) (Row a)
transmute from to f = asks (map go)
  where
  go row =
    maybe
      row
      (lmap f >>> uncurry (Map.insert to))
      (Map.pop from row)
