module Data.DataFrame where

import Prelude

import Data.Compactable (class Compactable, separate)
import Data.Filterable (class Filterable, partitionMap)
import Data.Function (on)
import Data.Generic.Rep (class Generic)
import Data.List (catMaybes, mapMaybe, partition)
import Data.List (filter) as List
import Data.List.Types (List)
import Data.Newtype (class Newtype, over, unwrap, wrap)

newtype DataFrame a = DataFrame (List a)

derive instance newtypeDF :: Newtype (DataFrame a) _

derive instance genericDF :: Generic (DataFrame a) _

derive instance functorDF :: Functor DataFrame

instance semigroupDF :: Semigroup (DataFrame a) where
  append df1 = wrap <<< on append unwrap df1

instance monoidDF :: Monoid (DataFrame a) where
  mempty = wrap mempty

instance compactbleDF :: Compactable DataFrame where
  compact = over DataFrame catMaybes
  separate = (\{left, right} -> {left: wrap left, right: wrap right}) <<< separate <<< unwrap

instance filterableDF :: Filterable DataFrame where
  filter f = over DataFrame (List.filter f)
  filterMap f = over DataFrame (mapMaybe f)
  partition f = (\{yes, no} -> {yes: wrap yes, no: wrap no}) <<< partition f <<< unwrap
  partitionMap f = (\{left, right} -> {left: wrap left, right: wrap right}) <<< partitionMap f <<< unwrap
