module Data.DataFrame where

import Prelude

import Data.Compactable (class Compactable, separate)
import Data.Filterable (class Filterable, partitionMap)
import Data.Foldable (intercalate, maximum, surround)
import Data.Function (on)
import Data.Generic.Rep (class Generic)
import Data.List (catMaybes, head, mapMaybe, partition, sortBy, take, transpose)
import Data.List (filter) as List
import Data.List.Types (List)
import Data.Map (Map)
import Data.Map (toUnfoldable) as Map
import Data.Maybe (fromMaybe, maybe)
import Data.Monoid (power)
import Data.Newtype (class Newtype, over, unwrap, wrap)
import Data.String (length)
import Data.String.NonEmpty (NonEmptyString)
import Data.Tuple (fst, snd)
import Data.Tuple.Nested (type (/\))

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

instance showUntypedDataFrame :: Show a => Show (DataFrame (Map NonEmptyString a)) where
  show =
    unwrap >>>
    take 10 >>>
    map (Map.toUnfoldable >>> sortBy (compare `on` fst)) >>>
    transpose >>>
    map drawColumn >>>
    transpose >>>
    map (surround " | ") >>> ((flip intercalate) <*> (head >>> maybe 0 length >>> power "-" >>> (_ <> "\n")))
    where
    drawColumn :: List (NonEmptyString /\ a) -> List String
    drawColumn xs =
      map
        (snd >>> padTo (max (maxLengthOfValues xs) (maybe 0 (fst >>> show >>> length) (head xs))))
        xs
    maxLengthOfValues :: List (NonEmptyString /\ a) -> Int
    maxLengthOfValues = map (snd >>> show >>> length) >>> maximum >>> fromMaybe 0
    padTo :: Int -> a -> String
    padTo i a =
      let pad = power " " $ (i - length (show a)) / 2
      in pad <> show a <> pad
