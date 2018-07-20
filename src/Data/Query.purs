module Data.Query where

import Prelude

import Control.Monad.Reader.Class (ask)
import Control.Monad.Reader.Trans (ReaderT)
import Data.DataFrame (DataFrame(..))
import Data.Filterable (filter) as Filterable
import Data.Function (on)
import Data.List (sortBy)
import Data.List.Lazy (List, uncons, (:))
import Data.List.Lazy (filter) as L
import Data.Maybe (maybe)
import Data.Newtype (over)
import Data.Ordering (invert)
import Data.Symbol (class IsSymbol, SProxy(..))
import Prim.Row (class Cons, class Lacks)
import Prim.RowList (class RowToList, Cons, Nil, kind RowList)
import Record (get, insert)
import Record (insert, rename) as R
import Type.Row (class ListToRow, RLProxy(..), RProxy)

type Query m a b = ReaderT (DataFrame a) m (DataFrame b)

filter :: ∀ m a. Monad m => (a -> Boolean) -> Query m a a
filter f = (Filterable.filter f) <$> ask

arrange
  :: ∀ m sym a rec without
  . Ord a
  => Monad m
  => IsSymbol sym
  => Lacks sym without
  => Cons sym a without rec
  => SProxy sym
  -> Boolean
  -> Query m ({|rec}) ({|rec})
arrange proxy asc = (over DataFrame $ sortBy (comp `on` (get proxy))) <$> ask
  where
    comp :: a -> a -> Ordering
    comp a = if asc then compare a else invert <<< compare a

pull
  :: ∀ m sym a rec without' without
  . Monad m
  => IsSymbol sym
  => Lacks sym without
  => Lacks sym without'
  => Cons sym a without rec
  => SProxy sym
  -> Query m {|rec} a
pull proxy = (over DataFrame $ map (( get proxy))) <$> ask

class RowSubset (row :: # Type) (xs :: RowList) (subrow :: # Type) | row xs -> subrow where
  subsetImpl :: RLProxy xs -> {|row} -> {|subrow}

instance nilRowSubset :: RowSubset row Nil () where subsetImpl _ _ = {}

instance consRowSubset
  ::
  ( IsSymbol sym
  , Cons sym a subrowWithoutSym subrowWithSym
  , Cons sym a srcWithoutSym src
  , Lacks sym subrowWithoutSym
  , RowSubset src rl subrowWithoutSym
  ) => RowSubset src (Cons sym a rl) subrowWithSym where
  subsetImpl _ src = insert (SProxy :: SProxy sym) (get (SProxy :: SProxy sym) src) (subsetImpl (RLProxy :: RLProxy rl) src)

subsetRow
  :: ∀ rl subR src
  . ListToRow rl subR
  => RowToList subR rl
  => RowSubset src rl subR
  => {|src}
  -> RProxy subR
  -> {|subR}
subsetRow src _ = subsetImpl (RLProxy :: RLProxy rl) src

select
  :: ∀ m rec rl subR
  . Monad m
  => RowSubset rec rl subR
  => RowToList subR rl
  => ListToRow rl subR
  => RProxy subR
  -> Query m {|rec} {|subR}
select proxy = (map (flip subsetRow proxy)) <$> ask

rename
  :: ∀ m inter input output sym sym' a
  . Monad m
  => IsSymbol sym
  => IsSymbol sym'
  => Cons sym a inter input
  => Lacks sym inter
  => Cons sym' a inter output
  => Lacks sym' inter
  => SProxy sym
  -> SProxy sym'
  -> Query m {|input} {|output}
rename proxy proxy' = (map (R.rename proxy proxy')) <$> ask

mutate
  :: ∀ m input output sym a
  . Monad m
  => IsSymbol sym
  => Lacks sym input
  => Cons sym a input output
  => SProxy sym
  -> ({|input} -> a)
  -> Query m {|input} {|output}
mutate proxy f = (map (\r -> R.insert proxy (f r) r)) <$> ask

transmute
  :: ∀ m input sym a out
  . Monad m
  => IsSymbol sym
  => Lacks sym ()
  => Cons sym a () out
  => SProxy sym
  -> ({|input} -> a)
  -> Query m {|input} {|out}
transmute proxy f = (map \r -> R.insert proxy (f r) {}) <$> ask

sort :: ∀ a. Ord a => List a -> List a
sort xs = maybe mempty (\{ head, tail } -> (sort $ L.filter (_ <= head) tail) <> (head : (sort $ L.filter (_ > head) tail))) $ uncons xs
