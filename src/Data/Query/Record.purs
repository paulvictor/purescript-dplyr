module Data.Query.Record where

import Prelude

import Control.Monad.Reader.Class (asks)
import Data.DataFrame (DataFrame(..))
import Data.Function (on)
import Data.List (sortBy)
import Data.Newtype (over)
import Data.Ordering (invert)
import Data.Query (Query)
import Data.Symbol (class IsSymbol, SProxy(..))
import Prim.Row (class Cons, class Lacks)
import Prim.RowList (class RowToList, Cons, Nil, kind RowList)
import Record (get, insert)
import Record (insert, rename) as R
import Type.Row (class ListToRow, RLProxy(..), RProxy)


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
arrange proxy asc = asks (over DataFrame $ sortBy (comp `on` (get proxy)))
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
pull proxy = asks (over DataFrame $ map ( get proxy))

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
select proxy = asks (map (flip subsetRow proxy))

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
rename proxy proxy' = asks (map (R.rename proxy proxy'))

mutate
  :: ∀ m input output sym a
  . Monad m
  => IsSymbol sym
  => Lacks sym input
  => Cons sym a input output
  => SProxy sym
  -> ({|input} -> a)
  -> Query m {|input} {|output}
mutate proxy f = asks (map (\r -> R.insert proxy (f r) r))

transmute
  :: ∀ m input sym a out
  . Monad m
  => IsSymbol sym
  => Lacks sym ()
  => Cons sym a () out
  => SProxy sym
  -> ({|input} -> a)
  -> Query m {|input} {|out}
transmute proxy f = asks (map \r -> R.insert proxy (f r) {})
