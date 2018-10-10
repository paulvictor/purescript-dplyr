module Types where

import Data.Map (Map)
import Data.String.NonEmpty (NonEmptyString)

type Column = NonEmptyString

type Row a = Map Column a
