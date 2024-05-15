from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable

from .store import DeltaLiveEntity
from .types import DestinationType


class Predicate(ABC):
    """
    Base class for predicates used for filtering DeltaLiveEntity objects.
    """

    @abstractmethod
    def __call__(self, entity: DeltaLiveEntity) -> bool:
        """
        Abstract method that should be implemented by subclasses.
        Evaluates the predicate for the given DeltaLiveEntity object.

        Args:
            entity (DeltaLiveEntity): The DeltaLiveEntity object to evaluate.

        Returns:
            bool: True if the predicate is satisfied, False otherwise.
        """
        pass

    def __or__(self, other: Predicate) -> OrPredicate:
        """
        Logical OR operator for combining two predicates.

        Args:
            other (Predicate): The other predicate to combine with.

        Returns:
            OrPredicate: The combined predicate.
        """
        return OrPredicate(self, other)

    def __and__(self, other: Predicate) -> AndPredicate:
        """
        Logical AND operator for combining two predicates.

        Args:
            other (Predicate): The other predicate to combine with.

        Returns:
            AndPredicate: The combined predicate.
        """
        return AndPredicate(self, other)

    def __invert__(self) -> NotPredicate:
        """
        Logical NOT operator for negating a predicate.

        Returns:
            NotPredicate: The negated predicate.
        """
        return NotPredicate(self)

    def __eq__(self, other: object) -> bool:
        """
        Equality operator for comparing two predicates.

        Args:
            other (object): The other object to compare with.

        Returns:
            bool: True if the predicates are equal, False otherwise.
        """
        return isinstance(other, self.__class__)

    def contains(self, other: object) -> bool:
        """
        Determines if the current predicate contains another predicate.

        Args:
            other (object): The other predicate to check.

        Returns:
            bool: True if the current predicate contains the other predicate, False otherwise.
        """
        return self == other


class NotPredicate(Predicate):
    """
    Predicate that negates the result of another predicate.
    """

    def __init__(self, predicate: Predicate):
        """
        Initializes a NotPredicate object.

        Args:
            predicate (Predicate): The predicate to negate.
        """
        self.predicate = predicate

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return not self.predicate(entity)

    def __str__(self):
        return f"~{self.predicate}"


class OrPredicate(Predicate):
    """
    Predicate that performs a logical OR operation on two predicates.
    """

    def __init__(self, lhs: Predicate, rhs: Predicate):
        """
        Initializes an OrPredicate object.

        Args:
            lhs (Predicate): The left-hand side predicate.
            rhs (Predicate): The right-hand side predicate.
        """
        self.lhs = lhs
        self.rhs = rhs

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return self.lhs(entity) or self.rhs(entity)

    def __str__(self):
        return f"({self.lhs} | {self.rhs})"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.lhs == other.lhs and self.rhs == other.rhs

    def contains(self, other: object) -> bool:
        """
        Determines if the current predicate contains another predicate.

        Args:
            other (object): The other predicate to check.

        Returns:
            bool: True if the current predicate contains the other predicate, False otherwise.
        """
        return self.lhs == other or self.rhs == other


class AndPredicate(Predicate):
    """
    Predicate that performs a logical AND operation on two predicates.
    """

    def __init__(self, lhs: Predicate, rhs: Predicate):
        """
        Initializes an AndPredicate object.

        Args:
            lhs (Predicate): The left-hand side predicate.
            rhs (Predicate): The right-hand side predicate.
        """
        self.lhs = lhs
        self.rhs = rhs

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return self.lhs(entity) and self.rhs(entity)

    def __str__(self):
        return f"({self.lhs} & {self.rhs})"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.lhs == other.lhs and self.rhs == other.rhs

    def contains(self, other: object) -> bool:
        """
        Determines if the current predicate contains another predicate.

        Args:
            other (object): The other predicate to check.

        Returns:
            bool: True if the current predicate contains the other predicate, False otherwise.
        """
        return self.lhs == other or self.rhs == other


class by_all(Predicate):
    """
    Predicate that matches all DeltaLiveEntity objects.
    """

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return True

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other)


class by_id(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific ID.
    """

    def __init__(self, id: str):
        """
        Initializes a by_id Predicate object.

        Args:
            id (str): The ID to match.
        """
        self.id = id

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.id == self.id

    def __str__(self):
        return f"id == {self.id}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.id == other.id


class by_destination_type(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific source type.
    """

    def __init__(self, destination_type: DestinationType):
        """
        Initializes a by_destination_type Predicate object.

        Args:
            destination_type (DestinationType): The destination_\ type to match.
        """
        self.destination_type = destination_type

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.destination_type == self.destination_type

    def __str__(self):
        return f"destination_type == {self.destination_type}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.destination_type == other.destination_type


class by_entity_id(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific entity ID.
    """

    def __init__(self, entity_id: str):
        """
        Initializes a by_entity_id Predicate object.

        Args:
            entity_id (str): The entity ID to match.
        """
        self.entity_id = entity_id

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.entity_id == self.entity_id

    def __str__(self):
        return f"entity_id == {self.entity_id}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.entity_id == other.entity_id


class by_group(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific group.
    """

    def __init__(self, group: str):
        """
        Initializes a by_group Predicate object.

        Args:
            group (str): The group to match.
        """
        group = None if group == "" else group
        self.group = group

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.group == self.group

    def __str__(self):
        return f"group == {self.group}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.group == other.group


class by_has_tag(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific tag.
    """

    def __init__(self, tag: str):
        """
        Initializes a by_tag Predicate object.

        Args:
            tag (str): The tag to match.
        """
        self.tag = tag

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return self.tag in entity.tags

    def __str__(self):
        return f"{self.tag} in tags"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.tag == other.tag


class by_has_tag_value(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific tag.
    """

    def __init__(self, tag: str, value: str):
        """
        Initializes a by_tag Predicate object.

        Args:
            tag (str): The tag to match.
        """
        self.tag = tag
        self.value = value

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return self.tag in entity.tags and entity.tags[self.tag] == self.value

    def __str__(self):
        return f"{self.tag} in tags and {self.tag} == {self.value}"

    def __eq__(self, other: object) -> bool:
        return (
            super().__eq__(other)
            and self.tag == other.tag
            and self.value == other.value
        )


class by_source(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific source.
    """

    def __init__(self, source: str):
        """
        Initializes a by_source Predicate object.

        Args:
            source (str): The source to match.
        """
        self.source = source

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.source == self.source

    def __str__(self):
        return f"source == {self.source}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.source == other.source


class by_destination(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific destination.
    """

    def __init__(self, destination: str):
        """
        Initializes a by_destination Predicate object.

        Args:
            destination (str): The destination to match.
        """
        self.destination = destination

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.destination == self.destination

    def __str__(self):
        return f"destination == {self.destination}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.destination == other.destination


class by_is_enabled(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects based on their enabled status.
    """

    def __init__(self, is_enabled: bool = True):
        """
        Initializes a by_is_enabled Predicate object.

        Args:
            enabled (bool, optional): The enabled status to match. Defaults to True.
        """
        self.is_enabled = is_enabled

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.is_enabled == self.is_enabled

    def __str__(self):
        return f"is_enabled == {self.is_enabled}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.is_enabled == other.is_enabled


class by_is_latest(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects based on their latest status.
    """

    def __init__(self, is_latest: bool = True):
        """
        Initializes a by_is_latest Predicate object.

        Args:
            is_latest (bool, optional): The latest status to match. Defaults to True.
        """
        self.is_latest = is_latest

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.is_latest == self.is_latest

    def __str__(self):
        return f"is_latest == {self.is_latest}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.is_latest == other.is_latest


class by_created_ts_between(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects based on their created timestamp.
    """

    def __init__(
        self,
        start_ts: datetime = datetime.min,
        end_ts: datetime = datetime.max,
        inclusive: bool = True,
    ):
        """
        Initializes a by_created_ts_between Predicate object.

        Args:
            start_ts (datetime, optional): The start timestamp to match. Defaults to datetime.min.
            end_ts (datetime, optional): The end timestamp to match. Defaults to datetime.max.
            inclusive (bool, optional): Whether the start and end timestamps are inclusive. Defaults to True.
        """
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.inclusive = inclusive

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        if self.inclusive:
            return self.start_ts <= entity.created_ts <= self.end_ts
        else:
            return self.start_ts < entity.created_ts < self.end_ts

    def __str__(self):
        return f"{self.start_ts} <= created_ts <= {self.end_ts}"

    def __eq__(self, other: object) -> bool:
        return (
            super().__eq__(other)
            and self.start_ts == other.start_ts
            and self.end_ts == other.end_ts
            and self.inclusive == other.inclusive
        )


class by_created_by(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects based on their latest status.
    """

    def __init__(self, created_by: str):
        """
        Initializes a by_is_latest Predicate object.

        Args:
            is_latest (bool, optional): The latest status to match. Defaults to True.
        """
        self.created_by = created_by

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.created_by == self.created_by

    def __str__(self):
        return f"created_by == {self.created_by}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.created_by == other.created_by


class by_modified_by(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects based on their latest status.
    """

    def __init__(self, modified_by: str):
        """
        Initializes a by_is_latest Predicate object.

        Args:
            is_latest (bool, optional): The latest status to match. Defaults to True.
        """
        self.modified_by = modified_by

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.modified_by == self.modified_by

    def __str__(self):
        return f"modified_by == {self.modified_by}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.modified_by == other.modified_by


class by_reference_ts(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects based on their latest status.
    """

    def __init__(self, reference_ts: datetime):
        """
        Initializes a by_not_expired Predicate object.

        Args:
            expiration_time (datetime): The expiration time to compare against.
        """
        self.reference_ts = reference_ts

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.created_ts <= self.reference_ts <= entity.expired_ts

    def __str__(self):
        return f"created_ts <= {self.reference_ts} <= expired_ts"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.reference_ts == other.reference_ts


class by_is_streaming(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific tag.
    """

    def __init__(self, is_streaming: bool = True):
        self.is_streaming = is_streaming

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return entity.is_streaming == self.is_streaming

    def __str__(self):
        return f"is_streaming == {self.is_streaming}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.is_streaming == other.is_streaming


class by_predicate(Predicate):
    """
    Predicate that matches DeltaLiveEntity objects with a specific tag.
    """

    def __init__(self, predicate: Callable[[DeltaLiveEntity], bool]):
        self.predicate = predicate

    def __call__(self, entity: DeltaLiveEntity) -> bool:
        return self.predicate(entity)

    def __str__(self):
        return f"predicate == {self.predicate}"

    def __eq__(self, other: object) -> bool:
        return super().__eq__(other) and self.predicate == other.predicate
