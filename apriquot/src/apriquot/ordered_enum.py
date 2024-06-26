from enum import StrEnum
from typing import Self


class InvalidEnumValueError(ValueError):
    """Exception raised for invalid enum values."""


class OrderedEnum(StrEnum):
    def __eq__(self, other: object) -> bool:
        """
        Check equality with another object.

        Args:
            other (object): The object to compare with.

        Returns
        -------
            bool: True if equal, False otherwise.
        """
        if isinstance(other, str):
            return self.value == other.lower()
        return super().__eq__(other)

    def __lt__(self, other: Self | str) -> bool:
        """
        Check if the enum member is less than another.

        Args:
            other (Union[OrderedEnum, str]): The object to compare with.

        Returns
        -------
            bool: True if less, False otherwise.
        """
        if isinstance(other, str):
            try:
                other = self.__class__(other.lower())
            except ValueError as e:
                msg = f"'{other}' is not a valid {self.__class__.__name__}"
                raise InvalidEnumValueError(msg) from e
        if not isinstance(other, OrderedEnum):
            return NotImplemented
        return list(self.__class__).index(self) < list(self.__class__).index(other)

    def __hash__(self) -> int:
        """
        Get the hash value of the enum member.

        Returns
        -------
            int: The hash value.
        """
        return hash(self.value)

    def __str__(self) -> str:
        """
        Get the string representation of the enum member.

        Returns
        -------
            str: The string representation.
        """
        return f"{self.__class__.__name__}.{self.name}"

    def __repr__(self) -> str:
        """
        Get the detailed string representation of the enum member.

        Returns
        -------
            str: The detailed string representation.
        """
        return f"<{self.__class__.__name__}.{self.name}: {self.value}>"

    @classmethod
    def values(cls) -> list[str]:
        """
        List all values of the enum.

        Returns
        -------
            list[str]: The list of all enum values.
        """
        return [member.value for member in cls]

    @classmethod
    def members(cls) -> list[Self]:
        """
        List all members of the enum.

        Returns
        -------
            list[OrderedEnum]: The list of all enum members.
        """
        return list(cls)
