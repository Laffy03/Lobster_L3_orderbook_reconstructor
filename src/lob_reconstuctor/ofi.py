from dataclasses import dataclass, field

@dataclass
class OFIPair:
    """
    Represents a pair of values for Order Flow Imbalance (OFI), specifically
    for size and count.

    Attributes
    ----------
    size : int, default=0
        The cumulative size (volume) component of the OFI.
    count : int, default=0
        The cumulative count (number of orders) component of the OFI.
    """
    size: int = 0
    count: int = 0

    def reset(self):
        self.size = 0
        self.count = 0


@dataclass
class OFI:
    """
    Represents the full set of components for Order Flow Imbalance (OFI).

    This class encapsulates various OFI components, each represented by an `OFIPair`,
    tracking both size and count. The components are typically used to measure
    market pressure from different types of order book events.

    Attributes
    ----------
    Lb : OFIPair
        Represents OFI from Limit Buy orders (new buy limits).
    La : OFIPair
        Represents OFI from Limit Ask orders (new sell limits).
    Db : OFIPair
        Represents OFI from Delete Buy orders (cancellations of buy limits).
    Da : OFIPair
        Represents OFI from Delete Ask orders (cancellations of sell limits).
    Mb : OFIPair
        Represents OFI from Market Buy orders (aggressor buys).
    Ma : OFIPair
        Represents OFI from Market Ask orders (aggressor sells).
    """
    Lb: OFIPair = field(default_factory=OFIPair)
    La: OFIPair = field(default_factory=OFIPair)
    Db: OFIPair = field(default_factory=OFIPair)
    Da: OFIPair = field(default_factory=OFIPair)
    Mb: OFIPair = field(default_factory=OFIPair)
    Ma: OFIPair = field(default_factory=OFIPair)

    def reset(self):
        for pair in (self.Lb, self.La, self.Db, self.Da, self.Mb, self.Ma):
            pair.reset()