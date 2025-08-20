from dataclasses import dataclass, field

@dataclass
class OFIPair:

    size: int = 0
    count: int = 0

    def reset(self):
        self.size = 0
        self.count = 0


@dataclass
class OFI:
    Lb: OFIPair = field(default_factory=OFIPair)
    La: OFIPair = field(default_factory=OFIPair)
    Db: OFIPair = field(default_factory=OFIPair)
    Da: OFIPair = field(default_factory=OFIPair)
    Mb: OFIPair = field(default_factory=OFIPair)
    Ma: OFIPair = field(default_factory=OFIPair)

    def reset(self):
        for pair in (self.Lb, self.La, self.Db, self.Da, self.Mb, self.Ma):
            pair.reset()