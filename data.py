from dataclasses import dataclass, asdict


@dataclass
class DaysData:
    date: str
    temp_avg: float
    relevant_cond_hours: int

@dataclass
class CityData:
    name: str
    temp_avg: float
    relevant_cond_hours: int
    days: list[DaysData]

    @property
    def get_dict(self):
        return {self.name: asdict(self, dict_factory=lambda x: {k: v for (k, v) in x if k != 'name'})}
