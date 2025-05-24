from dataclasses import dataclass
from typing import Optional

@dataclass
class AuthData:
    merchant_id: str
    merchant_name: str
    access_id: str
    token: str
    api_url: str

@dataclass
class Downline:
    url: str
    id: str
    name: str
    count: int
    amount: float
    register_date_time: str

@dataclass
class Bonus:
    url: str
    merchant_name: str
    id: str
    name: str
    transaction_type: str
    bonus_fixed: float
    amount: float
    min_withdraw: float
    max_withdraw: float
    withdraw_to_bonus_ratio: Optional[float]
    rollover: float
    balance: str
    claim_config: str
    claim_condition: str
    bonus: str
    bonus_random: str
    reset: str
    min_topup: float
    max_topup: float
    refer_link: str
