from pydantic import BaseModel
from pydantic import parse_obj_as, ValidationError
from typing import Type
import pandas as pd


class TaiwanStockPrice(BaseModel):
    StockID: str
    TradeVolume: int
    Transaction: int
    TradeValue: int
    Open: float
    Max: float
    Min: float
    Close: float
    Change: float
    Date: str


class TaiwanFuturesDaily(BaseModel):
    Date: str
    FuturesID: str
    ContractDate: str
    Open: float
    Max: float
    Min: float
    Close: float
    Change: float
    ChangePer: float
    Volume: float
    SettlementPrice: float
    OpenInterest: int
    TradingSession: str


def check_schema(df: pd.DataFrame, schema: Type) -> pd.DataFrame:
    """
    Validate DataFrame using the given Pydantic schema.

    Args:
        df (pd.DataFrame): Input data.
        schema (Type): A Pydantic BaseModel class.

    Returns:
        pd.DataFrame: Validated records as DataFrame.
    """
    
    #Check Schema
    try:
        records = parse_obj_as(list[schema], df.to_dict(orient="records"))
        return pd.DataFrame([r.dict() for r in records])
    
    except ValidationError as e:
        print("Schema validation failed:\n", e)
        return pd.DataFrame()


