from pydantic import BaseModel
from pydantic import parse_obj_as, ValidationError
from typing import Type
import pandas as pd
from typing import Optional

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


class TaiwanStockInfo(BaseModel):
    StockID: int
    StockName: str
    MarketType: str
    IndustryType: str   


class TDCCShareholding(BaseModel):
    Date: str
    StockID: str
    ShareholdingLevel: int
    NumberOfHolders: int
    NumberOfShares: int
    PercentageOfTotalShares: float

class TaiwanInstitutionalInvestor(BaseModel):
    StockID: str
    StockName: str
    ForeignBuy: int
    ForeignSell: int
    ForeignNet: int
    ForeignDealerBuy: int
    ForeignDealerSell: int
    ForeignDealerNet: int
    InvestmentTrustBuy: int
    InvestmentTrustSell: int
    InvestmentTrustNet: int
    DealerSelfBuy: int
    DealerSelfSell: int
    DealerSelfNet: int
    DealerHedgeBuy: int
    DealerHedgeSell: int
    DealerHedgeNet: int
    ThreeInstitutionNet: int
    Date: str

class TaiwanMarginPurchaseShortSale(BaseModel):
    StockID: str
    StockName: str
    MarginPurchaseBuy: int
    MarginPurchaseSell: int
    MarginPurchaseCashRepayment: int
    MarginPurchaseYesterdayBalance: int
    MarginPurchaseTodayBalance: int
    MarginPurchaseLimit: int
    ShortSaleBuy: int
    ShortSaleSell: int
    ShortSaleCashRepayment: int
    ShortSaleYesterdayBalance: int
    ShortSaleTodayBalance: int
    ShortSaleLimit: int
    OffsetLoanAndShort: int
    Note: str
    Date: str


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


