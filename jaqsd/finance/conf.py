from jaqsd.structure import Income, CashFlow, BalanceSheet, FinIndicator, SecDividend, ProfitExpress
import os


WEEKLY = os.environ.get("FINANCE_WEEKLY", "log.finance_weekly")
DAILY = os.environ.get("FINANCE_DAILY", "log.finance_daily")
LB = os.environ.get("LB", "lb")

VIEWS = {structure.view: structure for structure in [Income, CashFlow, BalanceSheet, FinIndicator, SecDividend, ProfitExpress]}

TABLES = {
    Income.view: "income",
    CashFlow.view: "cashFlow",
    BalanceSheet.view: "balanceSheet",
    FinIndicator.view: "finIndicator",
    SecDividend.view: "secDividend",
    ProfitExpress.view: "profitExpress"
}

INDEXES = {
    Income.view: [
        "report_date",
        "report_type",
        "comp_type_code",
        "symbol",
        "ann_date",
        "act_ann_date"
    ],
    CashFlow.view: [
        "report_date",
        "report_type",
        "update_flag",
        "comp_type_code",
        "symbol",
        "ann_date",
        "act_ann_date"
    ],
    BalanceSheet.view: [
        "report_date",
        "report_type",
        "update_flag",
        "comp_type_code",
        "symbol",
        "ann_date",
        "act_ann_date"
    ],
    FinIndicator.view: [
        "ann_date",
        "report_date",
        "symbol"
    ],
    SecDividend.view: [
        "symbol",
        "exdiv_date"
    ],
    ProfitExpress.view: [
        "symbol",
        "ann_date",
        "report_date"
    ]
}