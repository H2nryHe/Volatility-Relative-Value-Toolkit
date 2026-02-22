"""VX-style roll engine: roll N business days before expiry (no lookahead)."""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def _business_days_to_expiry(date: pd.Timestamp, expiry: pd.Timestamp) -> int:
    return int(np.busday_count(date.date(), expiry.date()))


def build_continuous_series(df: pd.DataFrame, roll_config: Dict[str, object]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build active-contract series and roll log using only same-day metadata."""

    if df.empty:
        return df.copy(), pd.DataFrame(columns=["date", "from_contract", "to_contract", "reason"])

    contract_col = str(roll_config.get("contract_column", "contract"))
    expiry_col = str(roll_config.get("expiry_column", "expiry"))
    root_col = str(roll_config.get("root_column", "root_symbol"))
    n_days = int(roll_config.get("n_days_before_expiry", 5))

    missing_required = [c for c in [contract_col, expiry_col, root_col] if c not in df.columns]
    if missing_required:
        continuous = df.copy()
        continuous["active_contract"] = continuous["symbol"].astype(str)
        continuous["roll_reason"] = "no_roll_metadata"
        empty_log = pd.DataFrame(columns=["date", "from_contract", "to_contract", "reason"])
        return continuous, empty_log

    work = df.copy()
    work[expiry_col] = pd.to_datetime(work[expiry_col], errors="coerce")

    selected_rows: List[pd.Series] = []
    roll_events: List[Dict[str, object]] = []

    for root, root_df in work.groupby(root_col, sort=True):
        root_df = root_df.sort_values(["date", expiry_col, contract_col]).copy()
        current_contract: str | None = None

        for date, day_df in root_df.groupby("date", sort=True):
            available = day_df.dropna(subset=[expiry_col, contract_col]).sort_values([expiry_col, contract_col])
            if available.empty:
                continue

            contracts = available[contract_col].astype(str).tolist()

            if current_contract is None or current_contract not in contracts:
                next_contract = str(available.iloc[0][contract_col])
                if current_contract is not None and current_contract != next_contract:
                    roll_events.append(
                        {
                            "date": pd.Timestamp(date),
                            "from_contract": current_contract,
                            "to_contract": next_contract,
                            "reason": "contract_unavailable",
                            "root_symbol": root,
                        }
                    )
                current_contract = next_contract
                roll_reason = "initialize_active_contract"
            else:
                current_row = available.loc[available[contract_col].astype(str) == current_contract].iloc[0]
                dte = _business_days_to_expiry(pd.Timestamp(date), pd.Timestamp(current_row[expiry_col]))

                later_contracts = available.loc[
                    available[expiry_col] > pd.Timestamp(current_row[expiry_col]),
                    contract_col,
                ].astype(str)

                if dte <= n_days and not later_contracts.empty:
                    next_contract = later_contracts.iloc[0]
                    if next_contract != current_contract:
                        roll_events.append(
                            {
                                "date": pd.Timestamp(date),
                                "from_contract": current_contract,
                                "to_contract": next_contract,
                                "reason": f"roll_{n_days}bd_before_expiry",
                                "root_symbol": root,
                            }
                        )
                        current_contract = next_contract
                        roll_reason = f"roll_{n_days}bd_before_expiry"
                    else:
                        roll_reason = "hold_active_contract"
                else:
                    roll_reason = "hold_active_contract"

            selected = available.loc[available[contract_col].astype(str) == current_contract].iloc[0].copy()
            selected["active_contract"] = current_contract
            selected["roll_reason"] = roll_reason
            selected_rows.append(selected)

    continuous = pd.DataFrame(selected_rows)
    if continuous.empty:
        continuous = pd.DataFrame(columns=[*df.columns, "active_contract", "roll_reason"])
    else:
        continuous.sort_values(["date", "active_contract"], inplace=True)
        continuous.reset_index(drop=True, inplace=True)

    roll_log = pd.DataFrame(roll_events)
    if roll_log.empty:
        roll_log = pd.DataFrame(columns=["date", "from_contract", "to_contract", "reason", "root_symbol"])
    else:
        roll_log.sort_values(["date", "root_symbol"], inplace=True)
        roll_log.reset_index(drop=True, inplace=True)

    return continuous, roll_log
