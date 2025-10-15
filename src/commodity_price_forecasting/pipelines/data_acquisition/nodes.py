import re
from pathlib import Path
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def process_tcb_file(file_path: Path) -> pd.DataFrame:
    try:
        df_raw = pd.read_excel(file_path, header=None, engine="openpyxl")

        date_col = df_raw.iloc[:, 11]
        non_null_dates = date_col.dropna()
        date = None
        
        if not non_null_dates.empty:
            date_value = non_null_dates.iloc[0]
            try:
                date = pd.to_datetime(date_value).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                return pd.DataFrame()
        else:
            return pd.DataFrame()

        start_idx: int | None = None
        end_idx: int | None = None

        for idx, row in df_raw.iterrows():
            first_col = str(row[0]).strip() if pd.notna(row[0]) else ""

            if start_idx is None and "চাল" in first_col and "সর্বনিম্ন" not in first_col:
                start_idx = idx

            if (
                start_idx is not None
                and "যে সকল বাজার হতে তথ্য সংগ্রহ করা হয়েছে" in first_col
            ):
                end_idx = idx
                break

        if start_idx is None or end_idx is None:
            return pd.DataFrame()

        df_section = df_raw.iloc[start_idx:end_idx].copy()
        df_section = df_section.reset_index(drop=True)

        categories: list[str] = [
            "চাল",
            "আটা/ময়দা",
            "ভোজ্য তেল",
            "ডাল",
            "মসলাঃ",
            "মাছ ও গোশত:",
            "গুড়া দুধ(প্যাকেটজাত)",
            "বিবিধঃ",
        ]

        current_category: str | None = None
        extracted_data: list[dict[str, str | None]] = []
        
        for idx, row in df_section.iterrows():
            col1 = str(row[0]).strip() if pd.notna(row[0]) else ""
            col2 = str(row[1]).strip() if pd.notna(row[1]) else ""
            col3 = str(row[2]).strip() if pd.notna(row[2]) else ""
            col4 = str(row[3]).strip() if pd.notna(row[3]) else ""


            if col1 in categories and (not col2 or col2 == "nan"):
                current_category = col1
                continue

            if (col1 == "nan" or not col1) and (
                "সর্বনিম্ন" in col3 and "সর্ব্বোচ্চ" in col4
            ):
                continue

            if (
                current_category
                and col1
                and col2
                and col3
                and col4
                and col1 != "nan"
                and col2 != "nan"
                and col3 != "nan"
                and col4 != "nan"
                and not col1.isdigit()
                and col1 not in categories
            ):

                if "=" in str(col3) or "=" in str(col4):
                    continue

                item_data: dict[str, str | None] = {
                    "date": date,
                    "category": current_category,
                    "item": col1.strip(),
                    "unit": col2.strip(),
                    "low_price": col3.strip(),
                    "high_price": col4.strip(),
                }
                extracted_data.append(item_data)

        return pd.DataFrame(extracted_data)

    except Exception as e:
        return pd.DataFrame()


def process_all_files(data_folder: str = 'data/tcb_files') -> pd.DataFrame:
    all_data: list[pd.DataFrame] = []
    path = Path(data_folder)

    files_processed: int = 0
    files_with_data: int = 0
    files_without_data: int = 0
    files_error: int = 0

    for file in path.glob("*.xlsx"):
        try:
            df_processed = process_tcb_file(file)

            if df_processed is not None and not df_processed.empty:
                all_data.append(df_processed)
                files_with_data += 1
            else:
                files_without_data += 1

            files_processed += 1

        except Exception:
            files_error += 1

    logger.info("=== PROCESSING SUMMARY ===")
    logger.info(f"Total files processed: {files_processed}")
    logger.info(f"Files with data extracted: {files_with_data}")
    logger.info(f"Files with NO data extracted: {files_without_data}")
    logger.info(f"Files with errors: {files_error}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total items extracted: {len(final_df)}")
        return final_df
    else:
        logger.warning("No data extracted from any files")
        return pd.DataFrame(
            columns=["date", "category", "item", "unit", "low_price", "high_price"]
        )
