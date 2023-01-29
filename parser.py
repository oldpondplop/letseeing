from pathlib import Path

import numpy as np
import pandas as pd


class ParseCsv:
    def __init__(self, csv_path):
        self.csv_path = Path(csv_path)
        self._df = self._parse_csv()

    def _parse_csv(self):
        headers = ["Data", "Detalii tranzactie", "Debit", "Credit", "Balanta"]
        data = pd.read_csv(self.csv_path, skiprows=[0], usecols=headers)
        data = data.dropna(how="all", subset=headers[1:])
        return data

    @staticmethod
    def _reformat_date(s):
        months = {
            "ianuarie": "01",
            "februarie": "02",
            "martie": "03",
            "aprilie": "04",
            "mai": "05",
            "iunie": "06",
            "iulie": "07",
            "august": "08",
            "septembrie": "09",
            "octombrie": "10",
            "noiembrie": "11",
            "decembrie": "12",
        }
        if isinstance(s, str):
            date = s.split(" ")
            if len(date) == 3:
                d, m, y = date
                date = "-".join((d, months[m], y))
                return date
        return s

    def _clean_df(self):
        clean = {
            "\.": "",  # 1.000,00 -> 1000.00
            ",": ".",  # 1.000,00 -> 1000.00
            "^[a-zA-Z](.*?)$": "0",  # 'str' - > 0
        }
        cols = self._df.columns[-3:]
        self._df[cols] = self._df[cols].replace(clean, regex=True).astype(float).fillna(0)
        # reformat the date
        self._df["Data"] = self._df["Data"].apply(self._reformat_date)
        self._df["Data"] = pd.to_datetime(self._df["Data"], errors="coerce", dayfirst=True)

    def _reshape_df(self):
        # reshape self._df, squash details
        sep = " ; "
        g = self._df["Balanta"].gt(0).cumsum()
        self._df = (
            self._df.groupby(g, as_index=False)
            .agg(
                {
                    "Data": "first",
                    "Balanta": "first",
                    "Credit": "first",
                    "Debit": "first",
                    "Detalii tranzactie": sep.join,
                }
            )
            .set_index("Data")
        )

    @property
    def get_csv_data(self):
        return self._parse_csv()

    @property
    def get_df(self):
        self._df = self._parse_csv()
        self._clean_df()
        self._reshape_df()
        return self._df


if __name__ == "__main__":
    data = ParseCsv("data.csv")
    df = data.get_df
    print(df.head(5))
