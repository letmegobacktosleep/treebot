import os
import pandas as pd

files = os.listdir("data/")
files = [file for file in files if os.path.splitext(file)[-1] == ".csv"]

for file in files:
    # skip non csv files
    if os.path.splitext(file)[-1] != ".csv":
        continue
    path = os.path.join("data", file)
    df = pd.read_csv(path)
    # skip files that don't conform to the old format
    if (
        "wet" not in df.columns or
        "dry" not in df.columns
    ):
        continue
    # rename the columns
    df = df.rename(
        columns={
            "wet": "start",
            "dry": "end"
        }
    )
    # add a new "type" column
    df['type'] = "water"
    # ???
    new_path = os.path.join("data", f"{os.path.splitext(file)[0]}_new{os.path.splitext(file)[-1]}")
    # write the file back
    df.to_csv(
        new_path, index=False,
        encoding="utf-8"
    )
