import pandas as pd
import json
from pathlib import Path

def parse_csv_of_individuals(fp):
    df = pd.read_csv(fp,
        usecols=["Name", "OpenAlex_id"],
        dtype=str,
        keep_default_na=False,      # empty cells stay as ""
    )

    df["Name"] = df["Name"].str.strip()
    df["OpenAlex_id"] = df["OpenAlex_id"].str.strip()

    df = df[(df["Name"] != "") & (df["OpenAlex_id"] != "")]
    return dict(zip(df["Name"], df["OpenAlex_id"]))

def write_json_to_local(data_obj,fp):
    path = Path(fp)
    
    with path.open("w", encoding="utf-8") as f:
        json.dump(data_obj, f, indent=2, ensure_ascii=False)
        
def read_json_from_local(fp):
    path = Path(fp)

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)