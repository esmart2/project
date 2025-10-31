import pandas as pd
import fuzzy
import uuid
import datetime
from utils import run_sql



def add_helper_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Add unique id per row
    df["id"] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Add/override import date
    df["import_date"] = datetime.datetime.now().strftime("%Y-%m-%d")

    # create metaphone columns
    double_metaphone = fuzzy.DMetaphone()
    df["first_name_metaphone_1"], df["first_name_metaphone_2"] = zip(*df["first_name"].fillna("").apply(double_metaphone))
    df["last_name_metaphone_1"], df["last_name_metaphone_2"] = zip( *df["last_name"].fillna("").apply(double_metaphone))

    # Get initials
    df["first_name_initial"] = df["first_name"].str[0]
    df["last_name_initial"] = df["last_name"].str[0]

    # Handle null date fields
    df["dob"] = df["dob"].where(pd.notnull(df["dob"]), None)

    return df


def insert_raw_data(df: pd.DataFrame):
    for row in df.itertuples(index=False):
        params = (
            row.id,
            row.first_name,
            row.first_name_metaphone_1.decode() if row.first_name_metaphone_1 else None,
            row.first_name_metaphone_2.decode() if row.first_name_metaphone_2 else None,
            row.first_name_initial,
            row.last_name,
            row.last_name_metaphone_1.decode() if row.last_name_metaphone_1 else None,
            row.last_name_metaphone_2.decode() if row.last_name_metaphone_2 else None,
            row.last_name_initial,
            row.dob,
            row.ssn_last4,
            row.phone,
            row.address,
            row.facility_id,
            row.medical_record_number,
            row.import_date
        )

        query = """
            insert into raw_patient_entity (id, first_name, first_name_metaphone_1, first_name_metaphone_2, first_name_initial, last_name, last_name_metaphone_1, last_name_metaphone_2, last_name_initial, dob, ssn_last4, phone, address, facility_id, medical_record_number, import_date)
            VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        print(run_sql(query, params))


def main(file_path: str):
    # Load in csv file
    raw_df = pd.read_csv(file_path)

    df_cleaned = add_helper_columns(raw_df)

    insert_raw_data(df_cleaned)

if __name__ == "__main__":

    file_path = "project_data.csv"
    main(file_path)
