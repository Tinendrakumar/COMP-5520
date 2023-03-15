import psycopg2
import csv

def export_discharge_summary_notes_to_csv():
    # Connect to the MIMIC-III database
    conn = psycopg2.connect(database="mimic", user="mimic_demo", password="mimic_demo", host="172.16.34.1", port="5432")
    cur = conn.cursor()

    # Set the search path to the MIMIC-III schema
    cur.execute('SET search_path to mimiciii')

    # Retrieve all Discharge Summary notes
    cur.execute("""
        SELECT n.row_id, n.chartdate, n.text, a.hospital_expire_flag, string_agg(DISTINCT d.icd9_code, '/') as icd9_codes
        FROM noteevents n
        INNER JOIN admissions a ON n.hadm_id = a.hadm_id
        LEFT JOIN diagnoses_icd d ON a.hadm_id = d.hadm_id
        WHERE n.category = 'Discharge summary'
        GROUP BY n.row_id, n.chartdate, n.text, a.hospital_expire_flag
        limit 10000
    """)

    # Export notes to CSV file
    with open('discharge_summary_notes_sample(3).csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['note_id', 'chartdate', 'note_text', 'hospital_expire_flag', 'icd9_codes'])
        for note_id, chartdate, note_text, hospital_expire_flag, icd9_codes in cur:
            chartdate_str = chartdate.strftime('%Y-%m-%dT%H:%M:%SZ')
            writer.writerow([note_id, chartdate_str, note_text, hospital_expire_flag, icd9_codes])
    # Close the database connection
    cur.close()
    conn.close()


if __name__ == '__main__':
    export_discharge_summary_notes_to_csv()
