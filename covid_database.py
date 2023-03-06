import psycopg2
import csv

# Connect to the database
conn = psycopg2.connect("host=127.0.0.1 dbname=data_model_exercise user=postgres password=twinkleandfluffy")
cur = conn.cursor()

# Create the database tables
def create_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS nhs_total_death (nhs_england_region varchar, date date,\
        new_deaths_with_positive_test int, cumulative_deaths_with_positive_test int,\
        new_deaths_without_positive_test int, cumulative_deaths_without_positive_test int,\
        new_deaths_total int, cumulative_deaths_total int, data_subject_to_change boolean);")
    
    cur.execute("CREATE TABLE IF NOT EXISTS nhs_daily_death (nhs_england_region varchar, date_of_death date,\
        date_of_report date, date_of_announcement date, new_deaths_with_positive_test int,\
        new_deaths_without_positive_test int, new_deaths_total int);")

    cur.execute("CREATE TABLE IF NOT EXISTS nhs_weekly_death (area_code text, area_name varchar, place_of_death varchar,\
        week_start date, week_end date, week_number int, year int, cause_of_death text, deaths int);")

# Insert data from CSV into the database tables
def insert_total_death(cur):
    with open('nhse_total_deaths_by_region.csv', 'r') as file:
        # Create a CSV reader object
        reader = csv.reader(file)
        # Skip the header row
        next(reader)
        # Read the data from the CSV file into a list of tuples
        data = [tuple(row) for row in reader]
        
        # Check if each row already exists in the table before inserting
        for row in data:
            cur.execute("SELECT COUNT(*) FROM nhs_total_death WHERE nhs_england_region=%s AND date=%s", (row[0], row[1]))
            count = cur.fetchone()[0]
            if count == 0:
                cur.execute("INSERT INTO nhs_total_death (nhs_england_region, date, new_deaths_with_positive_test, cumulative_deaths_with_positive_test,\
                    new_deaths_without_positive_test, cumulative_deaths_without_positive_test, new_deaths_total, cumulative_deaths_total, data_subject_to_change)\
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
        
        # Commit the changes to the database
        conn.commit()

def insert_nhse_daily_death(cur):
    with open('nhse_daily_announced_deaths_by_region.csv', 'r') as file:
        # Create a CSV reader object
        reader = csv.reader(file)
        # Skip the header row
        next(reader)
        # Read the data from the CSV file into a list of tuples
        data = [tuple(row) for row in reader]
        
        # Check if each row already exists in the table before inserting
        for row in data:
            if row[1] != 'NA':  # Check if date_of_death is valid
                cur.execute("SELECT COUNT(*) FROM nhs_daily_death WHERE nhs_england_region=%s AND date_of_death=%s", (row[0], row[1]))
                count = cur.fetchone()[0]

                if count == 0:
                    cur.execute("INSERT INTO nhs_daily_death (nhs_england_region varchar, date_of_death date,\
                    date_of_report date, date_of_announcement date, new_deaths_with_positive_test int,\
                    new_deaths_without_positive_test int, new_deaths_total int)\
                    VALUES (%s, %s, %s, %s, %s, %s, %s)", row)
            
        # Commit the changes to the database
        conn.commit()

def insert_ons_weekly_death(cur):
    with open('ons_deaths_weekly_occurrences_by_la_location.csv', 'r') as file:
        # Create a CSV reader object
        reader = csv.reader(file)
        # Skip the header row
        next(reader)
        # Read the data from the CSV file into a list of tuples
        data = [tuple(row) for row in reader]
        
        # Check if each row already exists in the table before inserting
        for row in data:
            cur.execute("SELECT COUNT(*) FROM nhs_weekly_death WHERE area_code=%s AND place_of_death=%s", (row[0], row[2]))
            count = cur.fetchone()[0]
            if count == 0:
                cur.execute("INSERT INTO nhs_weekly_death (area_code, area_name, place_of_death, week_start, week_end, week_number, year, cause_of_death, deaths)\
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8] if len(row) == 9 else None))
              
        # Commit the changes to the database
        conn.commit()

# Call the create_table() and insert_data_from_csv() functions
create_table(cur, conn)
insert_nhse_daily_death(cur)
insert_total_death(cur)
insert_ons_weekly_death(cur)

# Close the database connection
conn.close()
